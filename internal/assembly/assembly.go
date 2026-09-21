// Package assembly turns the spans of one trace into a tree.
//
// Spans arrive in whatever order the network delivers them, from processes
// that never coordinated. Nothing guarantees that a parent arrives before its
// children, that it arrives at all, or that the parent links even form a
// tree. The sampler has to judge a trace before any of that is settled, so
// the tree built here has to be useful when it is incomplete and never wrong
// about what it does know.
package assembly

import (
	"errors"
	"fmt"
	"slices"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	ErrNoSpans     = errors.New("assembly: no spans to assemble")
	ErrMixedTraces = errors.New("assembly: spans belong to more than one trace")
)

// Node is one span in the tree.
type Node struct {
	Span     trace.Span
	Parent   *Node
	Children []*Node
}

// Tree is an assembled trace.
type Tree struct {
	TraceID trace.TraceID

	// Roots are the nodes with no parent in the tree, earliest first. A
	// well-formed trace has exactly one.
	Roots []*Node

	nodes map[trace.SpanID]*Node
}

// Build assembles spans that all belong to one trace. The spans are expected
// to have passed trace.Span.Validate.
//
// The result does not depend on the order of spans. A span identifier that
// appears more than once keeps its last occurrence, which is the same rule
// the store applies to a retried export.
func Build(spans []trace.Span) (*Tree, error) {
	if len(spans) == 0 {
		return nil, ErrNoSpans
	}

	t := &Tree{
		TraceID: spans[0].TraceID,
		nodes:   make(map[trace.SpanID]*Node, len(spans)),
	}
	for _, s := range spans {
		if s.TraceID != t.TraceID {
			return nil, fmt.Errorf("%w: %s and %s", ErrMixedTraces, t.TraceID, s.TraceID)
		}
		t.nodes[s.SpanID] = &Node{Span: s}
	}

	for _, n := range t.nodes {
		parent, ok := t.nodes[n.Span.ParentSpanID]
		if n.Span.IsRoot() || !ok {
			t.Roots = append(t.Roots, n)
			continue
		}
		n.Parent = parent
		parent.Children = append(parent.Children, n)
	}

	// Map iteration order is random, so everything that was appended in that
	// order is sorted before anyone sees it.
	slices.SortFunc(t.Roots, byStart)
	for _, n := range t.nodes {
		slices.SortFunc(n.Children, byStart)
	}
	return t, nil
}

// Len is the number of spans in the tree.
func (t *Tree) Len() int { return len(t.nodes) }

// Node returns the node for a span identifier, and false if the tree has no
// such span.
func (t *Tree) Node(id trace.SpanID) (*Node, bool) {
	n, ok := t.nodes[id]
	return n, ok
}

// Walk visits every node reachable from the roots, parents before children
// and siblings in start order. It stops early if fn returns false.
func (t *Tree) Walk(fn func(n *Node, depth int) bool) {
	for _, r := range t.Roots {
		if !walk(r, 0, fn) {
			return
		}
	}
}

func walk(n *Node, depth int, fn func(*Node, int) bool) bool {
	if !fn(n, depth) {
		return false
	}
	for _, c := range n.Children {
		if !walk(c, depth+1, fn) {
			return false
		}
	}
	return true
}

// byStart orders nodes by start time, falling back to the span identifier so
// that two spans starting in the same nanosecond still sort the same way
// every time.
func byStart(a, b *Node) int {
	if c := a.Span.Start.Compare(b.Span.Start); c != 0 {
		return c
	}
	return slices.Compare(a.Span.SpanID[:], b.Span.SpanID[:])
}
