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

// Phase says whether more spans of a trace may still arrive.
type Phase uint8

const (
	// Open means the trace may still be receiving spans. A missing parent
	// could be one of them.
	Open Phase = iota

	// Sealed means the caller has decided the trace is finished, usually
	// because it has been quiet for long enough. A parent missing now is
	// never coming.
	Sealed
)

// Presence says whether a node's span was actually received.
type Presence uint8

const (
	// Received is a span that arrived.
	Received Presence = iota

	// Awaited stands in for a parent that some span names but that has not
	// arrived, in a trace that is still open.
	Awaited

	// Lost stands in for a parent that never arrived before the trace was
	// sealed. It was dropped somewhere, or the process that owned it died
	// before exporting.
	Lost
)

func (p Presence) String() string {
	switch p {
	case Awaited:
		return "awaited"
	case Lost:
		return "lost"
	default:
		return "received"
	}
}

// Node is one span in the tree, or a placeholder for a parent that is
// missing.
//
// A placeholder's Span carries only the trace and span identifiers, plus a
// start and end covering its children, so it still sorts and renders in a
// sensible place. Everything else on it is zero.
type Node struct {
	Span     trace.Span
	Presence Presence
	Parent   *Node
	Children []*Node
}

// Missing reports whether this node is a placeholder rather than a span that
// arrived.
func (n *Node) Missing() bool { return n.Presence != Received }

// Tree is an assembled trace.
type Tree struct {
	TraceID trace.TraceID

	// Roots are the nodes with no parent in the tree, earliest first. A
	// well-formed trace has exactly one. Placeholders are always roots, since
	// nothing is known about their own parent.
	Roots []*Node

	nodes   map[trace.SpanID]*Node
	missing []*Node
}

// Build assembles spans that all belong to one trace. The spans are expected
// to have passed trace.Span.Validate.
//
// A span whose parent is not among spans hangs under a placeholder for that
// parent, instead of becoming a root of its own. Siblings with the same
// missing parent stay together that way, and the placeholder records what the
// tree is waiting for. Whether the parent is still expected depends on phase.
//
// The result does not depend on the order of spans. A span identifier that
// appears more than once keeps its last occurrence, which is the same rule
// the store applies to a retried export.
func Build(spans []trace.Span, phase Phase) (*Tree, error) {
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

	absent := Awaited
	if phase == Sealed {
		absent = Lost
	}

	placeholders := make(map[trace.SpanID]*Node)
	for _, n := range t.nodes {
		if n.Span.IsRoot() {
			t.Roots = append(t.Roots, n)
			continue
		}
		parent, ok := t.nodes[n.Span.ParentSpanID]
		if !ok {
			parent, ok = placeholders[n.Span.ParentSpanID]
		}
		if !ok {
			parent = &Node{
				Span:     trace.Span{TraceID: t.TraceID, SpanID: n.Span.ParentSpanID},
				Presence: absent,
			}
			placeholders[n.Span.ParentSpanID] = parent
			t.Roots = append(t.Roots, parent)
			t.missing = append(t.missing, parent)
		}
		n.Parent = parent
		parent.Children = append(parent.Children, n)
	}

	// A placeholder has no times of its own, so it borrows the envelope of
	// its children. That has to happen before sorting, which reads them.
	for _, p := range t.missing {
		p.Span.Start, p.Span.End = p.Children[0].Span.Start, p.Children[0].Span.End
		for _, c := range p.Children[1:] {
			if c.Span.Start.Before(p.Span.Start) {
				p.Span.Start = c.Span.Start
			}
			if c.Span.End.After(p.Span.End) {
				p.Span.End = c.Span.End
			}
		}
	}

	// Map iteration order is random, so everything that was appended in that
	// order is sorted before anyone sees it.
	slices.SortFunc(t.Roots, byStart)
	slices.SortFunc(t.missing, byStart)
	for _, n := range t.nodes {
		slices.SortFunc(n.Children, byStart)
	}
	for _, n := range t.missing {
		slices.SortFunc(n.Children, byStart)
	}
	return t, nil
}

// Len is the number of spans in the tree, not counting placeholders.
func (t *Tree) Len() int { return len(t.nodes) }

// Missing returns the placeholders for parents that did not arrive, earliest
// first.
func (t *Tree) Missing() []*Node { return t.missing }

// Complete reports whether the tree is one trace with nothing missing: a
// single root that really is a root, and every parent accounted for.
func (t *Tree) Complete() bool {
	return len(t.missing) == 0 && len(t.Roots) == 1
}

// Node returns the node for a span that arrived, and false if the tree has
// no such span. Placeholders are not returned; use Missing for those.
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
