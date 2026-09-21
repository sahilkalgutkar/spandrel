package assembly

import (
	"slices"
	"time"
)

// SelfTime is how much of the span's own duration none of its children were
// running for. That is the time spent in the span itself, as opposed to
// waiting on work it handed off.
//
// Children can overlap each other, when calls go out concurrently, and can
// run past their parent's end, when the work is asynchronous. So this
// subtracts the union of the child intervals, clipped to the parent, rather
// than the sum of the child durations. The sum would give a negative self
// time for any span that fanned out in parallel.
//
// A placeholder has no time of its own and reports zero.
func (n *Node) SelfTime() time.Duration {
	if n.Missing() {
		return 0
	}

	start, end := n.Span.Start, n.Span.End
	busy := time.Duration(0)
	cursor := start

	// Children are sorted by start, so one pass merges their intervals.
	for _, c := range n.Children {
		from, to := later(c.Span.Start, cursor), earlier(c.Span.End, end)
		if to.After(from) {
			busy += to.Sub(from)
			cursor = to
		}
	}
	return end.Sub(start) - busy
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Segment is a stretch of the critical path spent in one span.
type Segment struct {
	Node       *Node
	Start, End time.Time
}

// Duration is how long the segment lasted.
func (s Segment) Duration() time.Duration { return s.End.Sub(s.Start) }

// CriticalPath returns the chain of work that decided how long the span
// took, as consecutive segments from its start to its end. Making anything
// off the path faster would not have made the span finish any sooner.
//
// The path is found by walking backwards from the end. At each moment it
// follows whichever child finished last before that moment, since the parent
// was waiting on that child. Anything the parent did between two children
// counts as the parent's own time. Children that ran past the parent's end, or
// started before it because of clock skew, are clipped to the parent's window,
// so the segments always cover the span exactly once.
//
// The walk recurses once per level of the tree, so its depth is the trace's
// depth rather than its span count.
func (n *Node) CriticalPath() []Segment {
	var path []Segment
	criticalPath(n, n.Span.Start, n.Span.End, &path)

	// The walk runs backwards, so the segments come out last first.
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path
}

// criticalPath appends the critical path of n within [floor, ceiling] to path,
// latest segment first.
func criticalPath(n *Node, floor, ceiling time.Time, path *[]Segment) {
	floor, cursor := later(floor, n.Span.Start), earlier(ceiling, n.Span.End)

	// Children in the order they finished, last first. Ties go to the one
	// that started later, which is the one the parent was really waiting on.
	children := make([]*Node, len(n.Children))
	copy(children, n.Children)
	slices.SortFunc(children, func(a, b *Node) int {
		if c := b.Span.End.Compare(a.Span.End); c != 0 {
			return c
		}
		return byStart(b, a)
	})

	for _, c := range children {
		end := earlier(c.Span.End, cursor)
		if !end.After(floor) {
			break
		}
		if !c.Span.Start.Before(cursor) {
			// It started after the point being explained, so the parent
			// cannot have been waiting on it then.
			continue
		}
		appendSegment(path, n, end, cursor)
		criticalPath(c, floor, end, path)
		cursor = later(c.Span.Start, floor)
	}
	appendSegment(path, n, floor, cursor)
}

// appendSegment adds a segment unless it is empty, merging it into the last
// one when the same span continues. That happens when the parent's own time
// is interrupted by a child that turns out to take no time on the path.
func appendSegment(path *[]Segment, n *Node, start, end time.Time) {
	if !end.After(start) {
		return
	}
	if last := len(*path) - 1; last >= 0 && (*path)[last].Node == n && (*path)[last].Start.Equal(end) {
		(*path)[last].Start = start
		return
	}
	*path = append(*path, Segment{Node: n, Start: start, End: end})
}
