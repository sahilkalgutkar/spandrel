package assembly

import "time"

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
