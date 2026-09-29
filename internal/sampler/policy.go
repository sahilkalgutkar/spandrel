// Package sampler decides which traces are kept, after it has seen them.
//
// Head sampling decides at the first span, before anyone knows whether the
// request will fail or crawl. Tail sampling holds every span of a trace until
// the trace looks finished, then judges the whole thing. The price is memory
// and a guess about when "finished" is, and most of this package is about
// paying that price safely.
package sampler

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/assembly"
)

// Verdict is what a policy decided about a trace, and why.
type Verdict struct {
	Keep bool

	// Reason names the policy that decided, so the counters can say which
	// rule is keeping what.
	Reason string
}

// Policy judges an assembled trace.
//
// A policy either decides or abstains, and the chain moves on when it
// abstains. That keeps each rule small: "keep errors" has no opinion about a
// trace without one, and should not have to pretend to.
type Policy interface {
	Evaluate(t *assembly.Tree) (v Verdict, decided bool)
}

// Decide runs the policies in order and returns the first decision. A trace
// that no policy decides on is dropped. A sampler that keeps whatever nobody
// asked for is just an expensive way of storing everything.
func Decide(t *assembly.Tree, policies []Policy) Verdict {
	for _, p := range policies {
		if v, ok := p.Evaluate(t); ok {
			return v
		}
	}
	return Verdict{Keep: false, Reason: "unclaimed"}
}

// KeepErrors keeps any trace in which some span reported an error.
//
// Only an explicit error status counts. An unset status means nobody said,
// and most instrumentation leaves it unset on success.
type KeepErrors struct{}

func (KeepErrors) Evaluate(t *assembly.Tree) (Verdict, bool) {
	failed := false
	t.Walk(func(n *assembly.Node, _ int) bool {
		failed = !n.Missing() && n.Span.Failed()
		return !failed
	})
	if failed {
		return Verdict{Keep: true, Reason: "error"}, true
	}
	return Verdict{}, false
}

// KeepSlow keeps any trace that took longer than Threshold end to end.
type KeepSlow struct {
	Threshold time.Duration
}

func (p KeepSlow) Evaluate(t *assembly.Tree) (Verdict, bool) {
	if Duration(t) > p.Threshold {
		return Verdict{Keep: true, Reason: "slow"}, true
	}
	return Verdict{}, false
}

// Duration is how long the trace took end to end: its root's duration when it
// has one root, and otherwise the span from the earliest start to the latest
// end across its roots.
//
// A trace missing its root still has a duration worth judging. The roots
// include the placeholder that stands in for the missing root, and that
// placeholder already covers the children that did arrive.
func Duration(t *assembly.Tree) time.Duration {
	start, end := t.Roots[0].Span.Start, t.Roots[0].Span.End
	for _, r := range t.Roots[1:] {
		if r.Span.Start.Before(start) {
			start = r.Span.Start
		}
		if r.Span.End.After(end) {
			end = r.Span.End
		}
	}
	return end.Sub(start)
}

// KeepFraction keeps a fixed share of traces and drops the rest. It always
// decides, so it belongs at the end of a chain.
//
// The choice is a function of the trace identifier, not a coin flip. Asking
// twice about the same trace gets the same answer, which matters for a span
// that arrives after its trace was judged, and for two sampler instances that
// see different halves of one trace. It reads the rightmost 56 bits because
// those are the bits the W3C trace context specification requires to be
// random when the random flag is set, which is also how OpenTelemetry's own
// ratio sampler reads them.
type KeepFraction struct {
	threshold uint64
}

// NewKeepFraction keeps the given share of traces, from 0 for none to 1 for
// all.
func NewKeepFraction(share float64) (KeepFraction, error) {
	if !(share >= 0 && share <= 1) {
		return KeepFraction{}, fmt.Errorf("sampler: share must be between 0 and 1, got %v", share)
	}
	return KeepFraction{threshold: uint64(math.Round(share * (1 << 56)))}, nil
}

func (p KeepFraction) Evaluate(t *assembly.Tree) (Verdict, bool) {
	id := t.TraceID
	random := binary.BigEndian.Uint64(id[8:]) & (1<<56 - 1)
	if random < p.threshold {
		return Verdict{Keep: true, Reason: "sampled"}, true
	}
	return Verdict{Keep: false, Reason: "sampled out"}, true
}
