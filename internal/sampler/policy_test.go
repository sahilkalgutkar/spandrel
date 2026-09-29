package sampler

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/assembly"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	testTrace = trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36}
	epoch     = time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
)

func sid(n byte) trace.SpanID {
	var id trace.SpanID
	id[7] = n
	return id
}

// span builds a span of testTrace running from start to end milliseconds
// after epoch.
func span(id, parent byte, start, end int) trace.Span {
	return trace.Span{
		TraceID:      testTrace,
		SpanID:       sid(id),
		ParentSpanID: sid(parent),
		Name:         "op",
		Service:      "svc",
		Start:        epoch.Add(time.Duration(start) * time.Millisecond),
		End:          epoch.Add(time.Duration(end) * time.Millisecond),
	}
}

func failed(s trace.Span) trace.Span {
	s.Status = trace.Status{Code: trace.StatusError, Message: "boom"}
	return s
}

func tree(t *testing.T, spans ...trace.Span) *assembly.Tree {
	t.Helper()
	tr, err := assembly.Build(spans, assembly.Sealed)
	if err != nil {
		t.Fatalf("assembly.Build: %v", err)
	}
	return tr
}

// decides is a policy with a fixed answer, for testing the chain itself.
type decides struct {
	v  Verdict
	ok bool
}

func (d decides) Evaluate(*assembly.Tree) (Verdict, bool) { return d.v, d.ok }

func TestDecideTakesTheFirstDecision(t *testing.T) {
	tr := tree(t, span(1, 0, 0, 10))
	abstain := decides{}
	keep := decides{Verdict{Keep: true, Reason: "first"}, true}
	drop := decides{Verdict{Keep: false, Reason: "second"}, true}

	tests := []struct {
		name     string
		policies []Policy
		want     Verdict
	}{
		{"abstentions are skipped", []Policy{abstain, keep, drop}, keep.v},
		{"a drop decides as firmly as a keep", []Policy{drop, keep}, drop.v},
		{"nobody deciding means drop", []Policy{abstain, abstain}, Verdict{Keep: false, Reason: "unclaimed"}},
		{"no policies at all means drop", nil, Verdict{Keep: false, Reason: "unclaimed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Decide(tr, tt.policies); got != tt.want {
				t.Errorf("Decide = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestKeepErrors(t *testing.T) {
	tests := []struct {
		name    string
		spans   []trace.Span
		decided bool
	}{
		{"a clean trace is not its business", []trace.Span{span(1, 0, 0, 100), span(2, 1, 10, 20)}, false},
		{"an error deep in the trace", []trace.Span{span(1, 0, 0, 100), span(2, 1, 10, 50), failed(span(3, 2, 20, 30))}, true},
		{"an error under a lost parent", []trace.Span{span(1, 0, 0, 100), failed(span(3, 2, 20, 30))}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := KeepErrors{}.Evaluate(tree(t, tt.spans...))
			if ok != tt.decided {
				t.Fatalf("decided = %v, want %v", ok, tt.decided)
			}
			if ok && (!v.Keep || v.Reason != "error") {
				t.Errorf("verdict = %+v, want kept for error", v)
			}
		})
	}
}

func TestKeepErrorsIgnoresUnsetAndOK(t *testing.T) {
	ok := span(2, 1, 10, 20)
	ok.Status = trace.Status{Code: trace.StatusOK}
	if _, decided := (KeepErrors{}).Evaluate(tree(t, span(1, 0, 0, 100), ok)); decided {
		t.Error("KeepErrors decided on a trace with no error status")
	}
}

func TestKeepSlow(t *testing.T) {
	p := KeepSlow{Threshold: 500 * time.Millisecond}
	tests := []struct {
		name    string
		spans   []trace.Span
		decided bool
	}{
		{"under the threshold", []trace.Span{span(1, 0, 0, 400)}, false},
		{"exactly the threshold is not slow", []trace.Span{span(1, 0, 0, 500)}, false},
		{"over the threshold", []trace.Span{span(1, 0, 0, 501)}, true},
		{"a missing root is judged by what arrived", []trace.Span{span(2, 1, 0, 300), span(3, 1, 400, 900)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := p.Evaluate(tree(t, tt.spans...))
			if ok != tt.decided {
				t.Fatalf("decided = %v, want %v", ok, tt.decided)
			}
			if ok && (!v.Keep || v.Reason != "slow") {
				t.Errorf("verdict = %+v, want kept for slow", v)
			}
		})
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name  string
		spans []trace.Span
		want  time.Duration
	}{
		{"one root", []trace.Span{span(1, 0, 0, 250), span(2, 1, 10, 20)}, 250 * time.Millisecond},
		{"two roots are measured together", []trace.Span{span(1, 0, 100, 200), span(5, 0, 0, 150)}, 200 * time.Millisecond},
		{"a later root that ends first", []trace.Span{span(1, 0, 0, 300), span(5, 0, 100, 200)}, 300 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Duration(tree(t, tt.spans...)); got != tt.want {
				t.Errorf("Duration = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKeepFractionRejects(t *testing.T) {
	for _, share := range []float64{-0.01, 1.01, math.NaN(), math.Inf(1)} {
		if _, err := NewKeepFraction(share); err == nil {
			t.Errorf("NewKeepFraction(%v) succeeded", share)
		}
	}
}

// traceWith returns a one-span tree whose trace identifier is id.
func traceWith(t *testing.T, id trace.TraceID) *assembly.Tree {
	t.Helper()
	s := span(1, 0, 0, 10)
	s.TraceID = id
	return tree(t, s)
}

func TestKeepFractionKeepsAboutTheShare(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	for _, share := range []float64{0, 0.1, 0.5, 1} {
		p, err := NewKeepFraction(share)
		if err != nil {
			t.Fatal(err)
		}

		const n = 20000
		kept := 0
		for range n {
			var id trace.TraceID
			for i := range id {
				id[i] = byte(rng.Uint32())
			}
			v, ok := p.Evaluate(traceWith(t, id))
			if !ok {
				t.Fatal("KeepFraction abstained")
			}
			if v.Keep {
				kept++
			}
		}

		// Three standard deviations of a binomial, plus exactness at the ends.
		got := float64(kept) / n
		tolerance := 3 * math.Sqrt(share*(1-share)/n)
		if math.Abs(got-share) > tolerance {
			t.Errorf("share %v kept %.4f of traces, outside ±%.4f", share, got, tolerance)
		}
	}
}

func TestKeepFractionIsStablePerTrace(t *testing.T) {
	p, _ := NewKeepFraction(0.5)
	tr := traceWith(t, testTrace)
	first, _ := p.Evaluate(tr)
	for range 100 {
		if again, _ := p.Evaluate(tr); again != first {
			t.Fatalf("the same trace got %+v, then %+v", first, again)
		}
	}
}

// Only the rightmost 56 bits are random under the W3C specification, so the
// leading bits must not sway the decision.
func TestKeepFractionIgnoresTheLeadingBits(t *testing.T) {
	p, _ := NewKeepFraction(0.5)
	a, b := testTrace, testTrace
	for i := range 9 {
		b[i] ^= 0xff
	}
	va, _ := p.Evaluate(traceWith(t, a))
	vb, _ := p.Evaluate(traceWith(t, b))
	if va != vb {
		t.Errorf("changing only the leading bits changed the verdict: %+v, then %+v", va, vb)
	}
}
