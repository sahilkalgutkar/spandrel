package sampler

import (
	"math"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// newLimited is newSampler with each service allowed rate kept traces a
// second, burst at once, and verdicts remembered for a minute.
func newLimited(t *testing.T, policies []Policy, rate float64, burst int) (*Sampler, *clock, *writer) {
	t.Helper()
	c, w := newClock(), newWriter()
	s, err := New(Config{
		Quiet:        time.Second,
		MaxAge:       5 * time.Second,
		MaxSpans:     1000,
		Policies:     policies,
		Remember:     time.Minute,
		MaxDecisions: 100,
		ServiceRate:  rate,
		ServiceBurst: burst,
		Now:          c.Now,
	}, w)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, c, w
}

func from(service string, s trace.Span) trace.Span {
	s.Service = service
	return s
}

// traces accepts one root span from service in each of traces first to last,
// then sweeps them all at the same instant.
func traces(t *testing.T, s *Sampler, c *clock, service string, first, last byte) {
	t.Helper()
	for n := first; n <= last; n++ {
		accept(t, s, in(n, from(service, span(1, 0, 0, 100))))
	}
	c.advance(time.Second)
	sweep(t, s)
}

func TestNewRejectsABadServiceRate(t *testing.T) {
	w := newWriter()
	base := Config{Quiet: time.Second, MaxAge: time.Minute, MaxSpans: 1}
	for name, cfg := range map[string]func(Config) Config{
		"negative rate":     func(c Config) Config { c.ServiceRate, c.ServiceBurst = -1, 1; return c },
		"rate not a number": func(c Config) Config { c.ServiceRate, c.ServiceBurst = math.NaN(), 1; return c },
		"no burst":          func(c Config) Config { c.ServiceRate = 1; return c },
	} {
		if _, err := New(cfg(base), w); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func TestAServiceGetsItsBurstThenItsRate(t *testing.T) {
	s, c, _ := newLimited(t, keepAll, 2, 3)

	traces(t, s, c, "noisy", 1, 5)
	st := s.Stats()
	if st.Kept != 3 || st.Reasons["rate limited"] != 2 {
		t.Fatalf("kept %d, rate limited %d; want the burst of 3 kept and 2 limited", st.Kept, st.Reasons["rate limited"])
	}

	// One second earns two more, and no more than that.
	traces(t, s, c, "noisy", 6, 9)
	if st := s.Stats(); st.Kept != 5 || st.Reasons["rate limited"] != 4 {
		t.Errorf("a second later: kept %d, rate limited %d; want 5 and 4", st.Kept, st.Reasons["rate limited"])
	}
}

func TestEachServiceHasItsOwnAllowance(t *testing.T) {
	s, c, w := newLimited(t, keepAll, 1, 1)
	traces(t, s, c, "noisy", 1, 3)
	traces(t, s, c, "quiet", 4, 4)

	if len(w.spans(traceID(4))) != 1 {
		t.Error("a quiet service lost its trace to a noisy one")
	}
}

func TestErrorTracesAreLimitedToo(t *testing.T) {
	s, c, _ := newLimited(t, []Policy{KeepErrors{}}, 1, 2)
	for n := byte(1); n <= 4; n++ {
		accept(t, s, in(n, failed(span(1, 0, 0, 100))))
	}
	c.advance(time.Second)
	sweep(t, s)

	if st := s.Stats(); st.Reasons["error"] != 2 || st.Reasons["rate limited"] != 2 {
		t.Errorf("verdicts %v: want 2 kept as errors and 2 rate limited", st.Reasons)
	}
}

func TestALateSpanOfARateLimitedTraceIsDropped(t *testing.T) {
	s, c, w := newLimited(t, keepAll, 1, 1)
	traces(t, s, c, "noisy", 1, 2)

	accept(t, s, in(2, from("noisy", span(2, 1, 10, 20))))
	sweep(t, s)
	if st := s.Stats(); st.LateDropped != 1 || len(w.spans(traceID(2))) != 0 {
		t.Errorf("the late span of a limited trace: %d late dropped, %d written", st.LateDropped, len(w.spans(traceID(2))))
	}
}

func TestATraceIsChargedToItsRootService(t *testing.T) {
	root := from("frontend", span(1, 0, 0, 100))
	child := from("payments", span(2, 1, 10, 50))
	orphan := from("payments", span(3, 9, 10, 50))
	tests := []struct {
		name  string
		spans []trace.Span
		want  string
	}{
		{"the root span's service", []trace.Span{child, root}, "frontend"},
		{"the first span that arrived when the root did not", []trace.Span{orphan}, "payments"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rootService(tree(t, tt.spans...)); got != tt.want {
				t.Errorf("charged to %q, want %q", got, tt.want)
			}
		})
	}
}

func TestARefilledBucketIsForgotten(t *testing.T) {
	s, c, _ := newLimited(t, keepAll, 1, 2)
	accept(t, s, in(1, from("busy", span(1, 0, 0, 100))), in(2, from("busy", span(1, 0, 0, 100))),
		in(3, from("light", span(1, 0, 0, 100))))
	c.advance(time.Second)
	sweep(t, s)

	// A second later busy has earned back one of the two it spent, and light
	// has its one back, so only busy's bucket is still worth keeping.
	c.advance(time.Second)
	sweep(t, s)
	if _, ok := s.buckets["light"]; ok {
		t.Error("a full bucket was kept")
	}
	if _, ok := s.buckets["busy"]; !ok {
		t.Error("a bucket still refilling was forgotten")
	}

	c.advance(time.Second)
	sweep(t, s)
	if len(s.buckets) != 0 {
		t.Errorf("%d buckets left once every service is full again", len(s.buckets))
	}
}
