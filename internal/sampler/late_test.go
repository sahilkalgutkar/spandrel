package sampler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/assembly"
	"github.com/sahilkalgutkar/spandrel/internal/ingest"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// These tests are about the two places tail sampling actually breaks: a span
// that turns up after its trace was judged, and a trace that is sealed or
// forgotten while spans for it are still on their way.

// newRemembering is newSampler with verdicts remembered for remember, up to
// max of them.
func newRemembering(t *testing.T, policies []Policy, remember time.Duration, max int) (*Sampler, *clock, *writer) {
	t.Helper()
	c, w := newClock(), newWriter()
	s, err := New(Config{
		Quiet:        time.Second,
		MaxAge:       5 * time.Second,
		MaxSpans:     1000,
		Policies:     policies,
		Remember:     remember,
		MaxDecisions: max,
		Now:          c.Now,
	}, w)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, c, w
}

// judged accepts spans for one trace and sweeps once it has gone quiet.
func judged(t *testing.T, s *Sampler, c *clock, spans ...trace.Span) {
	t.Helper()
	accept(t, s, spans...)
	c.advance(time.Second)
	sweep(t, s)
}

func TestNewRejectsABadMemory(t *testing.T) {
	w := newWriter()
	base := Config{Quiet: time.Second, MaxAge: time.Minute, MaxSpans: 1}
	negative, noRoom := base, base
	negative.Remember, negative.MaxDecisions = -time.Second, 10
	noRoom.Remember = time.Minute
	for name, cfg := range map[string]Config{"negative": negative, "no room": noRoom} {
		if _, err := New(cfg, w); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func TestALateSpanOfAKeptTraceIsWrittenWithoutBeingJudgedAgain(t *testing.T) {
	s, c, w := newRemembering(t, keepAll, time.Minute, 100)
	judged(t, s, c, span(1, 0, 0, 100))

	accept(t, s, span(2, 1, 10, 20))
	st := s.Stats()
	if st.Traces != 0 || st.Buffered != 1 || st.LateKept != 1 {
		t.Fatalf("after a late span: %d traces, %d buffered, %d late kept; want 0, 1, 1", st.Traces, st.Buffered, st.LateKept)
	}
	sweep(t, s)

	if got := len(w.spans(testTrace)); got != 2 {
		t.Errorf("wrote %d spans of the trace, want 2", got)
	}
	if st := s.Stats(); st.Kept != 1 || st.Buffered != 0 {
		t.Errorf("kept %d traces with %d still buffered, want 1 and 0", st.Kept, st.Buffered)
	}
}

// The bug the memory is for: the late span of an error trace is usually a
// healthy span, so judged on its own it is dropped from a trace that was kept.
func TestWithoutAMemoryALateSpanIsJudgedOnItsOwn(t *testing.T) {
	s, c, w := newSampler(t, []Policy{KeepErrors{}})
	judged(t, s, c, failed(span(1, 0, 0, 100)))
	judged(t, s, c, span(2, 1, 10, 20))

	if got := len(w.spans(testTrace)); got != 1 {
		t.Errorf("wrote %d spans of the error trace, want the 1 that was judged with it", got)
	}
	if st := s.Stats(); st.Kept != 1 || st.Dropped != 1 {
		t.Errorf("kept %d, dropped %d: want the trace kept once and its late span dropped once", st.Kept, st.Dropped)
	}

	s, c, w = newRemembering(t, []Policy{KeepErrors{}}, time.Minute, 100)
	judged(t, s, c, failed(span(1, 0, 0, 100)))
	judged(t, s, c, span(2, 1, 10, 20))
	if got := len(w.spans(testTrace)); got != 2 {
		t.Errorf("with a memory, wrote %d spans of the error trace, want 2", got)
	}
}

// Following the verdict cuts both ways: an error that arrives after its
// trace was dropped is dropped too. Judging the trace again on one span would
// keep a trace whose other spans are already gone.
func TestALateSpanFollowsADropEvenIfItFailed(t *testing.T) {
	s, c, w := newRemembering(t, []Policy{KeepErrors{}}, time.Minute, 100)
	judged(t, s, c, span(1, 0, 0, 100))

	accept(t, s, failed(span(2, 1, 10, 20)))
	c.advance(time.Second)
	sweep(t, s)

	if got := len(w.spans(testTrace)); got != 0 {
		t.Errorf("wrote %d spans of a dropped trace", got)
	}
	st := s.Stats()
	if st.LateDropped != 1 || st.Dropped != 1 || st.Traces != 0 || st.Buffered != 0 {
		t.Errorf("%d late dropped, %d traces dropped, %d pending, %d buffered; want 1, 1, 0, 0",
			st.LateDropped, st.Dropped, st.Traces, st.Buffered)
	}
}

// gate is a policy that waits to be released before it answers, so a test
// can send spans while a trace is being judged.
type gate struct {
	v       Verdict
	entered chan struct{}
	release chan struct{}
}

func newGate(v Verdict) *gate {
	return &gate{v: v, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) Evaluate(*assembly.Tree) (Verdict, bool) {
	close(g.entered)
	<-g.release
	return g.v, true
}

func TestASpanThatArrivesWhileItsTraceIsJudgedFollowsTheVerdict(t *testing.T) {
	for _, keep := range []bool{true, false} {
		g := newGate(Verdict{Keep: keep, Reason: "gate"})
		s, c, w := newSampler(t, []Policy{g})
		accept(t, s, span(1, 0, 0, 100))
		c.advance(time.Second)

		done := make(chan error)
		go func() { done <- s.Sweep(context.Background()) }()
		<-g.entered
		accept(t, s, span(2, 1, 10, 20))
		if st := s.Stats(); st.Traces != 0 || st.Buffered != 1 {
			t.Fatalf("keep=%v: a span for a trace being judged started over: %d traces, %d buffered", keep, st.Traces, st.Buffered)
		}
		close(g.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}

		want, st := 0, s.Stats()
		if keep {
			want = 2
		}
		if got := len(w.spans(testTrace)); got != want {
			t.Errorf("keep=%v: wrote %d spans, want %d", keep, got, want)
		}
		if st.Buffered != 0 || st.LateKept+st.LateDropped != 1 {
			t.Errorf("keep=%v: %d buffered, %d late; want 0 and 1", keep, st.Buffered, st.LateKept+st.LateDropped)
		}
	}
}

func TestAVerdictIsForgottenAfterRemember(t *testing.T) {
	s, c, w := newRemembering(t, keepAll, 10*time.Second, 100)
	judged(t, s, c, span(1, 0, 0, 100))
	if got := s.Stats().Decisions; got != 1 {
		t.Fatalf("remembering %d verdicts, want 1", got)
	}

	c.advance(9 * time.Second)
	sweep(t, s)
	accept(t, s, span(2, 1, 10, 20))
	if st := s.Stats(); st.LateKept != 1 {
		t.Fatalf("9s after the verdict a span was not late: %+v", st)
	}

	c.advance(time.Second)
	sweep(t, s)
	if got := s.Stats().Decisions; got != 0 {
		t.Fatalf("still remembering %d verdicts after 10s", got)
	}
	judged(t, s, c, span(3, 1, 20, 30))
	if st := s.Stats(); st.Kept != 2 || st.LateKept != 1 {
		t.Errorf("a forgotten trace should be judged again: kept %d, late %d", st.Kept, st.LateKept)
	}
	if got := len(w.spans(testTrace)); got != 3 {
		t.Errorf("wrote %d spans, want 3", got)
	}
}

// A span past its trace's memory but before the sweep that forgets it must
// not follow the old verdict either.
func TestAnExpiredVerdictIsIgnoredBeforeItIsForgotten(t *testing.T) {
	s, c, _ := newRemembering(t, keepAll, 10*time.Second, 100)
	judged(t, s, c, span(1, 0, 0, 100))
	c.advance(10 * time.Second)

	accept(t, s, span(2, 1, 10, 20))
	if st := s.Stats(); st.Traces != 1 || st.LateKept != 0 {
		t.Errorf("a span after the verdict expired: %d traces pending, %d late; want 1 and 0", st.Traces, st.LateKept)
	}
}

func TestTheOldestVerdictIsForgottenFirst(t *testing.T) {
	s, c, _ := newRemembering(t, keepAll, time.Hour, 2)
	for n := byte(1); n <= 3; n++ {
		judged(t, s, c, in(n, span(1, 0, 0, 100)))
	}
	if got := s.Stats().Decisions; got != 2 {
		t.Fatalf("remembering %d verdicts, want the 2 it has room for", got)
	}

	accept(t, s, in(1, span(2, 1, 10, 20)), in(3, span(2, 1, 10, 20)))
	if st := s.Stats(); st.Traces != 1 || st.LateKept != 1 {
		t.Errorf("trace 1 should be forgotten and trace 3 remembered: %d pending, %d late", st.Traces, st.LateKept)
	}
}

// keepThenDrop keeps the first trace it judges and drops every one after.
type keepThenDrop struct {
	mu     sync.Mutex
	judged int
}

func (p *keepThenDrop) Evaluate(*assembly.Tree) (Verdict, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.judged++
	return Verdict{Keep: p.judged == 1, Reason: "alternating"}, true
}

// A trace judged again leaves its first verdict queued for forgetting. That
// stale entry must not take the new verdict with it.
func TestForgettingAnOldVerdictKeepsANewerOne(t *testing.T) {
	s, c, _ := newRemembering(t, []Policy{&keepThenDrop{}}, 10*time.Second, 100)
	judged(t, s, c, span(1, 0, 0, 100))
	c.advance(10 * time.Second)

	// Expired, so this span starts the trace over, and the sweep that judges
	// it again is the same one that forgets the first verdict.
	judged(t, s, c, span(2, 1, 10, 20))
	if got := s.Stats().Decisions; got != 1 {
		t.Fatalf("remembering %d verdicts, want the new one", got)
	}
	accept(t, s, span(3, 1, 20, 30))
	if st := s.Stats(); st.LateDropped != 1 {
		t.Errorf("a late span should follow the new verdict, a drop: %+v", st)
	}
}

func TestForgettingGivesTheQueueBack(t *testing.T) {
	s, c, _ := newRemembering(t, keepAll, time.Second, 1000)
	spans := make([]trace.Span, 200)
	for i := range spans {
		spans[i] = in(byte(i), span(1, 0, 0, 100))
	}
	judged(t, s, c, spans...)
	grown := cap(s.order)

	c.advance(time.Second)
	sweep(t, s)
	if len(s.order) != 0 || cap(s.order) >= grown {
		t.Errorf("after forgetting everything the queue has length %d and capacity %d, was %d", len(s.order), cap(s.order), grown)
	}
}

// The eviction path: a trace that never goes quiet is sealed at MaxAge, and
// its spans keep coming. They used to start a new trace every quiet period.
func TestABusyTraceSealedByAgeKeepsFollowingItsVerdict(t *testing.T) {
	s, c, w := newRemembering(t, keepAll, time.Minute, 100)
	for i := 0; i < 20; i++ {
		accept(t, s, span(byte(i+1), 0, i*10, i*10+5))
		c.advance(500 * time.Millisecond)
		sweep(t, s)
	}

	st := s.Stats()
	if st.SealedByAge != 1 || st.Kept != 1 || st.Traces != 0 {
		t.Errorf("sealed by age %d times, kept %d traces, %d pending; want 1, 1, 0", st.SealedByAge, st.Kept, st.Traces)
	}
	if got := len(w.spans(testTrace)); got != 20 {
		t.Errorf("wrote %d of the 20 spans", got)
	}
}

func TestLateSpansOfKeptTracesCountAgainstTheBuffer(t *testing.T) {
	c, w := newClock(), newWriter()
	s, err := New(Config{
		Quiet: time.Second, MaxAge: 5 * time.Second, MaxSpans: 2,
		Policies: []Policy{KeepErrors{}}, Remember: time.Minute, MaxDecisions: 10, Now: c.Now,
	}, w)
	if err != nil {
		t.Fatal(err)
	}
	judged(t, s, c, in(1, failed(span(1, 0, 0, 100))))
	judged(t, s, c, in(2, span(1, 0, 0, 100)))

	accept(t, s, in(1, span(2, 1, 0, 10)), in(1, span(3, 1, 0, 10)))
	err = s.Accept(context.Background(), []trace.Span{in(3, span(1, 0, 0, 10))})
	if !errors.Is(err, ingest.ErrSinkFull) {
		t.Fatalf("two late spans waiting to be written should fill a buffer of two, got %v", err)
	}
	// A late span of a dropped trace is not held, so it fits anyway.
	accept(t, s, in(2, span(2, 1, 0, 10)))

	sweep(t, s)
	accept(t, s, in(3, span(1, 0, 0, 10)))
	if got := len(w.spans(traceID(1))); got != 3 {
		t.Errorf("wrote %d spans of the kept trace, want 3", got)
	}
}

func TestALateSpanTheWriterRefusesIsLost(t *testing.T) {
	s, c, w := newRemembering(t, keepAll, time.Minute, 100)
	judged(t, s, c, span(1, 0, 0, 100))
	w.mu.Lock()
	w.fail[testTrace] = true
	w.mu.Unlock()

	accept(t, s, span(2, 1, 10, 20))
	if err := s.Sweep(context.Background()); !errors.Is(err, errDiskFull) {
		t.Fatalf("Sweep returned %v, want the writer's error", err)
	}
	if st := s.Stats(); st.Lost != 1 || st.Buffered != 0 {
		t.Errorf("%d lost, %d buffered; want 1 and 0", st.Lost, st.Buffered)
	}
}

func TestFlushWritesLateSpans(t *testing.T) {
	s, c, w := newRemembering(t, keepAll, time.Minute, 100)
	judged(t, s, c, span(1, 0, 0, 100))
	accept(t, s, span(2, 1, 10, 20))

	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(w.spans(testTrace)); got != 2 {
		t.Errorf("wrote %d spans, want 2", got)
	}
}
