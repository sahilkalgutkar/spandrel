package sampler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/ingest"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// clock is a manual clock for the sampler's arrival times.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: epoch} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// writer records what it is given, and fails for any trace listed in fail.
type writer struct {
	mu      sync.Mutex
	written map[trace.TraceID][]trace.Span
	fail    map[trace.TraceID]bool
}

func newWriter() *writer {
	return &writer{written: make(map[trace.TraceID][]trace.Span), fail: make(map[trace.TraceID]bool)}
}

var errDiskFull = errors.New("disk full")

func (w *writer) Write(_ context.Context, spans []trace.Span) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := spans[0].TraceID
	if w.fail[id] {
		return errDiskFull
	}
	w.written[id] = append(w.written[id], spans...)
	return nil
}

func (w *writer) spans(id trace.TraceID) []trace.Span {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written[id]
}

// keepAll keeps every trace, so tests about timing do not depend on policy.
var keepAll = []Policy{decides{Verdict{Keep: true, Reason: "all"}, true}}

func newSampler(t *testing.T, policies []Policy) (*Sampler, *clock, *writer) {
	t.Helper()
	c, w := newClock(), newWriter()
	s, err := New(Config{
		Quiet:    time.Second,
		MaxAge:   5 * time.Second,
		MaxSpans: 1000,
		Policies: policies,
		Now:      c.Now,
	}, w)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, c, w
}

// traceID returns a trace identifier distinct for each n.
func traceID(n byte) trace.TraceID {
	id := testTrace
	id[15] = n
	return id
}

// in moves a span into trace n.
func in(n byte, s trace.Span) trace.Span {
	s.TraceID = traceID(n)
	return s
}

func accept(t *testing.T, s *Sampler, spans ...trace.Span) {
	t.Helper()
	if err := s.Accept(context.Background(), spans); err != nil {
		t.Fatalf("Accept: %v", err)
	}
}

func sweep(t *testing.T, s *Sampler) {
	t.Helper()
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
}

func TestNewRejects(t *testing.T) {
	w := newWriter()
	tests := []struct {
		name string
		cfg  Config
		out  Writer
	}{
		{"no writer", Config{Quiet: time.Second, MaxAge: time.Minute, MaxSpans: 1}, nil},
		{"no quiet period", Config{MaxAge: time.Minute, MaxSpans: 1}, w},
		{"max age under the quiet period", Config{Quiet: time.Minute, MaxAge: time.Second, MaxSpans: 1}, w},
		{"no room for spans", Config{Quiet: time.Second, MaxAge: time.Minute}, w},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg, tt.out); err == nil {
				t.Error("New succeeded")
			}
		})
	}
}

func TestNewDefaultsTheClock(t *testing.T) {
	s, err := New(Config{Quiet: time.Second, MaxAge: time.Second, MaxSpans: 1}, newWriter())
	if err != nil {
		t.Fatal(err)
	}
	if s.cfg.Now == nil {
		t.Error("no clock was set")
	}
}

func TestATraceWaitsForTheQuietPeriod(t *testing.T) {
	s, c, w := newSampler(t, keepAll)
	accept(t, s, span(1, 0, 0, 10))

	c.advance(time.Second - time.Millisecond)
	sweep(t, s)
	if got := s.Stats().Traces; got != 1 {
		t.Fatalf("trace judged before its quiet period was up; %d still buffered", got)
	}

	c.advance(time.Millisecond)
	sweep(t, s)
	if got := len(w.spans(testTrace)); got != 1 {
		t.Fatalf("wrote %d spans once the quiet period was up, want 1", got)
	}
	if got := s.Stats().Traces; got != 0 {
		t.Errorf("%d traces still buffered after being judged", got)
	}
}

func TestANewSpanRestartsTheQuietPeriod(t *testing.T) {
	s, c, w := newSampler(t, keepAll)
	accept(t, s, span(1, 0, 0, 100))

	c.advance(800 * time.Millisecond)
	accept(t, s, span(2, 1, 10, 20))

	c.advance(400 * time.Millisecond)
	sweep(t, s)
	if len(w.spans(testTrace)) != 0 {
		t.Fatal("judged a trace that had a span 400ms ago")
	}

	c.advance(600 * time.Millisecond)
	sweep(t, s)
	if got := len(w.spans(testTrace)); got != 2 {
		t.Errorf("wrote %d spans, want both", got)
	}
}

func TestABusyTraceIsJudgedAtMaxAge(t *testing.T) {
	s, c, w := newSampler(t, keepAll)

	// A span every half second never lets the trace go quiet.
	for i := range byte(10) {
		accept(t, s, span(i+1, 0, 0, 10))
		c.advance(500 * time.Millisecond)
		sweep(t, s)
		if written := len(w.spans(testTrace)) > 0; written != (i == 9) {
			t.Fatalf("after %v: written = %v", time.Duration(i+1)*500*time.Millisecond, written)
		}
	}

	if got := s.Stats().SealedByAge; got != 1 {
		t.Errorf("SealedByAge = %d, want 1", got)
	}
}

// The spans' own timestamps come from other machines. A trace claiming to
// be from last year must still get its full quiet period.
func TestSpanTimestampsDoNotDecideSealing(t *testing.T) {
	s, _, w := newSampler(t, keepAll)
	old := span(1, 0, 0, 10)
	old.Start, old.End = old.Start.AddDate(-1, 0, 0), old.End.AddDate(-1, 0, 0)
	accept(t, s, old)

	sweep(t, s)
	if len(w.spans(testTrace)) != 0 {
		t.Error("an old timestamp got the trace judged on arrival")
	}
}

func TestOnlyKeptTracesAreWritten(t *testing.T) {
	s, c, w := newSampler(t, []Policy{KeepErrors{}})
	accept(t, s,
		in(1, span(1, 0, 0, 100)),
		in(1, failed(span(2, 1, 10, 20))),
		in(2, span(1, 0, 0, 100)),
		in(2, span(2, 1, 10, 20)),
		in(2, span(3, 1, 30, 40)),
	)
	c.advance(time.Second)
	sweep(t, s)

	if got := len(w.spans(traceID(1))); got != 2 {
		t.Errorf("wrote %d spans of the failed trace, want 2", got)
	}
	if got := len(w.spans(traceID(2))); got != 0 {
		t.Errorf("wrote %d spans of the clean trace, want none", got)
	}

	st := s.Stats()
	if st.Kept != 1 || st.Dropped != 1 || st.KeptSpans != 2 || st.DroppedSpans != 3 {
		t.Errorf("kept %d (%d spans), dropped %d (%d spans), want 1 (2) and 1 (3)",
			st.Kept, st.KeptSpans, st.Dropped, st.DroppedSpans)
	}
	if st.Reasons["error"] != 1 || st.Reasons["unclaimed"] != 1 {
		t.Errorf("Reasons = %v", st.Reasons)
	}
	if st.Accepted != 5 || st.Buffered != 0 {
		t.Errorf("Accepted = %d, Buffered = %d, want 5 and 0", st.Accepted, st.Buffered)
	}
}

func TestTracesAreJudgedIndependently(t *testing.T) {
	s, c, w := newSampler(t, keepAll)
	accept(t, s, in(1, span(1, 0, 0, 10)))
	c.advance(700 * time.Millisecond)
	accept(t, s, in(2, span(1, 0, 0, 10)))
	c.advance(300 * time.Millisecond)
	sweep(t, s)

	if len(w.spans(traceID(1))) != 1 || len(w.spans(traceID(2))) != 0 {
		t.Error("the quiet trace and the recent one were not told apart")
	}
	if st := s.Stats(); st.Traces != 1 || st.Buffered != 1 {
		t.Errorf("Traces = %d, Buffered = %d, want 1 and 1", st.Traces, st.Buffered)
	}
}

func TestAWriterFailureIsCountedAndDoesNotStopTheSweep(t *testing.T) {
	s, c, w := newSampler(t, keepAll)
	w.fail[traceID(1)] = true
	accept(t, s,
		in(1, span(1, 0, 0, 10)),
		in(1, span(2, 1, 1, 5)),
		in(2, span(1, 0, 0, 10)),
	)
	c.advance(time.Second)

	err := s.Sweep(context.Background())
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("Sweep error = %v, want the writer's", err)
	}
	if len(w.spans(traceID(2))) != 1 {
		t.Error("the trace after the failure was not written")
	}
	if st := s.Stats(); st.Lost != 2 || st.Kept != 1 {
		t.Errorf("Lost = %d, Kept = %d, want 2 and 1", st.Lost, st.Kept)
	}
}

func TestStatsIsASnapshot(t *testing.T) {
	s, c, _ := newSampler(t, keepAll)
	accept(t, s, span(1, 0, 0, 10))
	c.advance(time.Second)
	sweep(t, s)

	st := s.Stats()
	st.Reasons["all"] = 99
	if s.Stats().Reasons["all"] != 1 {
		t.Error("changing a snapshot changed the sampler's counters")
	}
}

func TestConcurrentAcceptAndSweep(t *testing.T) {
	s, c, w := newSampler(t, keepAll)

	var wg sync.WaitGroup
	for g := range byte(8) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range byte(50) {
				accept(t, s, in(g, span(i+1, 0, 0, 10)))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			_ = s.Sweep(context.Background())
		}
	}()
	wg.Wait()

	c.advance(time.Hour)
	sweep(t, s)

	total := 0
	for g := range byte(8) {
		total += len(w.spans(traceID(g)))
	}
	if total != 8*50 {
		t.Errorf("wrote %d spans in total, want all %d", total, 8*50)
	}
}

func smallSampler(t *testing.T, maxSpans int) (*Sampler, *clock, *writer) {
	t.Helper()
	c, w := newClock(), newWriter()
	s, err := New(Config{Quiet: time.Second, MaxAge: 5 * time.Second, MaxSpans: maxSpans, Policies: keepAll, Now: c.Now}, w)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, c, w
}

func TestAFullBufferRefusesWithBackpressure(t *testing.T) {
	s, c, w := smallSampler(t, 3)
	accept(t, s, in(1, span(1, 0, 0, 10)), in(1, span(2, 1, 1, 5)))

	// Two more would make four. The whole batch is refused, including the
	// span that would have fit.
	err := s.Accept(context.Background(), []trace.Span{in(2, span(1, 0, 0, 10)), in(2, span(2, 1, 1, 5))})
	if !errors.Is(err, ingest.ErrSinkFull) {
		t.Fatalf("Accept error = %v, want ingest.ErrSinkFull", err)
	}
	if st := s.Stats(); st.Refused != 2 || st.Buffered != 2 || st.Traces != 1 {
		t.Errorf("Refused = %d, Buffered = %d, Traces = %d, want 2, 2, 1", st.Refused, st.Buffered, st.Traces)
	}

	// Filling it exactly is fine.
	accept(t, s, in(2, span(1, 0, 0, 10)))

	// Once a sweep has judged what was there, the retry gets in.
	c.advance(time.Second)
	sweep(t, s)
	accept(t, s, in(3, span(1, 0, 0, 10)), in(3, span(2, 1, 1, 5)))

	if len(w.spans(traceID(1))) != 2 || len(w.spans(traceID(2))) != 1 {
		t.Error("the traces buffered before the refusal were not written")
	}
}

func TestABatchLargerThanTheBufferIsNotRetryable(t *testing.T) {
	s, _, _ := smallSampler(t, 2)
	err := s.Accept(context.Background(), []trace.Span{span(1, 0, 0, 10), span(2, 1, 1, 5), span(3, 1, 6, 9)})
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("Accept error = %v, want ErrBatchTooLarge", err)
	}
	if errors.Is(err, ingest.ErrSinkFull) {
		t.Error("an oversized batch looks retryable; the client would resend it forever")
	}
}

func TestFlushJudgesEverythingNow(t *testing.T) {
	s, _, w := newSampler(t, keepAll)
	accept(t, s, in(1, span(1, 0, 0, 10)), in(2, span(2, 9, 1, 5)))

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(w.spans(traceID(1))) != 1 || len(w.spans(traceID(2))) != 1 {
		t.Error("Flush left traces unjudged")
	}
	if st := s.Stats(); st.Traces != 0 || st.Buffered != 0 {
		t.Errorf("Traces = %d, Buffered = %d after Flush", st.Traces, st.Buffered)
	}
}

// ctxWriter fails any write whose context is already done, the way a store
// honouring cancellation would.
type ctxWriter struct{ *writer }

func (w ctxWriter) Write(ctx context.Context, spans []trace.Span) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.writer.Write(ctx, spans)
}

func TestRunSweepsAndFlushesOnShutdown(t *testing.T) {
	c, w := newClock(), newWriter()
	s, err := New(Config{Quiet: time.Second, MaxAge: time.Minute, MaxSpans: 100, Policies: keepAll, Now: c.Now}, ctxWriter{w})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, time.Millisecond) }()

	// A quiet trace is picked up by a tick.
	accept(t, s, in(1, span(1, 0, 0, 10)))
	c.advance(time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for len(w.spans(traceID(1))) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Run never swept the quiet trace")
		}
		time.Sleep(time.Millisecond)
	}

	// A trace still in flight at shutdown is flushed, on a context the
	// shutdown did not cancel.
	accept(t, s, in(2, span(1, 0, 0, 10)))
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.spans(traceID(2))) != 1 {
		t.Error("the in-flight trace was not flushed on shutdown")
	}
}

func TestRunLogsSweepErrors(t *testing.T) {
	var logs bytes.Buffer
	c, w := newClock(), newWriter()
	w.fail[traceID(1)] = true
	s, err := New(Config{
		Quiet: time.Second, MaxAge: time.Minute, MaxSpans: 100, Policies: keepAll, Now: c.Now,
		Logger: slog.New(slog.NewTextHandler(&syncWriter{w: &logs}, nil)),
	}, w)
	if err != nil {
		t.Fatal(err)
	}

	accept(t, s, in(1, span(1, 0, 0, 10)))
	c.advance(time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, time.Millisecond) }()

	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().Lost == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the failing write never happened")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(logs.String(), "disk full") {
		t.Errorf("the sweep error was not logged; logs:\n%s", logs.String())
	}
}

// syncWriter serialises writes to a buffer shared between the Run goroutine
// and the test.
type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}
