package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	ctx  = context.Background()
	base = time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) set(t time.Time) { c.mu.Lock(); defer c.mu.Unlock(); c.t = t }

// spec describes a span compactly for tests. Identifiers are single bytes
// repeated to full width, which keeps tables readable.
type spec struct {
	traceID, spanID, parentID byte
	service, name             string
	offset, duration          time.Duration
	failed                    bool
}

func (s spec) span() trace.Span {
	var sp trace.Span
	for i := range sp.TraceID {
		sp.TraceID[i] = s.traceID
	}
	for i := range sp.SpanID {
		sp.SpanID[i] = s.spanID
	}
	if s.parentID != 0 {
		for i := range sp.ParentSpanID {
			sp.ParentSpanID[i] = s.parentID
		}
	}
	sp.Service, sp.Name = s.service, s.name
	sp.Start = base.Add(s.offset)
	sp.End = sp.Start.Add(s.duration)
	if s.failed {
		sp.Status.Code = trace.StatusError
	}
	return sp
}

func tid(b byte) trace.TraceID {
	var id trace.TraceID
	for i := range id {
		id[i] = b
	}
	return id
}

func write(t *testing.T, s Store, specs ...spec) {
	t.Helper()
	spans := make([]trace.Span, len(specs))
	for i, sp := range specs {
		spans[i] = sp.span()
	}
	if err := s.Write(ctx, spans); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func ids(sums []Summary) []trace.TraceID {
	out := make([]trace.TraceID, len(sums))
	for i, s := range sums {
		out[i] = s.TraceID
	}
	return out
}

func TestTraceReturnsSpansInStartOrder(t *testing.T) {
	m := NewMemory(0, nil)
	write(t, m,
		spec{1, 3, 1, "db", "UPDATE", 30 * time.Millisecond, 5 * time.Millisecond, false},
		spec{1, 1, 0, "api", "POST /pay", 0, 100 * time.Millisecond, false},
		spec{1, 2, 1, "card", "charge", 10 * time.Millisecond, 20 * time.Millisecond, false},
	)

	spans, err := m.Trace(ctx, tid(1))
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	var names []string
	for _, s := range spans {
		names = append(names, s.Name)
	}
	if fmt.Sprint(names) != "[POST /pay charge UPDATE]" {
		t.Errorf("order = %v", names)
	}

	if _, err := m.Trace(ctx, tid(9)); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown trace: err = %v, want ErrNotFound", err)
	}
}

// Exporters resend a whole batch when told to retry, so the same span can
// arrive more than once. Storing it twice would double-count it in every
// summary and put it in the waterfall twice.
func TestRetriedSpanReplacesTheOriginal(t *testing.T) {
	m := NewMemory(0, nil)
	write(t, m, spec{1, 1, 0, "api", "POST /pay", 0, 100 * time.Millisecond, false})
	write(t, m, spec{1, 1, 0, "api", "POST /pay", 0, 100 * time.Millisecond, true})

	spans, err := m.Trace(ctx, tid(1))
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("stored %d copies of one span, want 1", len(spans))
	}
	if !spans[0].Failed() {
		t.Error("the later copy did not replace the earlier one")
	}
}

// The batch is validated before anything is stored. Half a batch in the store
// and an error to the client would make the client retry, and the half that
// landed would then be written again.
func TestWriteIsAllOrNothing(t *testing.T) {
	m := NewMemory(0, nil)
	good := spec{1, 1, 0, "api", "POST /pay", 0, time.Millisecond, false}.span()
	bad := spec{2, 1, 0, "api", "", 0, time.Millisecond, false}.span()

	err := m.Write(ctx, []trace.Span{good, bad})
	if !errors.Is(err, trace.ErrNoName) {
		t.Fatalf("Write = %v, want ErrNoName", err)
	}
	if m.Len() != 0 {
		t.Errorf("stored %d traces from a rejected batch", m.Len())
	}
}

func TestFind(t *testing.T) {
	m := NewMemory(0, nil)
	write(t, m,
		// Trace 1: slow checkout.
		spec{1, 1, 0, "checkout", "POST /pay", 0, 900 * time.Millisecond, false},
		spec{1, 2, 1, "db", "UPDATE orders", 10 * time.Millisecond, 5 * time.Millisecond, false},
		// Trace 2: fast checkout, slow db.
		spec{2, 1, 0, "checkout", "POST /pay", time.Minute, 20 * time.Millisecond, false},
		spec{2, 2, 1, "db", "UPDATE orders", time.Minute, 2 * time.Second, true},
		// Trace 3: a different checkout operation, later.
		spec{3, 1, 0, "checkout", "GET /cart", 10 * time.Minute, 50 * time.Millisecond, false},
		// Trace 4: unrelated, much later.
		spec{4, 1, 0, "search", "GET /q", 3 * time.Hour, time.Second, false},
	)

	tests := []struct {
		name string
		q    Query
		want []byte // trace ids, newest first
	}{
		{"everything", Query{}, []byte{4, 3, 2, 1}},
		{"by service", Query{Service: "checkout"}, []byte{3, 2, 1}},
		{"by operation", Query{Service: "checkout", Operation: "POST /pay"}, []byte{2, 1}},
		{"unknown service", Query{Service: "nope"}, nil},
		{
			// Trace 2 has a slow span, but not a slow checkout span. Matching
			// it would answer a question nobody asked.
			name: "duration applies to the same span as the service",
			q:    Query{Service: "checkout", MinDuration: 500 * time.Millisecond},
			want: []byte{1},
		},
		{"minimum is inclusive", Query{Service: "checkout", MinDuration: 900 * time.Millisecond}, []byte{1}},
		{"maximum is inclusive", Query{Service: "checkout", MaxDuration: 50 * time.Millisecond}, []byte{3, 2}},
		{"duration without a service", Query{MinDuration: time.Second}, []byte{4, 2}},
		{"window overlapping one trace", Query{Start: base.Add(59 * time.Second), End: base.Add(61 * time.Second)}, []byte{2}},
		{"window inside a long span", Query{Start: base.Add(100 * time.Millisecond), End: base.Add(200 * time.Millisecond)}, []byte{1}},
		{"window before everything", Query{Start: base.Add(-time.Hour), End: base.Add(-time.Minute)}, nil},
		{"open ended window", Query{Start: base.Add(5 * time.Minute)}, []byte{4, 3}},
		{"window wider than the time index", Query{Start: base.Add(-48 * time.Hour), End: base.Add(48 * time.Hour)}, []byte{4, 3, 2, 1}},
		{"limit keeps the newest", Query{Limit: 2}, []byte{4, 3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := m.Find(ctx, tc.q)
			if err != nil {
				t.Fatalf("Find: %v", err)
			}
			want := make([]trace.TraceID, len(tc.want))
			for i, b := range tc.want {
				want[i] = tid(b)
			}
			if fmt.Sprint(ids(got)) != fmt.Sprint(want) {
				t.Errorf("Find(%+v) = %v, want trace ids %v", tc.q, len(got), tc.want)
				for _, s := range got {
					t.Logf("  got %x", s.TraceID[0])
				}
			}
		})
	}

	if _, err := m.Find(ctx, Query{Operation: "POST /pay"}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("invalid query: err = %v, want ErrBadQuery", err)
	}
}

func TestSummary(t *testing.T) {
	m := NewMemory(0, nil)
	write(t, m,
		spec{1, 2, 1, "db", "UPDATE", 10 * time.Millisecond, 5 * time.Millisecond, true},
		spec{1, 1, 0, "api", "POST /pay", 0, 100 * time.Millisecond, false},
		// Trace 2's root never arrived.
		spec{2, 3, 9, "card", "refund", 2 * time.Second, 10 * time.Millisecond, false},
		spec{2, 2, 9, "card", "charge", time.Second, 10 * time.Millisecond, false},
	)

	got, err := m.Find(ctx, Query{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d summaries, want 2", len(got))
	}

	partial, full := got[0], got[1]

	if !full.Complete || full.RootService != "api" || full.RootName != "POST /pay" {
		t.Errorf("full trace summary = %+v", full)
	}
	if full.Spans != 2 || !full.Failed || full.Duration != 100*time.Millisecond || !full.Start.Equal(base) {
		t.Errorf("full trace counts = %+v", full)
	}

	// With no root, the earliest span stands in, and Complete says so.
	if partial.Complete || partial.RootName != "charge" {
		t.Errorf("partial trace summary = %+v, want the earliest span and Complete false", partial)
	}
	if partial.Duration != time.Second+10*time.Millisecond {
		t.Errorf("partial trace duration = %v", partial.Duration)
	}
}

func TestEvictExpired(t *testing.T) {
	c := &clock{t: base}
	m := NewMemory(time.Hour, c.now)

	write(t, m,
		spec{1, 1, 0, "old", "a", 0, time.Minute, false},
		// Trace 2 started long ago but is still running, so it must stay.
		spec{2, 1, 0, "long", "b", 0, time.Minute, false},
		spec{2, 2, 1, "long", "c", 100 * time.Minute, time.Minute, false},
		spec{3, 1, 0, "new", "d", 90 * time.Minute, time.Minute, false},
	)

	c.set(base.Add(2 * time.Hour))
	if n := m.EvictExpired(); n != 1 {
		t.Fatalf("EvictExpired removed %d traces, want 1", n)
	}
	if _, err := m.Trace(ctx, tid(1)); !errors.Is(err, ErrNotFound) {
		t.Error("an expired trace is still readable")
	}
	if _, err := m.Trace(ctx, tid(2)); err != nil {
		t.Errorf("a trace that ended inside the window was evicted: %v", err)
	}

	// The indexes must forget the trace too, or they grow forever and every
	// search pays for traces that no longer exist.
	if _, ok := m.byService["old"]; ok {
		t.Error("byService still holds an evicted trace's service")
	}
	if _, ok := m.byOperation[opKey{"old", "a"}]; ok {
		t.Error("byOperation still holds an evicted trace's operation")
	}
	for b, set := range m.byDuration {
		if _, ok := set[tid(1)]; ok {
			t.Errorf("duration bucket %d still holds an evicted trace", b)
		}
	}
	if _, ok := m.byMinute[base.Unix()/60][tid(1)]; ok {
		t.Error("byMinute still holds an evicted trace")
	}
}

// A replacement span can carry different index keys from the one it replaced.
// Removal has to clear the keys the trace was ever indexed under, not just the
// ones its current spans have, or the old keys leak.
func TestEvictionForgetsKeysOfReplacedSpans(t *testing.T) {
	c := &clock{t: base}
	m := NewMemory(time.Minute, c.now)

	write(t, m, spec{1, 1, 0, "before", "op", 0, time.Second, false})
	write(t, m, spec{1, 1, 0, "after", "op", 0, time.Second, false})

	c.set(base.Add(time.Hour))
	m.EvictExpired()

	if len(m.byService) != 0 || len(m.byOperation) != 0 || len(m.byMinute) != 0 {
		t.Errorf("indexes leaked after eviction: services %v, operations %v, minutes %v", m.byService, m.byOperation, m.byMinute)
	}
}

func TestZeroRetentionKeepsEverything(t *testing.T) {
	c := &clock{t: base}
	m := NewMemory(0, c.now)
	write(t, m, spec{1, 1, 0, "api", "a", 0, time.Second, false})

	c.set(base.Add(1000 * time.Hour))
	if n := m.EvictExpired(); n != 0 || m.Len() != 1 {
		t.Errorf("zero retention evicted %d traces", n)
	}
}

func TestClosedStore(t *testing.T) {
	m := NewMemory(0, nil)
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := m.Write(ctx, []trace.Span{spec{1, 1, 0, "a", "b", 0, time.Second, false}.span()}); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v", err)
	}
	if _, err := m.Trace(ctx, tid(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Trace after Close = %v", err)
	}
	if _, err := m.Find(ctx, Query{}); !errors.Is(err, ErrClosed) {
		t.Errorf("Find after Close = %v", err)
	}
}

func TestAcceptWrites(t *testing.T) {
	m := NewMemory(0, nil)
	if err := m.Accept(ctx, []trace.Span{spec{1, 1, 0, "a", "b", 0, time.Second, false}.span()}); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d after Accept, want 1", m.Len())
	}
}

// Many exports arrive at once and searches run while they do. This is here
// for the race detector more than for its assertions.
func TestConcurrentWritesAndReads(t *testing.T) {
	// A fixed clock keeps every span inside retention, so eviction still runs
	// alongside the writes without being allowed to delete what they wrote.
	c := &clock{t: base}
	m := NewMemory(time.Hour, c.now)
	const writers, perWriter = 8, 200

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				s := spec{byte(w + 1), byte(i%250 + 1), 0, fmt.Sprintf("svc-%d", w), "op", time.Duration(i) * time.Millisecond, time.Millisecond, false}.span()
				if err := m.Write(ctx, []trace.Span{s}); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := m.Find(ctx, Query{Service: "svc-1"}); err != nil {
					t.Errorf("Find: %v", err)
					return
				}
				_, _ = m.Trace(ctx, tid(1))
				m.EvictExpired()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	if m.Len() != writers {
		t.Errorf("Len = %d, want %d", m.Len(), writers)
	}
}

func TestDurationBucket(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want int
	}{
		{0, 0},
		{-time.Second, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{time.Duration(1<<62 + 1), 63},
	}
	for _, tc := range tests {
		if got := durationBucket(tc.d); got != tc.want {
			t.Errorf("durationBucket(%d) = %d, want %d", tc.d, got, tc.want)
		}
	}
}

func BenchmarkMemoryWrite(b *testing.B) {
	m := NewMemory(0, nil)
	batch := make([]trace.Span, 100)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := range batch {
			batch[i] = spec{byte(n%250 + 1), byte(i%250 + 1), 0, "api", "op", time.Duration(n) * time.Millisecond, time.Millisecond, false}.span()
			// Spread across many traces so the benchmark measures inserting
			// into indexes, not replacing one trace's spans over and over.
			batch[i].TraceID[15] = byte(n >> 8)
			batch[i].TraceID[14] = byte(n >> 16)
		}
		if err := m.Write(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(batch))/b.Elapsed().Seconds(), "spans/s")
}
