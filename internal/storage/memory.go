package storage

import (
	"context"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// minuteIndexLimit caps how many one-minute buckets a time-window search will
// walk before it stops using the time index and scans instead. A day is 1440
// buckets; past that, the union of buckets costs more to build than the scan
// it was meant to save.
const minuteIndexLimit = 24 * 60

type idSet map[trace.TraceID]struct{}

type opKey struct{ service, name string }

// entry is one stored trace.
type entry struct {
	spans  []trace.Span
	bySpan map[trace.SpanID]int
	start  time.Time
	end    time.Time

	// keys records every index bucket this trace was ever added to. Removing
	// a trace uses these, not its current spans: a replaced span may have
	// been indexed under keys its replacement does not have, and recomputing
	// from the spans would leave the old entries behind forever.
	keys indexKeys
}

type indexKeys struct {
	services   map[string]struct{}
	operations map[opKey]struct{}
	durations  map[int]struct{}
	minutes    map[int64]struct{}
}

// Memory is a Store that keeps everything in process memory, with secondary
// indexes so a search does not have to look at every trace.
//
// It is the store tests run against, and the working set the persistent
// store rebuilds on open. Spans older than the retention window are removed
// by EvictExpired.
type Memory struct {
	mu        sync.RWMutex
	closed    bool
	now       func() time.Time
	retention time.Duration

	traces map[trace.TraceID]*entry

	byService   map[string]idSet
	byOperation map[opKey]idSet

	// byDuration buckets traces by the bit length of each span's duration in
	// nanoseconds, so bucket n holds spans between 2^(n-1) and 2^n. A slow
	// span search starts at the bucket its minimum falls in and skips every
	// faster bucket outright; the handful of spans in the boundary bucket are
	// checked exactly afterwards.
	byDuration [65]idSet

	// byMinute buckets traces by the minute each of their spans started.
	byMinute map[int64]idSet
}

// NewMemory returns an empty store. A zero retention keeps spans until the
// process exits. A nil now uses the wall clock; tests pass their own.
func NewMemory(retention time.Duration, now func() time.Time) *Memory {
	if now == nil {
		now = time.Now
	}
	m := &Memory{
		now:         now,
		retention:   retention,
		traces:      make(map[trace.TraceID]*entry),
		byService:   make(map[string]idSet),
		byOperation: make(map[opKey]idSet),
		byMinute:    make(map[int64]idSet),
	}
	for i := range m.byDuration {
		m.byDuration[i] = make(idSet)
	}
	return m
}

// Write stores spans. The batch is validated before anything is stored, so a
// batch with one invalid span stores none of them rather than half.
func (m *Memory) Write(_ context.Context, spans []trace.Span) error {
	for i := range spans {
		if err := spans[i].Validate(); err != nil {
			return fmt.Errorf("storage: span %d: %w", i, err)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}
	for i := range spans {
		m.insert(spans[i])
	}
	return nil
}

// Accept lets the store sit directly behind the ingest path until the sampler
// exists to go in between.
func (m *Memory) Accept(ctx context.Context, spans []trace.Span) error {
	return m.Write(ctx, spans)
}

func (m *Memory) insert(s trace.Span) {
	e, ok := m.traces[s.TraceID]
	if !ok {
		e = &entry{
			bySpan: make(map[trace.SpanID]int),
			start:  s.Start,
			end:    s.End,
			keys: indexKeys{
				services:   make(map[string]struct{}),
				operations: make(map[opKey]struct{}),
				durations:  make(map[int]struct{}),
				minutes:    make(map[int64]struct{}),
			},
		}
		m.traces[s.TraceID] = e
	}

	if i, dup := e.bySpan[s.SpanID]; dup {
		e.spans[i] = s
	} else {
		e.bySpan[s.SpanID] = len(e.spans)
		e.spans = append(e.spans, s)
	}

	// A duplicate can only widen the trace's bounds, never shrink them. That
	// is deliberately simple: a retried span is normally identical, and
	// recomputing bounds from every span on every write would make writes
	// grow with trace size.
	if s.Start.Before(e.start) {
		e.start = s.Start
	}
	if s.End.After(e.end) {
		e.end = s.End
	}

	id := s.TraceID
	addTo(m.byService, s.Service, id)
	e.keys.services[s.Service] = struct{}{}

	op := opKey{s.Service, s.Name}
	addTo(m.byOperation, op, id)
	e.keys.operations[op] = struct{}{}

	b := durationBucket(s.Duration())
	m.byDuration[b][id] = struct{}{}
	e.keys.durations[b] = struct{}{}

	minute := s.Start.Unix() / 60
	addTo(m.byMinute, minute, id)
	e.keys.minutes[minute] = struct{}{}
}

// Trace returns every span of one trace, ordered by start time.
func (m *Memory) Trace(_ context.Context, id trace.TraceID) ([]trace.Span, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrClosed
	}
	e, ok := m.traces[id]
	if !ok {
		return nil, ErrNotFound
	}

	out := slices.Clone(e.spans)
	slices.SortStableFunc(out, func(a, b trace.Span) int { return a.Start.Compare(b.Start) })
	return out, nil
}

// Find returns summaries of matching traces, newest first.
//
// An index only narrows the candidates. Every candidate is then checked
// against the full query, so an index being coarse - a duration bucket, a
// minute, a service a trace has since lost a span from - can cost time but
// never a wrong answer.
func (m *Memory) Find(_ context.Context, q Query) ([]Summary, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrClosed
	}

	var results []Summary
	for id := range m.candidates(q) {
		e := m.traces[id]
		if e == nil || !q.overlaps(e.start, e.end) {
			continue
		}
		if q.hasSpanPredicate() && !slices.ContainsFunc(e.spans, func(s trace.Span) bool { return q.matchesSpan(&s) }) {
			continue
		}
		results = append(results, summarise(id, e))
	}

	slices.SortFunc(results, func(a, b Summary) int {
		if c := b.Start.Compare(a.Start); c != 0 {
			return c
		}
		return slices.Compare(a.TraceID[:], b.TraceID[:])
	})

	if n := q.limit(); len(results) > n {
		results = results[:n]
	}
	return results, nil
}

// candidates picks the most selective index the query can use. The order is
// a judgement about typical selectivity rather than a measurement: an
// operation within a service is narrower than the service, a service is
// narrower than a bounded time window in a busy system, and a duration floor
// is the loosest because most searches that set one set it low.
func (m *Memory) candidates(q Query) idSet {
	switch {
	case q.Operation != "":
		return m.byOperation[opKey{q.Service, q.Operation}]
	case q.Service != "":
		return m.byService[q.Service]
	}

	if !q.Start.IsZero() && !q.End.IsZero() {
		first, last := q.Start.Unix()/60, q.End.Unix()/60
		// A trace's spans can start before the window and still overlap it,
		// so the time index only helps when it can be used exactly: every
		// span in the window started inside it. Widen by the longest trace
		// we would reasonably keep rather than miss those - here, a day
		// either side, after which the scan is cheaper anyway.
		if last-first <= minuteIndexLimit {
			set := make(idSet)
			for minute := first - minuteIndexLimit; minute <= last; minute++ {
				for id := range m.byMinute[minute] {
					set[id] = struct{}{}
				}
			}
			return set
		}
	}

	if q.MinDuration > 0 {
		set := make(idSet)
		for b := durationBucket(q.MinDuration); b < len(m.byDuration); b++ {
			for id := range m.byDuration[b] {
				set[id] = struct{}{}
			}
		}
		return set
	}

	all := make(idSet, len(m.traces))
	for id := range m.traces {
		all[id] = struct{}{}
	}
	return all
}

// EvictExpired removes every trace that ended before the retention window,
// and reports how many it removed. A zero retention removes nothing.
//
// A trace is judged by when it ended, not when it started. Evicting a
// long-running trace because its first span is old would delete the spans
// that arrived a minute ago along with it.
func (m *Memory) EvictExpired() int {
	if m.retention <= 0 {
		return 0
	}
	cutoff := m.now().Add(-m.retention)

	m.mu.Lock()
	defer m.mu.Unlock()

	var expired []trace.TraceID
	for id, e := range m.traces {
		if e.end.Before(cutoff) {
			expired = append(expired, id)
		}
	}
	for _, id := range expired {
		m.remove(id)
	}
	return len(expired)
}

func (m *Memory) remove(id trace.TraceID) {
	e := m.traces[id]
	delete(m.traces, id)

	for svc := range e.keys.services {
		removeFrom(m.byService, svc, id)
	}
	for op := range e.keys.operations {
		removeFrom(m.byOperation, op, id)
	}
	for b := range e.keys.durations {
		delete(m.byDuration[b], id)
	}
	for minute := range e.keys.minutes {
		removeFrom(m.byMinute, minute, id)
	}
}

// Len reports how many traces are stored.
func (m *Memory) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.traces)
}

// Close releases the store. Every later call fails with ErrClosed.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.traces = nil
	return nil
}

func summarise(id trace.TraceID, e *entry) Summary {
	sum := Summary{
		TraceID:  id,
		Start:    e.start,
		Duration: e.end.Sub(e.start),
		Spans:    len(e.spans),
	}

	var head *trace.Span
	for i := range e.spans {
		s := &e.spans[i]
		if s.Failed() {
			sum.Failed = true
		}
		if s.IsRoot() {
			head = s
			sum.Complete = true
		} else if !sum.Complete && (head == nil || s.Start.Before(head.Start)) {
			head = s
		}
	}
	sum.RootService = head.Service
	sum.RootName = head.Name
	return sum
}

func durationBucket(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return bits.Len64(uint64(d))
}

func addTo[K comparable](index map[K]idSet, key K, id trace.TraceID) {
	set, ok := index[key]
	if !ok {
		set = make(idSet)
		index[key] = set
	}
	set[id] = struct{}{}
}

func removeFrom[K comparable](index map[K]idSet, key K, id trace.TraceID) {
	set := index[key]
	delete(set, id)
	if len(set) == 0 {
		delete(index, key)
	}
}
