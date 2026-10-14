package sampler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/assembly"
	"github.com/sahilkalgutkar/spandrel/internal/ingest"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// ErrBatchTooLarge is returned for a batch with more spans than the buffer
// can ever hold. It deliberately does not wrap ingest.ErrSinkFull: a retry
// would be refused the same way, forever, so the client has to be told to
// stop rather than to back off.
var ErrBatchTooLarge = errors.New("sampler: batch is larger than the whole buffer")

// Writer is where kept traces go. The store satisfies it.
type Writer interface {
	Write(ctx context.Context, spans []trace.Span) error
}

// Config sets when a buffered trace counts as finished and how it is judged.
type Config struct {
	// Quiet is how long a trace must go without a new span before it is
	// judged. No process knows when a distributed trace has ended, so silence
	// is the best available signal. Too short and slow traces get judged in
	// pieces. Too long and every trace sits in memory for that long.
	Quiet time.Duration

	// MaxAge is how long a trace can stay buffered however busy it is. A
	// trace that never goes quiet, because of a leak or a stream that never
	// stops, would otherwise stay in memory forever.
	MaxAge time.Duration

	// MaxSpans caps how many spans can be buffered at once, across all
	// traces. Past it, Accept refuses with ingest.ErrSinkFull, which the
	// export handlers turn into a retryable error, so the pressure goes back
	// to the clients instead of into this process's memory.
	MaxSpans int

	// Policies judge each trace once it is sealed. See Decide.
	Policies []Policy

	// Remember is how long a verdict is kept after a trace is judged, so that
	// spans arriving later follow it: written if the trace was kept, dropped
	// if not. Without it, a late span opens a fresh buffer and is judged on
	// its own, and a late span from a kept error trace usually is not an
	// error itself, so it gets dropped from a trace that was kept. Zero
	// remembers nothing.
	Remember time.Duration

	// MaxDecisions caps how many verdicts are remembered at once. Past it the
	// oldest is forgotten first, which is the one least likely to see another
	// span.
	MaxDecisions int

	// ServiceRate caps how many traces a second each service can have kept,
	// counting a trace against the service of its root span. A service in
	// trouble fails every request it serves, KeepErrors keeps every one of
	// those traces, and without a cap that one service takes the whole
	// storage budget at the moment the others' traces matter most. The cap
	// applies to every kept trace, errors included: it still keeps
	// ServiceRate of them a second, which is plenty to see what went wrong.
	// Zero means no cap.
	ServiceRate float64

	// ServiceBurst is how many traces a service can have kept back to back
	// before ServiceRate starts to apply.
	ServiceBurst int

	// Now is the clock. Tests replace it; nil means time.Now.
	Now func() time.Time

	// Logger receives the errors Run cannot return. Nil discards them.
	Logger *slog.Logger
}

// Stats counts what the sampler has done since it started.
type Stats struct {
	// Accepted counts spans taken from clients, and Refused counts spans
	// turned away because the buffer was full. Refused spans were not lost:
	// the client was told to retry them.
	Accepted, Refused int64

	// Buffered is how many spans are held right now: waiting to be judged,
	// or late spans of kept traces waiting to be written. Traces is how many
	// traces are waiting to be judged.
	Buffered int
	Traces   int

	// Kept and Dropped count traces by verdict, and KeptSpans and
	// DroppedSpans count the spans in them.
	Kept, Dropped           int64
	KeptSpans, DroppedSpans int64

	// Reasons counts verdicts by the reason the deciding policy gave.
	Reasons map[string]int64

	// SealedByAge counts traces judged because they hit MaxAge rather than
	// going quiet. A steady rise means Quiet is too long or something is
	// leaking traces.
	SealedByAge int64

	// Lost counts spans of kept traces that the writer refused. They were
	// meant to be kept and were not, so this is the number to alert on.
	Lost int64

	// LateKept and LateDropped count spans that arrived after their trace
	// was judged, or while it was being judged, and followed its verdict.
	// Many of them mean Quiet is shorter than the gaps inside real traces.
	LateKept, LateDropped int64

	// Decisions is how many verdicts are remembered right now.
	Decisions int
}

// Sampler buffers spans by trace and judges each trace once it looks
// finished. It is an ingest sink: Accept is called from the export handlers.
type Sampler struct {
	cfg Config
	out Writer

	mu      sync.Mutex
	pending map[trace.TraceID]*pending
	stats   Stats

	// judging holds the traces taken out of pending and not yet decided, with
	// any spans that arrived for them in the meantime. Those spans wait for
	// the verdict instead of starting the trace over.
	judging map[trace.TraceID][]trace.Span

	// decided remembers verdicts, and order lists them oldest first so they
	// can be forgotten in that order. An entry in order whose time no longer
	// matches decided is stale: the trace was judged again since.
	decided map[trace.TraceID]decision
	order   []remembered

	// late holds spans of kept traces that arrived after the verdict, until
	// the next sweep writes them. Writing them from Accept would put the
	// store's latency on the ingest path.
	late map[trace.TraceID][]trace.Span

	// buckets hold each service's allowance of kept traces. A bucket that has
	// refilled is the same as no bucket, so sweeps remove those, and a
	// service that stops sending costs nothing.
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	at     time.Time
}

// refill adds what the rate has earned since the bucket was last touched, up
// to the burst.
func (b *bucket) refill(now time.Time, rate float64, burst int) {
	if elapsed := now.Sub(b.at); elapsed > 0 {
		b.tokens = min(float64(burst), b.tokens+rate*elapsed.Seconds())
		b.at = now
	}
}

type decision struct {
	keep bool
	at   time.Time
}

type remembered struct {
	id trace.TraceID
	at time.Time
}

type pending struct {
	id    trace.TraceID
	spans []trace.Span

	// first and last are when the sampler received the trace's first and
	// latest spans, by its own clock. The spans' own timestamps come from
	// other machines and can be skewed or wrong, so they are never used to
	// decide when to seal.
	first, last time.Time
}

// New returns a sampler that sends kept traces to out.
func New(cfg Config, out Writer) (*Sampler, error) {
	switch {
	case out == nil:
		return nil, errors.New("sampler: no writer")
	case cfg.Quiet <= 0:
		return nil, fmt.Errorf("sampler: quiet period must be positive, got %v", cfg.Quiet)
	case cfg.MaxAge < cfg.Quiet:
		return nil, fmt.Errorf("sampler: max age %v is shorter than the quiet period %v", cfg.MaxAge, cfg.Quiet)
	case cfg.MaxSpans <= 0:
		return nil, fmt.Errorf("sampler: max spans must be positive, got %d", cfg.MaxSpans)
	case cfg.Remember < 0:
		return nil, fmt.Errorf("sampler: remember must not be negative, got %v", cfg.Remember)
	case cfg.Remember > 0 && cfg.MaxDecisions <= 0:
		return nil, fmt.Errorf("sampler: remembering verdicts needs room for them, got max decisions %d", cfg.MaxDecisions)
	case !(cfg.ServiceRate >= 0):
		return nil, fmt.Errorf("sampler: service rate must not be negative, got %v", cfg.ServiceRate)
	case cfg.ServiceRate > 0 && cfg.ServiceBurst < 1:
		return nil, fmt.Errorf("sampler: a service rate needs a burst of at least 1, got %d", cfg.ServiceBurst)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Sampler{
		cfg:     cfg,
		out:     out,
		pending: make(map[trace.TraceID]*pending),
		stats:   Stats{Reasons: make(map[string]int64)},
		judging: make(map[trace.TraceID][]trace.Span),
		decided: make(map[trace.TraceID]decision),
		late:    make(map[trace.TraceID][]trace.Span),
		buckets: make(map[string]*bucket),
	}, nil
}

// Accept buffers spans until their traces are judged.
//
// A span whose trace has already been judged follows the verdict: it is
// queued for the next sweep to write if the trace was kept, and dropped if
// not. A span whose trace is being judged right now waits for the verdict.
//
// A batch that would take the buffer past MaxSpans is refused whole. Taking
// part of it would need a way to tell the client which part, and the protocol
// has none: a retry resends the whole batch either way. Late spans of dropped
// traces take no room, so they do not count.
func (s *Sampler) Accept(_ context.Context, spans []trace.Span) error {
	if len(spans) > s.cfg.MaxSpans {
		return fmt.Errorf("%w: %d spans, buffer holds %d", ErrBatchTooLarge, len(spans), s.cfg.MaxSpans)
	}
	now := s.cfg.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	held := 0
	for i := range spans {
		if keep, decided := s.verdict(spans[i].TraceID, now); !decided || keep {
			held++
		}
	}
	if s.stats.Buffered+held > s.cfg.MaxSpans {
		s.stats.Refused += int64(len(spans))
		return fmt.Errorf("%w: %d of %d spans buffered", ingest.ErrSinkFull, s.stats.Buffered, s.cfg.MaxSpans)
	}

	for _, sp := range spans {
		if waiting, ok := s.judging[sp.TraceID]; ok {
			s.judging[sp.TraceID] = append(waiting, sp)
			continue
		}
		if keep, decided := s.verdict(sp.TraceID, now); decided {
			if keep {
				s.late[sp.TraceID] = append(s.late[sp.TraceID], sp)
				s.stats.LateKept++
			} else {
				s.stats.LateDropped++
			}
			continue
		}
		p, ok := s.pending[sp.TraceID]
		if !ok {
			p = &pending{id: sp.TraceID, first: now}
			s.pending[sp.TraceID] = p
		}
		p.spans = append(p.spans, sp)
		p.last = now
	}
	s.stats.Accepted += int64(len(spans))
	s.stats.Buffered += held
	return nil
}

// verdict reports whether the trace has been judged and, if so, whether it
// was kept. A trace being judged right now counts as undecided but its spans
// are held, which is what the caller needs to size the batch. The lock must
// be held.
func (s *Sampler) verdict(id trace.TraceID, now time.Time) (keep, decided bool) {
	if _, ok := s.judging[id]; ok {
		return false, false
	}
	d, ok := s.decided[id]
	if !ok || now.Sub(d.at) >= s.cfg.Remember {
		return false, false
	}
	return d.keep, true
}

// Sweep judges every trace that has gone quiet or reached its maximum age,
// and writes the ones kept. It is meant to be called on a ticker.
//
// A writer error does not stop the sweep: the other traces are still judged
// and written, and the errors come back joined. The spans of a kept trace the
// writer refused are counted in Stats.Lost.
func (s *Sampler) Sweep(ctx context.Context) error {
	now := s.cfg.Now()
	err := s.judge(ctx, func(p *pending) bool {
		switch {
		case now.Sub(p.last) >= s.cfg.Quiet:
			return true
		case now.Sub(p.first) >= s.cfg.MaxAge:
			s.stats.SealedByAge++
			return true
		}
		return false
	})
	s.forget(now)
	return errors.Join(err, s.writeLate(ctx))
}

// Flush judges every buffered trace now, whether or not it looks finished.
//
// It is for shutdown. A trace cut off by a restart is judged on the spans
// that arrived, which is incomplete but honest. Dropping it unjudged would
// silently lose an error trace that happened to be in flight.
func (s *Sampler) Flush(ctx context.Context) error {
	err := s.judge(ctx, func(*pending) bool { return true })
	return errors.Join(err, s.writeLate(ctx))
}

// Run sweeps every interval until ctx is done, then flushes.
//
// Sweep errors are logged, not returned: one failed write should not stop
// the sampler for every trace after it, and Stats.Lost already counts the
// damage. The flush at the end runs on a context that ignores the
// cancellation that triggered it, since otherwise shutting down would cancel
// the very writes the flush is there to make. Its error is returned.
func (s *Sampler) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return s.Flush(context.WithoutCancel(ctx))
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil {
				s.cfg.Logger.Error("sampler sweep", "error", err)
			}
		}
	}
}

// judge removes the traces due says are ready, then decides on each and
// writes the kept ones. due runs with the lock held, so it may touch the
// counters. Assembling, deciding and writing happen after the lock is
// released, so a slow writer does not hold up Accept.
func (s *Sampler) judge(ctx context.Context, due func(*pending) bool) error {
	s.mu.Lock()
	var ready []*pending
	for id, p := range s.pending {
		if due(p) {
			ready = append(ready, p)
			delete(s.pending, id)
			s.judging[id] = nil
			s.stats.Buffered -= len(p.spans)
		}
	}
	s.mu.Unlock()

	// Oldest first, so traces are written in roughly the order they began.
	slices.SortFunc(ready, func(a, b *pending) int {
		if c := a.first.Compare(b.first); c != 0 {
			return c
		}
		return slices.Compare(a.id[:], b.id[:])
	})

	var errs []error
	for _, p := range ready {
		if err := s.decide(ctx, p); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Sampler) decide(ctx context.Context, p *pending) error {
	// The spans all share a trace and there is at least one, which is all
	// Build can refuse.
	tree, _ := assembly.Build(p.spans, assembly.Sealed)
	v := Decide(tree, s.cfg.Policies)
	if v.Keep && s.cfg.ServiceRate > 0 && !s.allow(rootService(tree)) {
		v = Verdict{Keep: false, Reason: "rate limited"}
	}

	var err error
	if v.Keep {
		if err = s.out.Write(ctx, p.spans); err != nil {
			err = fmt.Errorf("sampler: writing kept trace %s: %w", p.id, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.settle(p.id, v.Keep)
	s.stats.Reasons[v.Reason]++
	switch {
	case !v.Keep:
		s.stats.Dropped++
		s.stats.DroppedSpans += int64(len(p.spans))
	case err != nil:
		s.stats.Lost += int64(len(p.spans))
	default:
		s.stats.Kept++
		s.stats.KeptSpans += int64(len(p.spans))
	}
	return err
}

// Stats returns a snapshot of the counters.
func (s *Sampler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.Traces = len(s.pending)
	out.Decisions = len(s.decided)
	out.Reasons = make(map[string]int64, len(s.stats.Reasons))
	for k, v := range s.stats.Reasons {
		out.Reasons[k] = v
	}
	return out
}

// settle ends the judging of a trace: the spans that arrived while it was
// judged follow the verdict, and the verdict is remembered for spans still to
// come. The lock must be held.
func (s *Sampler) settle(id trace.TraceID, keep bool) {
	waiting := s.judging[id]
	delete(s.judging, id)
	switch {
	case len(waiting) == 0:
	case keep:
		s.late[id] = append(s.late[id], waiting...)
		s.stats.LateKept += int64(len(waiting))
	default:
		s.stats.Buffered -= len(waiting)
		s.stats.LateDropped += int64(len(waiting))
	}

	if s.cfg.Remember <= 0 {
		return
	}
	now := s.cfg.Now()
	s.decided[id] = decision{keep: keep, at: now}
	s.order = append(s.order, remembered{id: id, at: now})
	for len(s.decided) > s.cfg.MaxDecisions {
		s.forgetOldest()
	}
}

// forget drops the verdicts older than Remember, and the buckets of services
// that have their full allowance back. The lock must not be held.
func (s *Sampler) forget(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.order) > 0 && now.Sub(s.order[0].at) >= s.cfg.Remember {
		s.forgetOldest()
	}
	for service, b := range s.buckets {
		if b.refill(now, s.cfg.ServiceRate, s.cfg.ServiceBurst); b.tokens >= float64(s.cfg.ServiceBurst) {
			delete(s.buckets, service)
		}
	}
}

// forgetOldest drops the oldest entry in order, and its verdict unless the
// trace has been judged again since. The lock must be held.
func (s *Sampler) forgetOldest() {
	oldest := s.order[0]
	s.order[0] = remembered{}
	s.order = s.order[1:]
	if d, ok := s.decided[oldest.id]; ok && d.at.Equal(oldest.at) {
		delete(s.decided, oldest.id)
	}
	// Reslicing from the front never gives memory back, so copy once the
	// dead prefix would be most of the backing array.
	if cap(s.order) > 64 && len(s.order) < cap(s.order)/4 {
		s.order = slices.Clone(s.order)
	}
}

// writeLate writes the late spans of kept traces, a trace at a time. A failed
// write counts its spans as lost, like any other kept span the writer refused.
func (s *Sampler) writeLate(ctx context.Context) error {
	s.mu.Lock()
	late := s.late
	s.late = make(map[trace.TraceID][]trace.Span)
	for _, spans := range late {
		s.stats.Buffered -= len(spans)
	}
	s.mu.Unlock()

	ids := slices.SortedFunc(maps.Keys(late), func(a, b trace.TraceID) int { return slices.Compare(a[:], b[:]) })
	var errs []error
	for _, id := range ids {
		if err := s.out.Write(ctx, late[id]); err != nil {
			errs = append(errs, fmt.Errorf("sampler: writing late spans of kept trace %s: %w", id, err))
			s.mu.Lock()
			s.stats.Lost += int64(len(late[id]))
			s.mu.Unlock()
		}
	}
	return errors.Join(errs...)
}

// allow takes one kept trace from service's allowance, and reports whether
// there was one to take.
func (s *Sampler) allow(service string) bool {
	now := s.cfg.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[service]
	if !ok {
		b = &bucket{tokens: float64(s.cfg.ServiceBurst), at: now}
		s.buckets[service] = b
	}
	b.refill(now, s.cfg.ServiceRate, s.cfg.ServiceBurst)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// rootService is the service a trace is charged to: its root span's, or when
// the root never arrived, that of the first span that did, walking down from
// the earliest root.
func rootService(t *assembly.Tree) string {
	var service string
	t.Walk(func(n *assembly.Node, _ int) bool {
		if n.Missing() {
			return true
		}
		service = n.Span.Service
		return false
	})
	return service
}
