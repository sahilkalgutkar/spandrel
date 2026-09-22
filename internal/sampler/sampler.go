package sampler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	// Now is the clock. Tests replace it; nil means time.Now.
	Now func() time.Time

	// Logger receives the errors Run cannot return. Nil discards them.
	Logger *slog.Logger
}

// Stats counts what the sampler has done since it started.
type Stats struct {
	// Accepted counts spans taken into the buffer, and Refused counts spans
	// turned away because it was full. Refused spans were not lost: the
	// client was told to retry them.
	Accepted, Refused int64

	// Buffered and Traces are how many spans, and how many traces, are
	// waiting to be judged right now.
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
}

// Sampler buffers spans by trace and judges each trace once it looks
// finished. It is an ingest sink: Accept is called from the export handlers.
type Sampler struct {
	cfg Config
	out Writer

	mu      sync.Mutex
	pending map[trace.TraceID]*pending
	stats   Stats
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
	}, nil
}

// Accept buffers spans until their traces are judged.
//
// A batch that would take the buffer past MaxSpans is refused whole. Taking
// part of it would need a way to tell the client which part, and the protocol
// has none: a retry resends the whole batch either way.
func (s *Sampler) Accept(_ context.Context, spans []trace.Span) error {
	if len(spans) > s.cfg.MaxSpans {
		return fmt.Errorf("%w: %d spans, buffer holds %d", ErrBatchTooLarge, len(spans), s.cfg.MaxSpans)
	}
	now := s.cfg.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stats.Buffered+len(spans) > s.cfg.MaxSpans {
		s.stats.Refused += int64(len(spans))
		return fmt.Errorf("%w: %d of %d spans buffered", ingest.ErrSinkFull, s.stats.Buffered, s.cfg.MaxSpans)
	}

	for _, sp := range spans {
		p, ok := s.pending[sp.TraceID]
		if !ok {
			p = &pending{id: sp.TraceID, first: now}
			s.pending[sp.TraceID] = p
		}
		p.spans = append(p.spans, sp)
		p.last = now
	}
	s.stats.Accepted += int64(len(spans))
	s.stats.Buffered += len(spans)
	return nil
}

// Sweep judges every trace that has gone quiet or reached its maximum age,
// and writes the ones kept. It is meant to be called on a ticker.
//
// A writer error does not stop the sweep: the other traces are still judged
// and written, and the errors come back joined. The spans of a kept trace the
// writer refused are counted in Stats.Lost.
func (s *Sampler) Sweep(ctx context.Context) error {
	now := s.cfg.Now()
	return s.judge(ctx, func(p *pending) bool {
		switch {
		case now.Sub(p.last) >= s.cfg.Quiet:
			return true
		case now.Sub(p.first) >= s.cfg.MaxAge:
			s.stats.SealedByAge++
			return true
		}
		return false
	})
}

// Flush judges every buffered trace now, whether or not it looks finished.
//
// It is for shutdown. A trace cut off by a restart is judged on the spans
// that arrived, which is incomplete but honest. Dropping it unjudged would
// silently lose an error trace that happened to be in flight.
func (s *Sampler) Flush(ctx context.Context) error {
	return s.judge(ctx, func(*pending) bool { return true })
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

	var err error
	if v.Keep {
		if err = s.out.Write(ctx, p.spans); err != nil {
			err = fmt.Errorf("sampler: writing kept trace %s: %w", p.id, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
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
	out.Reasons = make(map[string]int64, len(s.stats.Reasons))
	for k, v := range s.stats.Reasons {
		out.Reasons[k] = v
	}
	return out
}
