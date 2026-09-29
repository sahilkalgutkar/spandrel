// Package storage keeps spans and finds traces again.
//
// The Store interface is what the query API and the sampler are written
// against. Implementations decide how spans are kept; they do not get to
// decide what a match is, which is why query validation and matching live
// here rather than in each implementation.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	// ErrNotFound means no spans are stored for the requested trace.
	ErrNotFound = errors.New("storage: trace not found")

	// ErrClosed means the store has been closed.
	ErrClosed = errors.New("storage: store is closed")

	// ErrBadQuery wraps every reason Query.Validate refuses a query.
	ErrBadQuery = errors.New("storage: invalid query")
)

const (
	// DefaultLimit is how many traces Find returns when the query does not say.
	DefaultLimit = 20

	// MaxLimit bounds a single Find. A search that wants more than this is a
	// search that should be narrowed, not paged through a thousand at a time.
	MaxLimit = 1000
)

// Store keeps spans and answers questions about the traces they form.
type Store interface {
	// Write stores spans. A span whose identifier is already stored for its
	// trace replaces the earlier copy, because exporters retry batches and a
	// retried batch is not new data.
	Write(ctx context.Context, spans []trace.Span) error

	// Trace returns every stored span of one trace, ordered by start time.
	Trace(ctx context.Context, id trace.TraceID) ([]trace.Span, error)

	// Find returns summaries of traces matching q, newest first.
	Find(ctx context.Context, q Query) ([]Summary, error)

	Close() error
}

// Query describes a trace search.
//
// Service, Operation and the duration bounds apply to a single span: a trace
// matches only if one span satisfies all of them at once. Asking for slow
// checkout calls should not match a trace where checkout was fast and some
// other service was slow.
//
// Start and End apply to the trace as a whole, which matches if any part of
// it overlaps the window.
type Query struct {
	Service     string
	Operation   string
	MinDuration time.Duration
	MaxDuration time.Duration // zero means unbounded
	Start       time.Time     // zero means unbounded
	End         time.Time     // zero means unbounded
	Limit       int           // zero means DefaultLimit
}

// Validate reports why q cannot be run, if it cannot.
func (q Query) Validate() error {
	switch {
	case q.Operation != "" && q.Service == "":
		// Operation names are only unique within a service. "GET /health"
		// exists in every service there is, and a search for it across all
		// of them is almost never the question someone meant to ask.
		return fmt.Errorf("%w: an operation needs a service", ErrBadQuery)
	case q.MinDuration < 0 || q.MaxDuration < 0:
		return fmt.Errorf("%w: durations cannot be negative", ErrBadQuery)
	case q.MaxDuration > 0 && q.MaxDuration < q.MinDuration:
		return fmt.Errorf("%w: maximum duration %v is below minimum %v", ErrBadQuery, q.MaxDuration, q.MinDuration)
	case !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start):
		return fmt.Errorf("%w: window ends before it starts", ErrBadQuery)
	case q.Limit < 0 || q.Limit > MaxLimit:
		return fmt.Errorf("%w: limit must be between 0 and %d", ErrBadQuery, MaxLimit)
	}
	return nil
}

func (q Query) limit() int {
	if q.Limit == 0 {
		return DefaultLimit
	}
	return q.Limit
}

// hasSpanPredicate reports whether any condition applies to individual spans.
func (q Query) hasSpanPredicate() bool {
	return q.Service != "" || q.MinDuration > 0 || q.MaxDuration > 0
}

// matchesSpan reports whether one span satisfies every span-level condition.
func (q Query) matchesSpan(s *trace.Span) bool {
	if q.Service != "" && s.Service != q.Service {
		return false
	}
	if q.Operation != "" && s.Name != q.Operation {
		return false
	}
	d := s.Duration()
	if d < q.MinDuration {
		return false
	}
	if q.MaxDuration > 0 && d > q.MaxDuration {
		return false
	}
	return true
}

// overlaps reports whether a trace running from start to end touches the
// query window.
func (q Query) overlaps(start, end time.Time) bool {
	if !q.Start.IsZero() && end.Before(q.Start) {
		return false
	}
	if !q.End.IsZero() && start.After(q.End) {
		return false
	}
	return true
}

// Summary is what a search returns for each trace: enough to list it and
// decide whether to open it, without shipping every span.
type Summary struct {
	TraceID trace.TraceID

	// RootService and RootName describe the root span. A trace whose root has
	// not arrived, or never will, is summarised by its earliest span instead,
	// and Complete says which happened.
	RootService string
	RootName    string
	Complete    bool

	Start    time.Time
	Duration time.Duration
	Spans    int
	Failed   bool
}
