// Package ingest accepts spans over the OpenTelemetry protocol and hands them
// to whatever comes next. Today that is a sink the caller supplies; once the
// sampler exists it will be the sampler.
package ingest

import (
	"context"
	"sync"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// Sink receives spans that passed translation and validation.
//
// Accept may be called from many goroutines at once, one per inbound export,
// and must not keep a reference to the slice after it returns: the ingest
// path is free to reuse it.
type Sink interface {
	Accept(ctx context.Context, spans []trace.Span) error
}

// Recorder is a Sink that keeps everything it is given. It exists for tests
// and for running the daemon before there is anywhere better to send spans.
type Recorder struct {
	mu    sync.Mutex
	spans []trace.Span
}

// Accept stores a copy of spans.
func (r *Recorder) Accept(_ context.Context, spans []trace.Span) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

// Spans returns a copy of everything recorded so far.
func (r *Recorder) Spans() []trace.Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]trace.Span, len(r.spans))
	copy(out, r.spans)
	return out
}
