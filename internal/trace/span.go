package trace

import (
	"errors"
	"fmt"
	"time"
)

// SpanKind says where a span sits relative to the call it describes. The
// assembler needs it to tell a client span from the server span it caused,
// which is the edge that crosses a service boundary and therefore the only
// edge the service graph is built from.
type SpanKind uint8

const (
	SpanUnspecified SpanKind = iota
	SpanInternal
	SpanServer
	SpanClient
	SpanProducer
	SpanConsumer
)

func (k SpanKind) String() string {
	switch k {
	case SpanInternal:
		return "internal"
	case SpanServer:
		return "server"
	case SpanClient:
		return "client"
	case SpanProducer:
		return "producer"
	case SpanConsumer:
		return "consumer"
	default:
		return "unspecified"
	}
}

// StatusCode is the outcome a span reports.
type StatusCode uint8

const (
	// StatusUnset is the default and means nobody said. It is not the same as
	// success, and the sampler must not treat it as one: most instrumentation
	// only ever sets a status when something went wrong.
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

func (c StatusCode) String() string {
	switch c {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	default:
		return "unset"
	}
}

// Status is a span's outcome and, when it failed, why.
type Status struct {
	Code    StatusCode
	Message string
}

// Span is one unit of work inside a trace.
//
// This is the internal shape, not the wire shape. Spans arrive as OpenTelemetry
// protobuf and leave as JSON, and neither of those gets a say in what lives
// here.
type Span struct {
	TraceID TraceID
	SpanID  SpanID

	// ParentSpanID is the zero value on a root span. That is the same
	// "absent" convention the specification uses, so IsRoot is just a
	// validity check rather than a separate flag that could disagree with it.
	ParentSpanID SpanID

	Name string
	Kind SpanKind

	// Service is promoted out of Resource rather than left in it. It is the
	// one resource attribute that everything downstream needs constantly -
	// the storage indexes key on it, the per-service rate limiter reads it
	// on every span, the service graph is made of it - and walking a slice
	// for it each time would be a silly cost to pay that often.
	Service string

	Start time.Time
	End   time.Time

	Status     Status
	Attributes []KeyValue
	Resource   []KeyValue
}

// The ways a span can arrive malformed. They are separate errors so the
// ingest path can count them separately and say which clients are broken.
var (
	ErrNoTraceID      = errors.New("trace: span has no trace identifier")
	ErrNoSpanID       = errors.New("trace: span has no span identifier")
	ErrNoName         = errors.New("trace: span has no name")
	ErrNoService      = errors.New("trace: span has no service name")
	ErrNoStart        = errors.New("trace: span has no start time")
	ErrNoEnd          = errors.New("trace: span has no end time")
	ErrEndBeforeStart = errors.New("trace: span ends before it starts")
	ErrSelfParent     = errors.New("trace: span is its own parent")
)

// Duration is how long the span took. Validate guarantees this is not
// negative for any span that passed it.
func (s Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// IsRoot reports whether this span has no parent, and is therefore the
// entry point of its trace.
func (s Span) IsRoot() bool { return !s.ParentSpanID.IsValid() }

// Failed reports whether the span explicitly said it failed. An unset status
// is not a failure: most instrumentation only sets a status when something
// went wrong, so treating unset as success would be wrong just as often as
// treating it as failure.
func (s Span) Failed() bool { return s.Status.Code == StatusError }

// Attribute returns the value for key, and false if the span does not carry
// it. Resource attributes are not consulted; ask Resource for those.
//
// This is a linear scan on purpose. Spans carry a handful of attributes each,
// and building a map per span would cost more in allocation than the scan
// ever saves.
func (s Span) Attribute(key string) (Value, bool) {
	for _, kv := range s.Attributes {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return Value{}, false
}

// Validate reports the first reason this span cannot be stored.
//
// It runs on the ingest path, before a span reaches the buffer the sampler
// keys by trace identifier. Anything that gets past here is trusted by every
// later stage, which is why the self-parent check lives here rather than in
// the tree builder: a span parented to itself is a one-node cycle, and the
// cheapest place to refuse it is before anything tries to walk it.
func (s Span) Validate() error {
	switch {
	case !s.TraceID.IsValid():
		return ErrNoTraceID
	case !s.SpanID.IsValid():
		return ErrNoSpanID
	case s.Name == "":
		return fmt.Errorf("%w: %s", ErrNoName, s.SpanID)
	case s.Service == "":
		return fmt.Errorf("%w: %s", ErrNoService, s.SpanID)
	case s.ParentSpanID == s.SpanID:
		return fmt.Errorf("%w: %s", ErrSelfParent, s.SpanID)
	case s.Start.IsZero():
		return fmt.Errorf("%w: %s", ErrNoStart, s.SpanID)
	case s.End.IsZero():
		return fmt.Errorf("%w: %s", ErrNoEnd, s.SpanID)
	case s.End.Before(s.Start):
		return fmt.Errorf("%w: %s ran from %s to %s", ErrEndBeforeStart, s.SpanID, s.Start, s.End)
	}
	return nil
}
