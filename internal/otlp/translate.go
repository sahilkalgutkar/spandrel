// Package otlp turns OpenTelemetry protocol payloads into spandrel's own span
// model. It is the only package that knows the wire format exists; nothing
// past it imports the generated protobuf types.
package otlp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// unknownService is what the OpenTelemetry SDKs themselves fall back to when
// nobody configured a service name. Using the same string means a span from a
// misconfigured app looks the same here as it does in every other backend,
// rather than being thrown away over a missing label.
const unknownService = "unknown_service"

const serviceNameKey = "service.name"

// Rejection records one span that could not be translated and why.
type Rejection struct {
	// Index counts spans across the whole request in the order they appear,
	// so a client-side log can be matched back to the span that was refused.
	Index int
	Err   error
}

// ErrBadIDLength means an identifier arrived as the wrong number of bytes.
var ErrBadIDLength = errors.New("otlp: identifier has the wrong byte length")

// Translate converts every span in resourceSpans. Spans that fail are left
// out and reported, instead of failing the whole batch: the protocol has a
// partial-success response for exactly this, and one bad span from a buggy
// library should not cost an application the other thousand in the batch.
func Translate(resourceSpans []*tracepb.ResourceSpans) ([]trace.Span, []Rejection) {
	var (
		spans      []trace.Span
		rejections []Rejection
		index      int
	)

	for _, rs := range resourceSpans {
		resource := keyValues(rs.GetResource().GetAttributes())
		service := serviceName(resource)

		for _, ss := range rs.GetScopeSpans() {
			for _, ps := range ss.GetSpans() {
				span, err := translateSpan(ps, service, resource)
				if err == nil {
					err = span.Validate()
				}
				if err != nil {
					rejections = append(rejections, Rejection{Index: index, Err: err})
				} else {
					spans = append(spans, span)
				}
				index++
			}
		}
	}

	return spans, rejections
}

func translateSpan(ps *tracepb.Span, service string, resource []trace.KeyValue) (trace.Span, error) {
	var span trace.Span

	if err := copyID(span.TraceID[:], ps.GetTraceId(), "trace"); err != nil {
		return span, err
	}
	if err := copyID(span.SpanID[:], ps.GetSpanId(), "span"); err != nil {
		return span, err
	}
	// An empty parent is how the protocol says root. Anything else has to be
	// a full eight bytes like any other span identifier.
	if parent := ps.GetParentSpanId(); len(parent) > 0 {
		if err := copyID(span.ParentSpanID[:], parent, "parent span"); err != nil {
			return span, err
		}
	}

	span.Name = ps.GetName()
	span.Kind = spanKind(ps.GetKind())
	span.Service = service
	span.Start = unixNano(ps.GetStartTimeUnixNano())
	span.End = unixNano(ps.GetEndTimeUnixNano())
	span.Status = trace.Status{
		Code:    statusCode(ps.GetStatus().GetCode()),
		Message: ps.GetStatus().GetMessage(),
	}
	span.Attributes = keyValues(ps.GetAttributes())
	// Every span in a resource block shares the same slice. The model treats
	// attributes as read-only once built, and copying the resource per span
	// would multiply the memory the sampler has to buffer.
	span.Resource = resource

	return span, nil
}

func copyID(dst, src []byte, what string) error {
	if len(src) != len(dst) {
		return fmt.Errorf("%w: %s identifier is %d bytes, want %d", ErrBadIDLength, what, len(src), len(dst))
	}
	copy(dst, src)
	return nil
}

// unixNano maps the protocol's zero timestamp onto Go's zero time, so that
// Validate reports a missing timestamp instead of accepting a span that
// claims to have started in 1970.
func unixNano(n uint64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(n)).UTC()
}

func serviceName(resource []trace.KeyValue) string {
	for _, kv := range resource {
		if kv.Key != serviceNameKey {
			continue
		}
		if s, ok := kv.Value.AsString(); ok && s != "" {
			return s
		}
	}
	return unknownService
}

func keyValues(in []*commonpb.KeyValue) []trace.KeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]trace.KeyValue, 0, len(in))
	for _, kv := range in {
		out = append(out, trace.KeyValue{Key: kv.GetKey(), Value: value(kv.GetValue())})
	}
	return out
}

// value maps the protocol's attribute value onto the model's four scalar
// kinds. Arrays, maps and raw bytes have no scalar equivalent; they are kept
// as a string rather than dropped, because an attribute nobody can query on
// is still worth seeing when you open the trace.
func value(v *commonpb.AnyValue) trace.Value {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return trace.StringValue(x.StringValue)
	case *commonpb.AnyValue_BoolValue:
		return trace.BoolValue(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return trace.IntValue(x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return trace.FloatValue(x.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return trace.StringValue(hex.EncodeToString(x.BytesValue))
	case *commonpb.AnyValue_ArrayValue, *commonpb.AnyValue_KvlistValue:
		b, err := protojson.Marshal(v)
		if err != nil {
			return trace.Value{}
		}
		return trace.StringValue(string(b))
	default:
		return trace.Value{}
	}
}

func spanKind(k tracepb.Span_SpanKind) trace.SpanKind {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return trace.SpanInternal
	case tracepb.Span_SPAN_KIND_SERVER:
		return trace.SpanServer
	case tracepb.Span_SPAN_KIND_CLIENT:
		return trace.SpanClient
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return trace.SpanProducer
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return trace.SpanConsumer
	default:
		return trace.SpanUnspecified
	}
}

func statusCode(c tracepb.Status_StatusCode) trace.StatusCode {
	switch c {
	case tracepb.Status_STATUS_CODE_OK:
		return trace.StatusOK
	case tracepb.Status_STATUS_CODE_ERROR:
		return trace.StatusError
	default:
		return trace.StatusUnset
	}
}
