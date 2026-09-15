package otlp

import (
	"bytes"
	"errors"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	traceID  = bytes.Repeat([]byte{0xab}, 16)
	spanID   = bytes.Repeat([]byte{0x01}, 8)
	parentID = bytes.Repeat([]byte{0x02}, 8)
	start    = time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC)
)

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func wireSpan() *tracepb.Span {
	return &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentID,
		Name:              "SELECT orders",
		Kind:              tracepb.Span_SPAN_KIND_CLIENT,
		StartTimeUnixNano: uint64(start.UnixNano()),
		EndTimeUnixNano:   uint64(start.Add(42 * time.Millisecond).UnixNano()),
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "deadlock"},
		Attributes:        []*commonpb.KeyValue{str("db.system", "postgresql")},
	}
}

func request(service string, spans ...*tracepb.Span) []*tracepb.ResourceSpans {
	var attrs []*commonpb.KeyValue
	if service != "" {
		attrs = append(attrs, str("service.name", service))
	}
	return []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: attrs},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}
}

func TestTranslateCarriesEveryField(t *testing.T) {
	spans, rejections := Translate(request("orders-api", wireSpan()))
	if len(rejections) != 0 {
		t.Fatalf("unexpected rejections: %+v", rejections)
	}
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	got := spans[0]

	if !bytes.Equal(got.TraceID[:], traceID) || !bytes.Equal(got.SpanID[:], spanID) || !bytes.Equal(got.ParentSpanID[:], parentID) {
		t.Errorf("identifiers not carried over: %s %s %s", got.TraceID, got.SpanID, got.ParentSpanID)
	}
	if got.Name != "SELECT orders" || got.Kind != trace.SpanClient || got.Service != "orders-api" {
		t.Errorf("name, kind or service wrong: %q %v %q", got.Name, got.Kind, got.Service)
	}
	if !got.Start.Equal(start) || got.Duration() != 42*time.Millisecond {
		t.Errorf("timing wrong: start %v, duration %v", got.Start, got.Duration())
	}
	if !got.Failed() || got.Status.Message != "deadlock" {
		t.Errorf("status wrong: %+v", got.Status)
	}
	if v, ok := got.Attribute("db.system"); !ok || v != trace.StringValue("postgresql") {
		t.Errorf("attribute db.system = %v, %v", v, ok)
	}
	if len(got.Resource) != 1 || got.Resource[0].Key != "service.name" {
		t.Errorf("resource attributes not kept: %+v", got.Resource)
	}
}

func TestEmptyParentMeansRoot(t *testing.T) {
	ps := wireSpan()
	ps.ParentSpanId = nil

	spans, rejections := Translate(request("orders-api", ps))
	if len(rejections) != 0 || len(spans) != 1 {
		t.Fatalf("got %d spans and %+v", len(spans), rejections)
	}
	if !spans[0].IsRoot() {
		t.Error("a span with an empty parent was not treated as a root")
	}
}

// SDKs label a span with unknown_service when nobody configured a name. A
// span that arrives that way is still a real span and must not be refused.
func TestMissingServiceNameFallsBack(t *testing.T) {
	spans, rejections := Translate(request("", wireSpan()))
	if len(rejections) != 0 || len(spans) != 1 {
		t.Fatalf("got %d spans and %+v", len(spans), rejections)
	}
	if spans[0].Service != unknownService {
		t.Errorf("Service = %q, want %q", spans[0].Service, unknownService)
	}
}

// One broken span must cost the client that span only, and the rejection has
// to say which one it was.
func TestBadSpansAreRejectedIndividually(t *testing.T) {
	shortTrace := wireSpan()
	shortTrace.TraceId = traceID[:8]

	shortParent := wireSpan()
	shortParent.ParentSpanId = []byte{0x02}

	noEnd := wireSpan()
	noEnd.EndTimeUnixNano = 0

	spans, rejections := Translate(request("orders-api", wireSpan(), shortTrace, shortParent, noEnd))

	if len(spans) != 1 {
		t.Errorf("got %d good spans, want 1", len(spans))
	}
	if len(rejections) != 3 {
		t.Fatalf("got %d rejections, want 3: %+v", len(rejections), rejections)
	}

	want := []struct {
		index int
		err   error
	}{
		{1, ErrBadIDLength},
		{2, ErrBadIDLength},
		{3, trace.ErrNoEnd},
	}
	for i, w := range want {
		if rejections[i].Index != w.index || !errors.Is(rejections[i].Err, w.err) {
			t.Errorf("rejection %d = {%d %v}, want {%d %v}", i, rejections[i].Index, rejections[i].Err, w.index, w.err)
		}
	}
}

// Indexes run across resource and scope blocks, not within each one, so they
// still identify a span when a batch mixes several services.
func TestRejectionIndexSpansResourceBlocks(t *testing.T) {
	bad := wireSpan()
	bad.SpanId = nil

	req := append(request("a", wireSpan(), wireSpan()), request("b", bad)...)
	_, rejections := Translate(req)

	if len(rejections) != 1 || rejections[0].Index != 2 {
		t.Fatalf("rejections = %+v, want one at index 2", rejections)
	}
}

func TestAttributeValueKinds(t *testing.T) {
	tests := []struct {
		name string
		in   *commonpb.AnyValue
		want trace.Value
	}{
		{"string", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}}, trace.StringValue("x")},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, trace.BoolValue(true)},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: -3}}, trace.IntValue(-3)},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0.5}}, trace.FloatValue(0.5)},
		{"bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xde, 0xad}}}, trace.StringValue("dead")},
		{"missing", nil, trace.Value{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := value(tc.in); got != tc.want {
				t.Errorf("value() = %v (%v), want %v (%v)", got, got.Kind(), tc.want, tc.want.Kind())
			}
		})
	}
}

// Arrays and maps are kept as their protobuf JSON form. That encoder
// deliberately varies its whitespace between runs so nobody depends on the
// exact bytes, so this decodes the string back rather than comparing it.
func TestNonScalarValuesKeepTheirContent(t *testing.T) {
	in := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
		Values: []*commonpb.AnyValue{
			{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
			{Value: &commonpb.AnyValue_StringValue{StringValue: "two"}},
		},
	}}}

	got, ok := value(in).AsString()
	if !ok {
		t.Fatalf("an array attribute did not come back as a string")
	}

	var decoded commonpb.AnyValue
	if err := protojson.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("stored form %q does not decode: %v", got, err)
	}
	if !proto.Equal(&decoded, in) {
		t.Errorf("stored form %q lost content", got)
	}
}

func TestKindAndStatusMapping(t *testing.T) {
	kinds := map[tracepb.Span_SpanKind]trace.SpanKind{
		tracepb.Span_SPAN_KIND_UNSPECIFIED: trace.SpanUnspecified,
		tracepb.Span_SPAN_KIND_INTERNAL:    trace.SpanInternal,
		tracepb.Span_SPAN_KIND_SERVER:      trace.SpanServer,
		tracepb.Span_SPAN_KIND_CLIENT:      trace.SpanClient,
		tracepb.Span_SPAN_KIND_PRODUCER:    trace.SpanProducer,
		tracepb.Span_SPAN_KIND_CONSUMER:    trace.SpanConsumer,
	}
	for in, want := range kinds {
		if got := spanKind(in); got != want {
			t.Errorf("spanKind(%v) = %v, want %v", in, got, want)
		}
	}

	codes := map[tracepb.Status_StatusCode]trace.StatusCode{
		tracepb.Status_STATUS_CODE_UNSET: trace.StatusUnset,
		tracepb.Status_STATUS_CODE_OK:    trace.StatusOK,
		tracepb.Status_STATUS_CODE_ERROR: trace.StatusError,
	}
	for in, want := range codes {
		if got := statusCode(in); got != want {
			t.Errorf("statusCode(%v) = %v, want %v", in, got, want)
		}
	}
}
