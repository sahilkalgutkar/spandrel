package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	discard    = slog.New(slog.NewTextHandler(io.Discard, nil))
	spanPrefix = regexp.MustCompile(`span \d+:`)
)

// dial starts the service on an in-memory listener and returns a client. The
// real gRPC stack runs end to end, so encoding and status codes are exercised
// as a client sees them, without binding a port the test could lose to
// another process between choosing it and listening on it.
func dial(t *testing.T, sink Sink) coltracepb.TraceServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, NewTraceService(sink, discard))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return coltracepb.NewTraceServiceClient(conn)
}

func span(id byte) *tracepb.Span {
	start := time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC)
	return &tracepb.Span{
		TraceId:           bytes.Repeat([]byte{0xab}, 16),
		SpanId:            bytes.Repeat([]byte{id}, 8),
		Name:              "GET /orders",
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: uint64(start.UnixNano()),
		EndTimeUnixNano:   uint64(start.Add(10 * time.Millisecond).UnixNano()),
	}
}

func exportRequest(spans ...*tracepb.Span) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key:   "service.name",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "orders-api"}},
		}}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
}

func TestExportForwardsValidSpans(t *testing.T) {
	sink := &Recorder{}
	client := dial(t, sink)

	resp, err := client.Export(context.Background(), exportRequest(span(1), span(2)))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp.GetPartialSuccess() != nil {
		t.Errorf("unexpected partial success on a clean batch: %v", resp.GetPartialSuccess())
	}

	got := sink.Spans()
	if len(got) != 2 {
		t.Fatalf("sink received %d spans, want 2", len(got))
	}
	if got[0].Service != "orders-api" {
		t.Errorf("Service = %q, want orders-api", got[0].Service)
	}
}

// Broken spans come back through partial success with a normal OK status.
// An error status would make the exporter retry the batch, and a span with a
// four-byte identifier is not going to be fixed by sending it again.
func TestExportReportsRejectedSpansAsPartialSuccess(t *testing.T) {
	sink := &Recorder{}
	client := dial(t, sink)

	broken := span(2)
	broken.SpanId = []byte{1, 2, 3, 4}

	resp, err := client.Export(context.Background(), exportRequest(span(1), broken))
	if err != nil {
		t.Fatalf("Export returned an error for a partially bad batch: %v", err)
	}

	ps := resp.GetPartialSuccess()
	if ps.GetRejectedSpans() != 1 {
		t.Errorf("RejectedSpans = %d, want 1", ps.GetRejectedSpans())
	}
	if !strings.Contains(ps.GetErrorMessage(), "span 1") {
		t.Errorf("ErrorMessage %q does not say which span was rejected", ps.GetErrorMessage())
	}
	if n := len(sink.Spans()); n != 1 {
		t.Errorf("sink received %d spans, want only the valid one", n)
	}
}

func TestRejectionSummaryIsCapped(t *testing.T) {
	var spans []*tracepb.Span
	for i := range 10 {
		s := span(byte(i + 1))
		s.EndTimeUnixNano = 0
		spans = append(spans, s)
	}

	resp, err := dial(t, &Recorder{}).Export(context.Background(), exportRequest(spans...))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	msg := resp.GetPartialSuccess().GetErrorMessage()
	// Count the "span N:" prefixes, not the word: each reason quotes the model's
	// own error text, which says "span" too.
	if got := len(spanPrefix.FindAllString(msg, -1)); got != maxReportedRejections {
		t.Errorf("message names %d spans, want %d: %q", got, maxReportedRejections, msg)
	}
	if !strings.HasSuffix(msg, "and 7 more") {
		t.Errorf("message does not report the remainder: %q", msg)
	}
}

type failingSink struct{ err error }

func (f failingSink) Accept(context.Context, []trace.Span) error { return f.err }

// A full sink has to surface as Unavailable, which the exporters retry with
// backoff. Anything else and a burst of traffic becomes silently dropped
// spans instead of a slower client.
func TestSinkErrorsMapToStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"full sink is retryable", ErrSinkFull, codes.Unavailable},
		{"wrapped full sink is still retryable", errors.Join(errors.New("buffer"), ErrSinkFull), codes.Unavailable},
		{"anything else is internal", errors.New("disk on fire"), codes.Internal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dial(t, failingSink{tc.err}).Export(context.Background(), exportRequest(span(1)))
			if got := status.Code(err); got != tc.want {
				t.Errorf("status = %v, want %v", got, tc.want)
			}
		})
	}
}

// A batch where every span is broken never reaches the sink. A sink that is
// full should not be able to fail a request that had nothing to give it.
func TestAllRejectedBatchSkipsTheSink(t *testing.T) {
	broken := span(1)
	broken.TraceId = nil

	resp, err := dial(t, failingSink{ErrSinkFull}).Export(context.Background(), exportRequest(broken))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Errorf("RejectedSpans = %d, want 1", resp.GetPartialSuccess().GetRejectedSpans())
	}
}
