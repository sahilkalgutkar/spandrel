package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sahilkalgutkar/spandrel/internal/sampler"
	"github.com/sahilkalgutkar/spandrel/internal/storage"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// sampled is a daemon running serveSampled in front of a memory store, with a
// sampler that keeps error traces and nothing else.
type sampled struct {
	grpcAddr string
	httpAddr string
	store    *storage.Memory
	sampler  *sampler.Sampler
	stop     func() error
}

func startSampled(t *testing.T, quiet time.Duration) *sampled {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg, err := samplerConfig(config{
		sampleQuiet:  quiet,
		sampleMaxAge: 100 * quiet,
		sampleBuffer: 1000,
		remember:     time.Minute,
		maxDecisions: 100,
	}, logger)
	if err != nil {
		t.Fatalf("samplerConfig: %v", err)
	}
	store := storage.NewMemory(0, nil)
	smp, err := sampler.New(cfg, store)
	if err != nil {
		t.Fatalf("sampler.New: %v", err)
	}

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding gRPC listener: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding HTTP listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveSampled(ctx, logger, smp, grpcLis, httpLis, 5*time.Second, quiet/4) }()

	stopped := false
	var stopErr error
	stop := func() error {
		if !stopped {
			stopped = true
			cancel()
			stopErr = <-done
		}
		return stopErr
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("serveSampled returned %v on shutdown", err)
		}
		store.Close()
	})
	return &sampled{grpcAddr: grpcLis.Addr().String(), httpAddr: httpLis.Addr().String(), store: store, sampler: smp, stop: stop}
}

func grpcExporter(t *testing.T, addr string) sdktrace.SpanExporter {
	t.Helper()
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpoint(addr), otlptracegrpc.WithInsecure())
	if err != nil {
		t.Fatalf("creating exporter: %v", err)
	}
	return exporter
}

// emitHealthy records a one-span trace that finishes cleanly.
func emitHealthy(t *testing.T, exporter sdktrace.SpanExporter) oteltrace.TraceID {
	t.Helper()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("checkout-api"))),
		sdktrace.WithSyncer(exporter),
	)
	_, root := provider.Tracer("spandreld-test").Start(context.Background(), "GET /health",
		oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	root.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatalf("flushing spans through the exporter: %v", err)
	}
	return root.SpanContext().TraceID()
}

// stored waits up to five seconds for a trace to reach the store.
func stored(t *testing.T, s *storage.Memory, id oteltrace.TraceID) []trace.Span {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		spans, err := s.Trace(context.Background(), trace.TraceID(id))
		if err == nil {
			return spans
		}
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("reading trace %s: %v", id, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s never reached the store", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheDaemonKeepsErrorTracesAndDropsTheRest(t *testing.T) {
	d := startSampled(t, 50*time.Millisecond)
	failed := emit(t, grpcExporter(t, d.grpcAddr))
	healthy := emitHealthy(t, grpcExporter(t, d.grpcAddr))

	checkTrace(t, failed, stored(t, d.store, failed))

	deadline := time.Now().Add(5 * time.Second)
	for d.sampler.Stats().Dropped == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the healthy trace was never judged")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := d.store.Trace(context.Background(), trace.TraceID(healthy)); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("the healthy trace was stored, or reading it failed: %v", err)
	}
}

// A trace still waiting for its quiet period when the daemon stops is judged
// on the way out, after the exports have drained, and written before the
// store closes.
func TestShutdownJudgesTracesStillWaiting(t *testing.T) {
	d := startSampled(t, time.Hour)
	failed := emit(t, grpcExporter(t, d.grpcAddr))
	if got := d.sampler.Stats().Buffered; got != 3 {
		t.Fatalf("%d spans waiting before shutdown, want 3", got)
	}

	if err := d.stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}
	checkTrace(t, failed, stored(t, d.store, failed))
}

func TestSamplerConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	base, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := samplerConfig(base, logger)
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if len(cfg.Policies) != 3 {
		t.Errorf("default chain has %d policies, want errors, slow and a share", len(cfg.Policies))
	}
	if _, err := sampler.New(cfg, storage.NewMemory(0, nil)); err != nil {
		t.Errorf("the defaults do not make a sampler: %v", err)
	}

	noSlow := base
	noSlow.keepSlowerThan = 0
	if cfg, _ := samplerConfig(noSlow, logger); len(cfg.Policies) != 2 {
		t.Errorf("with -keep-slower-than 0 the chain has %d policies, want 2", len(cfg.Policies))
	}

	tooMuch := base
	tooMuch.keepShare = 1.5
	if _, err := samplerConfig(tooMuch, logger); err == nil {
		t.Error("a share over 1 was accepted")
	}
}

func TestParseFlagsRefusesANegativeSlowThreshold(t *testing.T) {
	if _, err := parseFlags([]string{"-keep-slower-than", "-1s"}); err == nil {
		t.Error("a negative -keep-slower-than was accepted")
	}
}

func TestRunRefusesSettingsTheSamplerRejects(t *testing.T) {
	if err := run([]string{"-sample-quiet", "0s", "-grpc-addr", "127.0.0.1:0", "-http-addr", "127.0.0.1:0"}, nil); err == nil {
		t.Error("run started with a quiet period of zero")
	}
	if err := run([]string{"-keep-share", "2"}, nil); err == nil {
		t.Error("run started with a share of 2")
	}
}

// failedExport is an OTLP request holding one failed span of trace id.
func failedExport(t *testing.T, id [16]byte) []byte {
	t.Helper()
	start := time.Now().Add(-time.Second)
	body, err := proto.Marshal(&coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key:   "service.name",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "orders-api"}},
		}}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{
			TraceId:           id[:],
			SpanId:            bytes.Repeat([]byte{7}, 8),
			Name:              "POST /orders",
			StartTimeUnixNano: uint64(start.UnixNano()),
			EndTimeUnixNano:   uint64(start.Add(10 * time.Millisecond).UnixNano()),
			Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR},
		}}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// The case the flush order exists for: an export already in flight when
// shutdown begins finishes during the drain, after shutdown started. The
// sampler must still be taking spans then, and judge them afterwards.
func TestAnExportThatFinishesDuringTheDrainIsStillStored(t *testing.T) {
	d := startSampled(t, time.Hour)
	id := [16]byte{0xd4, 0xa1, 15: 1}
	body := failedExport(t, id)

	conn, err := net.Dial("tcp", d.httpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /v1/traces HTTP/1.1\r\nHost: spandrel\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", len(body))
	// Give the server time to read the headers, which makes the request active
	// and something shutdown has to wait for.
	time.Sleep(100 * time.Millisecond)

	stopped := make(chan error, 1)
	go func() { stopped <- d.stop() }()

	// Shutdown closes the listeners first, so once a new connection is
	// refused, the drain has begun.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", d.httpAddr)
		if err != nil {
			break
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Fatal("the HTTP listener never closed")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := conn.Write(body); err != nil {
		t.Fatalf("sending the body: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("export during the drain got %s", resp.Status)
	}

	if err := <-stopped; err != nil {
		t.Fatalf("stopping: %v", err)
	}
	if _, err := d.store.Trace(context.Background(), trace.TraceID(id)); err != nil {
		t.Errorf("the span accepted during the drain was not stored: %v", err)
	}
}
