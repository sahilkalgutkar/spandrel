package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/sahilkalgutkar/spandrel/internal/ingest"
	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// startDaemon runs serve on listeners bound to port zero. Holding the
// listeners from bind to serve means there is no gap in which another process
// can take the port, which is the race that has broken port-probing tests in
// my other projects.
func startDaemon(t *testing.T) (grpcAddr, httpAddr string, sink *ingest.Recorder) {
	t.Helper()

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding gRPC listener: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding HTTP listener: %v", err)
	}

	sink = &ingest.Recorder{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- serve(ctx, logger, sink, grpcLis, httpLis, 5*time.Second) }()

	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve returned %v on shutdown", err)
		}
	})

	return grpcLis.Addr().String(), httpLis.Addr().String(), sink
}

// emit records a small real trace through the SDK - a server span with a
// client call and a failed database query under it - and flushes it through
// the given exporter.
func emit(t *testing.T, exporter sdktrace.SpanExporter) oteltrace.TraceID {
	t.Helper()

	res := resource.NewSchemaless(semconv.ServiceName("checkout-api"))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSyncer(exporter),
	)
	tracer := provider.Tracer("spandreld-test")

	ctx, root := tracer.Start(context.Background(), "POST /checkout", oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	_, call := tracer.Start(ctx, "charge card", oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	call.SetAttributes(attribute.Int("retry.count", 2))
	call.End()
	_, query := tracer.Start(ctx, "UPDATE orders")
	query.SetStatus(codes.Error, "deadlock detected")
	query.End()
	root.End()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("flushing spans through the exporter: %v", err)
	}

	return root.SpanContext().TraceID()
}

// checkTrace asserts that what arrived is the trace the SDK built, shape and
// all, and not merely three spans of some kind.
func checkTrace(t *testing.T, want oteltrace.TraceID, got []trace.Span) {
	t.Helper()

	if len(got) != 3 {
		t.Fatalf("received %d spans, want 3", len(got))
	}

	byName := map[string]trace.Span{}
	for _, s := range got {
		if s.TraceID.String() != want.String() {
			t.Errorf("span %q has trace %s, want %s", s.Name, s.TraceID, want)
		}
		if s.Service != "checkout-api" {
			t.Errorf("span %q has service %q, want checkout-api", s.Name, s.Service)
		}
		byName[s.Name] = s
	}

	root, ok := byName["POST /checkout"]
	if !ok || !root.IsRoot() || root.Kind != trace.SpanServer {
		t.Fatalf("root span missing or wrong: %+v", root)
	}

	call := byName["charge card"]
	if call.ParentSpanID != root.SpanID || call.Kind != trace.SpanClient {
		t.Errorf("client span not parented to the root, or wrong kind: %+v", call)
	}
	if v, ok := call.Attribute("retry.count"); !ok || v != trace.IntValue(2) {
		t.Errorf("retry.count = %v, %v; want 2", v, ok)
	}

	query := byName["UPDATE orders"]
	if query.ParentSpanID != root.SpanID || !query.Failed() || query.Status.Message != "deadlock detected" {
		t.Errorf("failed query span not carried faithfully: %+v", query)
	}
}

// These use the real OpenTelemetry SDK exporters rather than payloads built by
// hand. A hand-built payload can only prove the server agrees with my reading
// of the specification; this proves it agrees with what applications
// actually send.
func TestRealExporters(t *testing.T) {
	t.Run("grpc", func(t *testing.T) {
		grpcAddr, _, sink := startDaemon(t)

		exporter, err := otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithEndpoint(grpcAddr),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			t.Fatalf("creating exporter: %v", err)
		}

		checkTrace(t, emit(t, exporter), sink.Spans())
	})

	for _, compression := range []struct {
		name string
		opt  otlptracehttp.Compression
	}{
		{"http", otlptracehttp.NoCompression},
		{"http gzip", otlptracehttp.GzipCompression},
	} {
		t.Run(compression.name, func(t *testing.T) {
			_, httpAddr, sink := startDaemon(t)

			exporter, err := otlptracehttp.New(context.Background(),
				otlptracehttp.WithEndpoint(httpAddr),
				otlptracehttp.WithInsecure(),
				otlptracehttp.WithCompression(compression.opt),
			)
			if err != nil {
				t.Fatalf("creating exporter: %v", err)
			}

			checkTrace(t, emit(t, exporter), sink.Spans())
		})
	}
}

func TestHealthEndpoint(t *testing.T) {
	_, httpAddr, _ := startDaemon(t)

	resp, err := http.Get("http://" + httpAddr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// A failing server must bring serve back, not leave it running on one leg.
func TestServeReturnsWhenAListenerFails(t *testing.T) {
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	httpLis.Close()

	done := make(chan error, 1)
	go func() {
		done <- serve(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), &ingest.Recorder{}, grpcLis, httpLis, time.Second)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("serve returned nil after a listener failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve kept running after the HTTP listener failed")
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if cfg.grpcAddr != ":4317" || cfg.httpAddr != ":4318" || cfg.logLevel != slog.LevelInfo {
		t.Errorf("defaults = %+v, want the standard OpenTelemetry ports at info", cfg)
	}

	cfg, err = parseFlags([]string{"-grpc-addr", "127.0.0.1:9000", "-log-level", "debug", "-drain-timeout", "3s"})
	if err != nil {
		t.Fatalf("explicit flags: %v", err)
	}
	if cfg.grpcAddr != "127.0.0.1:9000" || cfg.logLevel != slog.LevelDebug || cfg.drainTimeout != 3*time.Second {
		t.Errorf("explicit flags = %+v", cfg)
	}

	if _, err := parseFlags([]string{"-log-level", "loud"}); err == nil {
		t.Error("an invalid log level was accepted")
	}
	if _, err := parseFlags([]string{"-no-such-flag"}); err == nil {
		t.Error("an unknown flag was accepted")
	}
}

func TestRunRefusesAnAddressInUse(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	defer taken.Close()

	err = run([]string{"-grpc-addr", taken.Addr().String(), "-http-addr", "127.0.0.1:0"}, nil)
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Errorf("run with a taken port returned %v, want a listen error", err)
	}
}
