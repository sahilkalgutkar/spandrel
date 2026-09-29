// Command spandreld runs the tracing backend: it listens for OpenTelemetry
// exports over gRPC and HTTP and hands the spans on.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sahilkalgutkar/spandrel/internal/ingest"
	"github.com/sahilkalgutkar/spandrel/internal/storage"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "spandreld:", err)
		os.Exit(1)
	}
}

type config struct {
	grpcAddr     string
	httpAddr     string
	logLevel     slog.Level
	drainTimeout time.Duration
	dataDir      string
	retention    time.Duration
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("spandreld", flag.ContinueOnError)

	// 4317 and 4318 are the ports every OpenTelemetry SDK defaults to, so an
	// app pointed at this host needs no port configuration at all.
	var cfg config
	fs.StringVar(&cfg.grpcAddr, "grpc-addr", ":4317", "address for OpenTelemetry gRPC exports")
	fs.StringVar(&cfg.httpAddr, "http-addr", ":4318", "address for OpenTelemetry HTTP exports")
	fs.DurationVar(&cfg.drainTimeout, "drain-timeout", 10*time.Second, "how long in-flight exports get to finish on shutdown")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "directory to keep spans in; empty keeps them in memory only")
	fs.DurationVar(&cfg.retention, "retention", 72*time.Hour, "how long a trace is kept after it ends; 0 keeps everything")
	level := fs.String("log-level", "info", "debug, info, warn or error")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if err := cfg.logLevel.UnmarshalText([]byte(*level)); err != nil {
		return cfg, fmt.Errorf("invalid -log-level %q", *level)
	}
	if cfg.retention < 0 {
		return cfg, fmt.Errorf("invalid -retention %v: cannot be negative", cfg.retention)
	}
	return cfg, nil
}

func run(args []string, logOut *os.File) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: cfg.logLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("closing store", "error", err)
		}
	}()
	go evictLoop(ctx, store, time.Minute, logger)

	grpcLis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("listening for gRPC on %s: %w", cfg.grpcAddr, err)
	}
	httpLis, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		grpcLis.Close()
		return fmt.Errorf("listening for HTTP on %s: %w", cfg.httpAddr, err)
	}

	// The store sits directly behind ingest for now. When the sampler lands
	// it goes in between, and this is the line that changes.
	return serve(ctx, logger, store, grpcLis, httpLis, cfg.drainTimeout)
}

// store is what the daemon needs from either storage implementation.
type store interface {
	storage.Store
	ingest.Sink
	EvictExpired() int
}

// openStore keeps spans in memory when no data directory is given, which is
// the right default for trying it out and the wrong one for anything you
// would be upset to lose on a restart.
func openStore(cfg config) (store, error) {
	if cfg.dataDir == "" {
		return storage.NewMemory(cfg.retention, nil), nil
	}
	d, err := storage.OpenDisk(cfg.dataDir, cfg.retention, nil)
	if err != nil {
		return nil, fmt.Errorf("opening store in %s: %w", cfg.dataDir, err)
	}
	return d, nil
}

// evictLoop drops expired traces every interval until ctx ends. Eviction takes
// the store's write lock, so running it on a ticker rather than on every write
// keeps its cost out of the ingest path.
func evictLoop(ctx context.Context, s store, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := s.EvictExpired(); n > 0 {
				logger.Debug("evicted expired traces", "count", n)
			}
		}
	}
}

// serve runs both transports on listeners the caller already holds, until ctx
// ends or either server fails. Taking listeners rather than addresses is what
// lets a test bind to port zero and read back the real port without a window
// in which something else can take it.
func serve(ctx context.Context, logger *slog.Logger, sink ingest.Sink, grpcLis, httpLis net.Listener, drainTimeout time.Duration) error {
	grpcServer := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(grpcServer, ingest.NewTraceService(sink, logger))
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	mux := http.NewServeMux()
	mux.Handle(ingest.TracesPath, ingest.NewHTTPHandler(sink, logger))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		logger.Info("accepting gRPC exports", "addr", grpcLis.Addr().String())
		errs <- grpcServer.Serve(grpcLis)
	}()
	go func() {
		logger.Info("accepting HTTP exports", "addr", httpLis.Addr().String())
		if err := httpServer.Serve(httpLis); !errors.Is(err, http.ErrServerClosed) {
			errs <- err
			return
		}
		errs <- nil
	}()

	var serveErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case serveErr = <-errs:
		logger.Error("server stopped unexpectedly", "error", serveErr)
	}

	// Stop advertising health first, so a load balancer stops routing new
	// exports here while the in-flight ones drain.
	healthServer.Shutdown()

	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()

	if err := httpServer.Shutdown(drainCtx); err != nil {
		logger.Warn("HTTP exports did not drain in time", "error", err)
	}

	select {
	case <-grpcDone:
	case <-drainCtx.Done():
		logger.Warn("gRPC exports did not drain in time, closing connections")
		grpcServer.Stop()
	}

	return serveErr
}
