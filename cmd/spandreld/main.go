// Command spandreld runs the tracing backend: it listens for OpenTelemetry
// exports over gRPC and HTTP, holds each trace until it looks finished,
// decides whether to keep it, and stores the ones it keeps.
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
	"github.com/sahilkalgutkar/spandrel/internal/sampler"
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

	sampleQuiet    time.Duration
	sampleMaxAge   time.Duration
	sampleBuffer   int
	keepSlowerThan time.Duration
	keepShare      float64
	serviceRate    float64
	serviceBurst   int
	remember       time.Duration
	maxDecisions   int
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

	// Error traces are always kept. These decide when a trace is judged and
	// what else is kept along with them.
	fs.DurationVar(&cfg.sampleQuiet, "sample-quiet", 5*time.Second, "how long a trace goes without a new span before it is judged")
	fs.DurationVar(&cfg.sampleMaxAge, "sample-max-age", time.Minute, "how long a trace can wait to be judged however busy it is")
	fs.IntVar(&cfg.sampleBuffer, "sample-buffer", 200_000, "most spans held waiting for a verdict; past it exports are refused with a retryable error")
	fs.DurationVar(&cfg.keepSlowerThan, "keep-slower-than", time.Second, "keep every trace that took longer than this; 0 turns it off")
	fs.Float64Var(&cfg.keepShare, "keep-share", 0.1, "share of the remaining traces to keep, from 0 to 1")
	fs.Float64Var(&cfg.serviceRate, "service-rate", 50, "most traces a second kept for one service; 0 means no cap")
	fs.IntVar(&cfg.serviceBurst, "service-burst", 100, "traces a service can have kept back to back before -service-rate applies")
	fs.DurationVar(&cfg.remember, "remember", 5*time.Minute, "how long a verdict is remembered, so spans arriving after it follow it")
	fs.IntVar(&cfg.maxDecisions, "max-decisions", 100_000, "most verdicts remembered at once")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if err := cfg.logLevel.UnmarshalText([]byte(*level)); err != nil {
		return cfg, fmt.Errorf("invalid -log-level %q", *level)
	}
	if cfg.retention < 0 {
		return cfg, fmt.Errorf("invalid -retention %v: cannot be negative", cfg.retention)
	}
	if cfg.keepSlowerThan < 0 {
		return cfg, fmt.Errorf("invalid -keep-slower-than %v: cannot be negative", cfg.keepSlowerThan)
	}
	return cfg, nil
}

// samplerConfig turns the flags into the sampler's settings and policy chain:
// errors first, then slow traces, then a share of whatever is left. The
// sampler checks the rest when it is built.
func samplerConfig(cfg config, logger *slog.Logger) (sampler.Config, error) {
	share, err := sampler.NewKeepFraction(cfg.keepShare)
	if err != nil {
		return sampler.Config{}, fmt.Errorf("invalid -keep-share: %w", err)
	}
	policies := []sampler.Policy{sampler.KeepErrors{}}
	if cfg.keepSlowerThan > 0 {
		policies = append(policies, sampler.KeepSlow{Threshold: cfg.keepSlowerThan})
	}
	return sampler.Config{
		Quiet:        cfg.sampleQuiet,
		MaxAge:       cfg.sampleMaxAge,
		MaxSpans:     cfg.sampleBuffer,
		Policies:     append(policies, share),
		Remember:     cfg.remember,
		MaxDecisions: cfg.maxDecisions,
		ServiceRate:  cfg.serviceRate,
		ServiceBurst: cfg.serviceBurst,
		Logger:       logger,
	}, nil
}

func run(args []string, logOut *os.File) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: cfg.logLevel}))
	sampling, err := samplerConfig(cfg, logger)
	if err != nil {
		return err
	}

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

	smp, err := sampler.New(sampling, store)
	if err != nil {
		return err
	}

	grpcLis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("listening for gRPC on %s: %w", cfg.grpcAddr, err)
	}
	httpLis, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		grpcLis.Close()
		return fmt.Errorf("listening for HTTP on %s: %w", cfg.httpAddr, err)
	}

	return serveSampled(ctx, logger, smp, grpcLis, httpLis, cfg.drainTimeout, cfg.sampleQuiet/4)
}

// store is what the daemon needs from either storage implementation.
type store interface {
	storage.Store
	EvictExpired() int
}

// serveSampled runs serve with the sampler as the sink, sweeping it every
// sweepEvery, and flushes it once serve has returned.
//
// The order is the point. The flush judges every trace still buffered, so it
// has to come after the exports have drained: flushing when shutdown begins
// would judge traces whose spans were still arriving, and a span accepted
// after the flush would sit in the buffer and never be written. The store is
// closed by the caller, after this returns, so the flush still has somewhere
// to write.
func serveSampled(ctx context.Context, logger *slog.Logger, s *sampler.Sampler, grpcLis, httpLis net.Listener, drainTimeout, sweepEvery time.Duration) error {
	sweepCtx, stopSweeping := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSweeping()
	flushed := make(chan error, 1)
	go func() { flushed <- s.Run(sweepCtx, max(sweepEvery, time.Millisecond)) }()

	serveErr := serve(ctx, logger, s, grpcLis, httpLis, drainTimeout)

	stopSweeping()
	flushErr := <-flushed
	if flushErr != nil {
		flushErr = fmt.Errorf("flushing the sampler: %w", flushErr)
	}
	st := s.Stats()
	logger.Info("sampler stopped",
		"kept", st.Kept, "dropped", st.Dropped, "lost", st.Lost, "refused", st.Refused,
		"late_kept", st.LateKept, "late_dropped", st.LateDropped, "sealed_by_age", st.SealedByAge)
	return errors.Join(serveErr, flushErr)
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
