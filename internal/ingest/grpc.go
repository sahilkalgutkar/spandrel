package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sahilkalgutkar/spandrel/internal/otlp"
)

// maxReportedRejections caps how many individual reasons go back to the
// client in the partial-success message. A client sending a batch of ten
// thousand broken spans needs to know they are broken and why, not to receive
// ten thousand copies of the same sentence.
const maxReportedRejections = 3

// ErrSinkFull is what a Sink returns when it cannot take more spans right now.
// The gRPC service reports it as Unavailable, which the OpenTelemetry
// exporters treat as retryable, so a full buffer turns into client-side
// backoff rather than lost data.
var ErrSinkFull = errors.New("ingest: sink is full")

// TraceService implements the OpenTelemetry trace export RPC.
type TraceService struct {
	coltracepb.UnimplementedTraceServiceServer

	sink   Sink
	logger *slog.Logger
}

// NewTraceService returns a service that forwards valid spans to sink.
func NewTraceService(sink Sink, logger *slog.Logger) *TraceService {
	return &TraceService{sink: sink, logger: logger}
}

// Export translates the request, forwards what is valid, and reports what was
// not through partial success rather than an error status. Failing the call
// would make the exporter retry the whole batch, and the broken spans in it
// would be just as broken the second time.
func (s *TraceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	resp, err := export(ctx, s.sink, req)
	if err != nil {
		if errors.Is(err, ErrSinkFull) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		s.logger.Error("sink refused spans", "error", err)
		return nil, status.Error(codes.Internal, "could not accept spans")
	}
	if ps := resp.GetPartialSuccess(); ps != nil {
		s.logger.Warn("rejected spans", "count", ps.GetRejectedSpans(), "reason", ps.GetErrorMessage())
	}
	return resp, nil
}

// export is the transport-neutral core, shared with the HTTP endpoint so the
// two cannot drift in what they accept or how they report it.
func export(ctx context.Context, sink Sink, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	spans, rejections := otlp.Translate(req.GetResourceSpans())

	if len(spans) > 0 {
		if err := sink.Accept(ctx, spans); err != nil {
			return nil, err
		}
	}

	resp := &coltracepb.ExportTraceServiceResponse{}
	if len(rejections) > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(len(rejections)),
			ErrorMessage:  summarise(rejections),
		}
	}
	return resp, nil
}

func summarise(rejections []otlp.Rejection) string {
	msg := ""
	for i, r := range rejections {
		if i == maxReportedRejections {
			msg += fmt.Sprintf("; and %d more", len(rejections)-maxReportedRejections)
			break
		}
		if i > 0 {
			msg += "; "
		}
		msg += fmt.Sprintf("span %d: %v", r.Index, r.Err)
	}
	return msg
}
