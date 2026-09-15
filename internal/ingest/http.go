package ingest

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TracesPath is where the OpenTelemetry HTTP exporters send spans unless
// someone configures otherwise.
const TracesPath = "/v1/traces"

const (
	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"

	// maxBodyBytes bounds a single export after decompression. The SDKs batch
	// at a few megabytes by default; this leaves room for that and refuses a
	// request that would otherwise decompress into however much memory it
	// likes.
	maxBodyBytes = 16 << 20

	// retryAfterSeconds is what a client is told to wait when the sink is
	// full. Long enough that a burst has room to drain, short enough that the
	// exporter's own queue does not overflow while it waits.
	retryAfterSeconds = "2"
)

// HTTPHandler serves the OpenTelemetry HTTP trace export endpoint, in both
// its protobuf and JSON encodings.
type HTTPHandler struct {
	sink   Sink
	logger *slog.Logger
}

// NewHTTPHandler returns a handler that forwards valid spans to sink.
func NewHTTPHandler(sink Sink, logger *slog.Logger) *HTTPHandler {
	return &HTTPHandler{sink: sink, logger: logger}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "export requires POST", http.StatusMethodNotAllowed)
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (mediaType != contentTypeProtobuf && mediaType != contentTypeJSON) {
		http.Error(w, fmt.Sprintf("Content-Type must be %s or %s", contentTypeProtobuf, contentTypeJSON), http.StatusUnsupportedMediaType)
		return
	}

	body, err := readBody(r)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}

	req := &coltracepb.ExportTraceServiceRequest{}
	if err := unmarshal(mediaType, body, req); err != nil {
		http.Error(w, "could not decode export request: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := export(r.Context(), h.sink, req)
	if err != nil {
		if errors.Is(err, ErrSinkFull) {
			// 503 with Retry-After is what the exporters treat as retryable,
			// the same way gRPC's Unavailable is.
			w.Header().Set("Retry-After", retryAfterSeconds)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		h.logger.Error("sink refused spans", "error", err)
		http.Error(w, "could not accept spans", http.StatusInternalServerError)
		return
	}
	if ps := resp.GetPartialSuccess(); ps != nil {
		h.logger.Warn("rejected spans", "count", ps.GetRejectedSpans(), "reason", ps.GetErrorMessage())
	}

	// The response is encoded the same way as the request. The specification
	// requires it, and a JSON client handed protobuf back could not read its
	// own partial-success report.
	out, err := marshal(mediaType, resp)
	if err != nil {
		h.logger.Error("encoding export response", "error", err)
		http.Error(w, "could not encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", mediaType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

var errBodyTooLarge = fmt.Errorf("request body exceeds %d bytes", maxBodyBytes)

// readBody reads the request, decompressing it first when the client says it
// is gzipped, which the SDKs do by default. The size cap applies to the
// decompressed bytes: capping the compressed size alone would let a small
// request expand into an unbounded allocation.
func readBody(r *http.Request) ([]byte, error) {
	var reader io.Reader = r.Body

	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("body is not valid gzip: %w", err)
		}
		defer gz.Close()
		reader = gz
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", r.Header.Get("Content-Encoding"))
	}

	body, err := io.ReadAll(io.LimitReader(reader, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	if len(body) > maxBodyBytes {
		return nil, errBodyTooLarge
	}
	return body, nil
}

func unmarshal(mediaType string, body []byte, m proto.Message) error {
	if mediaType == contentTypeJSON {
		return protojson.Unmarshal(body, m)
	}
	return proto.Unmarshal(body, m)
}

func marshal(mediaType string, m proto.Message) ([]byte, error) {
	if mediaType == contentTypeJSON {
		return protojson.Marshal(m)
	}
	return proto.Marshal(m)
}
