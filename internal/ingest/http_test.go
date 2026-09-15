package ingest

import (
	"bytes"
	"compress/gzip"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func post(t *testing.T, h http.Handler, contentType, encoding string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, TracesPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func mustProto(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return b
}

func gzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(b); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	return buf.Bytes()
}

func TestHTTPAcceptsEachEncoding(t *testing.T) {
	req := exportRequest(span(1), span(2))

	jsonBody, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	tests := []struct {
		name        string
		contentType string
		encoding    string
		body        []byte
	}{
		{"protobuf", "application/x-protobuf", "", mustProto(t, req)},
		{"gzipped protobuf", "application/x-protobuf", "gzip", gzipped(t, mustProto(t, req))},
		{"json", "application/json", "", jsonBody},
		{"json with a charset parameter", "application/json; charset=utf-8", "", jsonBody},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sink := &Recorder{}
			rec := post(t, NewHTTPHandler(sink, discard), tc.contentType, tc.encoding, tc.body)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
			}
			if n := len(sink.Spans()); n != 2 {
				t.Errorf("sink received %d spans, want 2", n)
			}
			if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(tc.contentType, got) {
				t.Errorf("response Content-Type = %q, want it to match the request's %q", got, tc.contentType)
			}
		})
	}
}

// A JSON client must get its partial-success report back as JSON, or it has
// no way to learn which of its spans were refused.
func TestHTTPPartialSuccessUsesTheRequestEncoding(t *testing.T) {
	broken := span(2)
	broken.SpanId = []byte{1}

	body, err := protojson.Marshal(exportRequest(span(1), broken))
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	rec := post(t, NewHTTPHandler(&Recorder{}, discard), "application/json", "", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}

	var resp coltracepb.ExportTraceServiceResponse
	if err := protojson.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON the client can decode: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Errorf("RejectedSpans = %d, want 1", resp.GetPartialSuccess().GetRejectedSpans())
	}
}

func TestHTTPRefusesBadRequests(t *testing.T) {
	valid := mustProto(t, exportRequest(span(1)))

	tests := []struct {
		name        string
		method      string
		contentType string
		encoding    string
		body        []byte
		want        int
	}{
		{"wrong method", http.MethodGet, "application/x-protobuf", "", nil, http.StatusMethodNotAllowed},
		{"missing content type", http.MethodPost, "", "", valid, http.StatusUnsupportedMediaType},
		{"unsupported content type", http.MethodPost, "text/plain", "", valid, http.StatusUnsupportedMediaType},
		{"unsupported encoding", http.MethodPost, "application/x-protobuf", "br", valid, http.StatusBadRequest},
		{"claims gzip but is not", http.MethodPost, "application/x-protobuf", "gzip", valid, http.StatusBadRequest},
		{"undecodable protobuf", http.MethodPost, "application/x-protobuf", "", []byte{0xff, 0xff, 0xff}, http.StatusBadRequest},
		{"undecodable json", http.MethodPost, "application/json", "", []byte("{not json"), http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, TracesPath, bytes.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			rec := httptest.NewRecorder()
			NewHTTPHandler(&Recorder{}, discard).ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// The size cap has to apply after decompression. A few kilobytes of gzipped
// zeros expand into tens of megabytes, and checking only the bytes on the
// wire would let that straight through into memory.
func TestHTTPCapsTheDecompressedSize(t *testing.T) {
	bomb := gzipped(t, make([]byte, maxBodyBytes+1))
	if len(bomb) > 1<<16 {
		t.Fatalf("test setup: compressed body is %d bytes, expected it to be small", len(bomb))
	}

	rec := post(t, NewHTTPHandler(&Recorder{}, discard), "application/x-protobuf", "gzip", bomb)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestHTTPSinkErrors(t *testing.T) {
	body := mustProto(t, exportRequest(span(1)))

	t.Run("full sink asks the client to retry", func(t *testing.T) {
		rec := post(t, NewHTTPHandler(failingSink{ErrSinkFull}, discard), "application/x-protobuf", "", body)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
		if rec.Header().Get("Retry-After") == "" {
			t.Error("a 503 without Retry-After leaves the exporter guessing how long to back off")
		}
	})

	t.Run("anything else is a server error", func(t *testing.T) {
		rec := post(t, NewHTTPHandler(failingSink{errors.New("disk on fire")}, discard), "application/x-protobuf", "", body)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500", rec.Code)
		}
	})
}
