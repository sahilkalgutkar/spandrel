package trace

import (
	"errors"
	"testing"
	"time"
)

// validSpan returns a span that passes Validate, for tests that want to break
// exactly one thing about it.
func validSpan(t *testing.T) Span {
	t.Helper()

	traceID, err := ParseTraceID("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("setting up: %v", err)
	}
	spanID, err := ParseSpanID("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("setting up: %v", err)
	}

	start := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

	return Span{
		TraceID: traceID,
		SpanID:  spanID,
		Name:    "GET /checkout",
		Kind:    SpanServer,
		Service: "checkout-api",
		Start:   start,
		End:     start.Add(150 * time.Millisecond),
		Status:  Status{Code: StatusOK},
	}
}

func TestValidate(t *testing.T) {
	parentID, err := ParseSpanID("051581bf3cb55c13")
	if err != nil {
		t.Fatalf("setting up: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Span)
		wantErr error
	}{
		{
			name:   "a well formed span",
			mutate: func(*Span) {},
		},
		{
			name:   "a well formed child span",
			mutate: func(s *Span) { s.ParentSpanID = parentID },
		},
		{
			name:    "no trace identifier",
			mutate:  func(s *Span) { s.TraceID = TraceID{} },
			wantErr: ErrNoTraceID,
		},
		{
			name:    "no span identifier",
			mutate:  func(s *Span) { s.SpanID = SpanID{} },
			wantErr: ErrNoSpanID,
		},
		{
			name:    "no name",
			mutate:  func(s *Span) { s.Name = "" },
			wantErr: ErrNoName,
		},
		{
			name:    "no service",
			mutate:  func(s *Span) { s.Service = "" },
			wantErr: ErrNoService,
		},
		{
			// A one-node cycle. Refusing it here means the tree builder never
			// has to defend against walking one.
			name:    "parented to itself",
			mutate:  func(s *Span) { s.ParentSpanID = s.SpanID },
			wantErr: ErrSelfParent,
		},
		{
			name:    "no start time",
			mutate:  func(s *Span) { s.Start = time.Time{} },
			wantErr: ErrNoStart,
		},
		{
			name:    "no end time",
			mutate:  func(s *Span) { s.End = time.Time{} },
			wantErr: ErrNoEnd,
		},
		{
			// Clock skew between two hosts can do this to a span that was
			// perfectly well behaved on the machine that emitted it.
			name:    "ends before it starts",
			mutate:  func(s *Span) { s.End = s.Start.Add(-time.Millisecond) },
			wantErr: ErrEndBeforeStart,
		},
		{
			// Zero duration is legal. Plenty of real spans are shorter than
			// the clock resolution that timed them.
			name:   "starts and ends at the same instant",
			mutate: func(s *Span) { s.End = s.Start },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			span := validSpan(t)
			tc.mutate(&span)

			err := span.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestIsRoot(t *testing.T) {
	span := validSpan(t)
	if !span.IsRoot() {
		t.Error("a span with no parent did not report itself as a root")
	}

	parentID, err := ParseSpanID("051581bf3cb55c13")
	if err != nil {
		t.Fatalf("setting up: %v", err)
	}
	span.ParentSpanID = parentID
	if span.IsRoot() {
		t.Error("a span with a parent reported itself as a root")
	}
}

func TestDuration(t *testing.T) {
	span := validSpan(t)
	if got, want := span.Duration(), 150*time.Millisecond; got != want {
		t.Errorf("Duration() = %v, want %v", got, want)
	}
}

// Unset is not success. Most instrumentation sets a status only when
// something goes wrong, so a sampler that read unset as OK would keep
// nothing, and one that read it as failure would keep everything.
func TestFailed(t *testing.T) {
	tests := []struct {
		code StatusCode
		want bool
	}{
		{code: StatusUnset, want: false},
		{code: StatusOK, want: false},
		{code: StatusError, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			span := validSpan(t)
			span.Status = Status{Code: tc.code}
			if got := span.Failed(); got != tc.want {
				t.Errorf("Failed() on a %v span = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}

func TestAttribute(t *testing.T) {
	span := validSpan(t)
	span.Attributes = []KeyValue{
		{Key: "http.method", Value: StringValue("GET")},
		{Key: "http.status_code", Value: IntValue(200)},
	}
	span.Resource = []KeyValue{
		{Key: "host.name", Value: StringValue("ip-10-0-1-7")},
	}

	if got, ok := span.Attribute("http.status_code"); !ok {
		t.Error("Attribute() did not find an attribute the span carries")
	} else if code, _ := got.AsInt(); code != 200 {
		t.Errorf("Attribute() = %v, want 200", got)
	}

	if _, ok := span.Attribute("http.route"); ok {
		t.Error("Attribute() found an attribute the span does not carry")
	}

	// Resource attributes are a separate set. Folding them in here would let
	// a span-level search match on something the span never set.
	if _, ok := span.Attribute("host.name"); ok {
		t.Error("Attribute() returned a resource attribute")
	}
}

func TestKindAndStatusNames(t *testing.T) {
	kinds := map[SpanKind]string{
		SpanUnspecified: "unspecified",
		SpanInternal:    "internal",
		SpanServer:      "server",
		SpanClient:      "client",
		SpanProducer:    "producer",
		SpanConsumer:    "consumer",
		SpanKind(200):   "unspecified",
	}
	for kind, want := range kinds {
		if got := kind.String(); got != want {
			t.Errorf("SpanKind(%d).String() = %q, want %q", kind, got, want)
		}
	}

	codes := map[StatusCode]string{
		StatusUnset:     "unset",
		StatusOK:        "ok",
		StatusError:     "error",
		StatusCode(200): "unset",
	}
	for code, want := range codes {
		if got := code.String(); got != want {
			t.Errorf("StatusCode(%d).String() = %q, want %q", code, got, want)
		}
	}
}
