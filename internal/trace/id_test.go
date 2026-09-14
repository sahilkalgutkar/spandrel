package trace

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestParseTraceID(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // expected String() output, empty when an error is expected
		err   error
	}{
		{
			name:  "round trips a well formed identifier",
			input: "4bf92f3577b34da6a3ce929d0e0e4736",
			want:  "4bf92f3577b34da6a3ce929d0e0e4736",
		},
		{
			// Lenient in, strict out: whoever pastes this from a log that
			// upcased it should not have to care.
			name:  "accepts uppercase and normalises it",
			input: "4BF92F3577B34DA6A3CE929D0E0E4736",
			want:  "4bf92f3577b34da6a3ce929d0e0e4736",
		},
		{
			name:  "rejects a short identifier",
			input: "4bf92f3577b34da6",
			err:   ErrIDLength,
		},
		{
			name:  "rejects a long identifier",
			input: "4bf92f3577b34da6a3ce929d0e0e4736ff",
			err:   ErrIDLength,
		},
		{
			name:  "rejects an empty identifier",
			input: "",
			err:   ErrIDLength,
		},
		{
			name:  "rejects non-hexadecimal characters",
			input: "zzf92f3577b34da6a3ce929d0e0e4736",
			err:   ErrIDHex,
		},
		{
			// The specification reserves all-zero to mean absent, so it must
			// never survive parsing into something the rest of the system
			// treats as a real identifier.
			name:  "rejects the all zero identifier",
			input: strings.Repeat("0", 32),
			err:   ErrIDZero,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTraceID(tc.input)

			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("ParseTraceID(%q) error = %v, want %v", tc.input, err, tc.err)
				}
				if got.IsValid() {
					t.Errorf("ParseTraceID(%q) returned a valid identifier alongside an error", tc.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseTraceID(%q) unexpected error: %v", tc.input, err)
			}
			if got.String() != tc.want {
				t.Errorf("ParseTraceID(%q).String() = %q, want %q", tc.input, got.String(), tc.want)
			}
			if !got.IsValid() {
				t.Errorf("ParseTraceID(%q) produced an identifier that reports itself invalid", tc.input)
			}
		})
	}
}

func TestParseSpanID(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		err   error
	}{
		{
			name:  "round trips a well formed identifier",
			input: "00f067aa0ba902b7",
			want:  "00f067aa0ba902b7",
		},
		{
			name:  "accepts uppercase and normalises it",
			input: "00F067AA0BA902B7",
			want:  "00f067aa0ba902b7",
		},
		{
			// A span identifier is half the width of a trace identifier, so
			// the trace form has to be rejected here rather than silently
			// truncated.
			name:  "rejects a trace sized identifier",
			input: "4bf92f3577b34da6a3ce929d0e0e4736",
			err:   ErrIDLength,
		},
		{
			name:  "rejects non-hexadecimal characters",
			input: "00f067aa0ba902bg",
			err:   ErrIDHex,
		},
		{
			name:  "rejects the all zero identifier",
			input: strings.Repeat("0", 16),
			err:   ErrIDZero,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSpanID(tc.input)

			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("ParseSpanID(%q) error = %v, want %v", tc.input, err, tc.err)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseSpanID(%q) unexpected error: %v", tc.input, err)
			}
			if got.String() != tc.want {
				t.Errorf("ParseSpanID(%q).String() = %q, want %q", tc.input, got.String(), tc.want)
			}
		})
	}
}

// A failed decode must not leave the caller's identifier half overwritten.
// UnmarshalText writes through a pointer, so getting this wrong would corrupt
// an identifier that the caller still believes is the old one.
func TestUnmarshalTextLeavesReceiverIntactOnError(t *testing.T) {
	const original = "4bf92f3577b34da6a3ce929d0e0e4736"

	id, err := ParseTraceID(original)
	if err != nil {
		t.Fatalf("setting up: %v", err)
	}

	// Valid hex for the first half, junk in the second, so an in-place decode
	// would get part way through before failing.
	err = id.UnmarshalText([]byte("ffffffffffffffffzzzzzzzzzzzzzzzz"))
	if !errors.Is(err, ErrIDHex) {
		t.Fatalf("UnmarshalText error = %v, want %v", err, ErrIDHex)
	}

	if id.String() != original {
		t.Errorf("identifier was modified by a failed decode: got %q, want %q", id.String(), original)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	type span struct {
		TraceID TraceID `json:"trace_id"`
		SpanID  SpanID  `json:"span_id"`
	}

	in := span{}
	var err error
	if in.TraceID, err = ParseTraceID("4bf92f3577b34da6a3ce929d0e0e4736"); err != nil {
		t.Fatalf("setting up: %v", err)
	}
	if in.SpanID, err = ParseSpanID("00f067aa0ba902b7"); err != nil {
		t.Fatalf("setting up: %v", err)
	}

	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	// Hex rather than the base64 Go would produce for a bare byte array. A
	// stored span should be greppable by the identifier you read in a log.
	const want = `{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7"}`
	if string(encoded) != want {
		t.Errorf("json.Marshal = %s, want %s", encoded, want)
	}

	var out span
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round trip changed the value: got %+v, want %+v", out, in)
	}
}

func TestZeroValueIsNotValid(t *testing.T) {
	var (
		traceID TraceID
		spanID  SpanID
	)

	// A root span carries a zero parent, and the tree builder will lean on
	// exactly this to recognise one.
	if traceID.IsValid() {
		t.Error("the zero TraceID reports itself valid")
	}
	if spanID.IsValid() {
		t.Error("the zero SpanID reports itself valid")
	}
}

func TestNewIDsAreValidAndDistinct(t *testing.T) {
	const draws = 128

	seenTraces := make(map[TraceID]struct{}, draws)
	seenSpans := make(map[SpanID]struct{}, draws)

	for i := range draws {
		traceID, err := NewTraceID()
		if err != nil {
			t.Fatalf("NewTraceID: %v", err)
		}
		spanID, err := NewSpanID()
		if err != nil {
			t.Fatalf("NewSpanID: %v", err)
		}

		if !traceID.IsValid() {
			t.Fatalf("NewTraceID returned an invalid identifier on draw %d", i)
		}
		if !spanID.IsValid() {
			t.Fatalf("NewSpanID returned an invalid identifier on draw %d", i)
		}

		if _, dup := seenTraces[traceID]; dup {
			t.Fatalf("NewTraceID repeated %s within %d draws", traceID, draws)
		}
		if _, dup := seenSpans[spanID]; dup {
			t.Fatalf("NewSpanID repeated %s within %d draws", spanID, draws)
		}

		seenTraces[traceID] = struct{}{}
		seenSpans[spanID] = struct{}{}
	}
}
