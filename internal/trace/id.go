package trace

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// TraceID identifies a trace. Sixteen bytes, which is what the W3C trace
// context specification fixes and therefore what arrives on the wire.
type TraceID [16]byte

// SpanID identifies one span inside a trace. Eight bytes, same source.
type SpanID [8]byte

// The three ways a caller can hand over a bad identifier. They are separate
// errors because the ingest path treats them differently: a length or hex
// problem means a broken client worth counting, while an all-zero identifier
// is what a correctly-built but unsampled SDK sends, which is routine.
var (
	ErrIDLength = errors.New("trace: identifier has the wrong length")
	ErrIDHex    = errors.New("trace: identifier is not valid hexadecimal")
	ErrIDZero   = errors.New("trace: identifier is all zero")
)

// ParseTraceID decodes the 32-character hex form of a trace identifier.
func ParseTraceID(s string) (TraceID, error) {
	var id TraceID
	err := parseID(s, id[:])
	return id, err
}

// ParseSpanID decodes the 16-character hex form of a span identifier.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	err := parseID(s, id[:])
	return id, err
}

// NewTraceID draws a random trace identifier.
func NewTraceID() (TraceID, error) {
	var id TraceID
	err := randomID(id[:])
	return id, err
}

// NewSpanID draws a random span identifier.
func NewSpanID() (SpanID, error) {
	var id SpanID
	err := randomID(id[:])
	return id, err
}

// IsValid reports whether the identifier is usable. Only the all-zero value
// is not: the specification reserves it to mean "absent", which is how a
// root span says it has no parent.
func (t TraceID) IsValid() bool { return !allZero(t[:]) }

// IsValid reports whether the identifier is usable. See TraceID.IsValid.
func (s SpanID) IsValid() bool { return !allZero(s[:]) }

// String returns the lowercase hex form.
func (t TraceID) String() string { return hex.EncodeToString(t[:]) }

// String returns the lowercase hex form.
func (s SpanID) String() string { return hex.EncodeToString(s[:]) }

// MarshalText puts the hex form in JSON rather than the base64 that Go would
// otherwise produce for a byte array, so a stored span stays greppable.
func (t TraceID) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// MarshalText puts the hex form in JSON. See TraceID.MarshalText.
func (s SpanID) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText decodes the hex form.
func (t *TraceID) UnmarshalText(b []byte) error { return parseID(string(b), t[:]) }

// UnmarshalText decodes the hex form.
func (s *SpanID) UnmarshalText(b []byte) error { return parseID(string(b), s[:]) }

// parseID decodes s into out, which must already be the right length. Both
// identifier types share it because the three ways this can fail are the same
// for each, and I would rather they stay wrong in only one place.
//
// It decodes into scratch space and copies on success. UnmarshalText hands us
// the caller's identifier directly, and decoding in place would leave it half
// overwritten when the input turns out to be malformed halfway through.
func parseID(s string, out []byte) error {
	want := len(out) * 2
	if len(s) != want {
		return fmt.Errorf("%w: got %d characters, want %d", ErrIDLength, len(s), want)
	}

	// hex.Decode accepts either case. Being lenient here and always emitting
	// lowercase from String costs nothing and spares me a class of bug report
	// from whoever pastes an identifier out of a log that upcased it.
	buf := make([]byte, len(out))
	if _, err := hex.Decode(buf, []byte(s)); err != nil {
		return fmt.Errorf("%w: %v", ErrIDHex, err)
	}

	if allZero(buf) {
		return ErrIDZero
	}

	copy(out, buf)
	return nil
}

// randomID fills out with random bytes, redrawing on the all-zero value so
// the result always satisfies IsValid. The loop will not run twice in the
// lifetime of this software, but the invariant is cheaper to hold here than
// to reason about at every call site.
func randomID(out []byte) error {
	for {
		if _, err := rand.Read(out); err != nil {
			return fmt.Errorf("trace: drawing a random identifier: %w", err)
		}
		if !allZero(out) {
			return nil
		}
	}
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
