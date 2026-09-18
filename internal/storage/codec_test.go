package storage

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

func richSpan() trace.Span {
	s := spec{1, 2, 3, "checkout", "POST /pay", 5 * time.Millisecond, 250 * time.Millisecond, true}.span()
	s.Kind = trace.SpanServer
	s.Status.Message = "card declined"
	s.Attributes = []trace.KeyValue{
		{Key: "http.method", Value: trace.StringValue("POST")},
		{Key: "http.status_code", Value: trace.IntValue(-402)},
		{Key: "retried", Value: trace.BoolValue(true)},
		{Key: "cold", Value: trace.BoolValue(false)},
		{Key: "ratio", Value: trace.FloatValue(math.Pi)},
		{Key: "unset", Value: trace.Value{}},
	}
	s.Resource = []trace.KeyValue{{Key: "service.name", Value: trace.StringValue("checkout")}}
	return s
}

func TestCodecRoundTrip(t *testing.T) {
	cases := map[string]trace.Span{
		"every field set":            richSpan(),
		"root with nothing optional": spec{9, 9, 0, "a", "b", 0, 0, false}.span(),
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := decodeSpan(encodeSpan(nil, &in))
			if err != nil {
				t.Fatalf("decodeSpan: %v", err)
			}
			if !reflect.DeepEqual(out, in) {
				t.Errorf("round trip changed the span:\n got  %+v\n want %+v", out, in)
			}
		})
	}
}

// Every prefix of a valid record is invalid. That property is what lets
// replay tell a torn tail from a record that happens to end early.
func TestCodecRejectsEveryTruncation(t *testing.T) {
	s := richSpan()
	full := encodeSpan(nil, &s)

	for n := range len(full) {
		if _, err := decodeSpan(full[:n]); !errors.Is(err, errCorruptSpan) {
			t.Fatalf("decoding the first %d of %d bytes: err = %v, want errCorruptSpan", n, len(full), err)
		}
	}
}

func TestCodecRejectsMalformedInput(t *testing.T) {
	s := richSpan()
	good := encodeSpan(nil, &s)

	tests := map[string][]byte{
		"unknown version": append([]byte{99}, good[1:]...),
		"trailing bytes":  append(append([]byte{}, good...), 0),
		// Version, three identifiers, then a name length far beyond the
		// record. This must fail cleanly, not attempt the allocation.
		"absurd string length": append(append([]byte{codecVersion}, make([]byte, 32)...), 0xff, 0xff, 0xff, 0xff, 0x0f),
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSpan(data); !errors.Is(err, errCorruptSpan) {
				t.Errorf("err = %v, want errCorruptSpan", err)
			}
		})
	}
}

func TestCodecRejectsUnknownValueKind(t *testing.T) {
	s := spec{1, 1, 0, "a", "b", 0, 0, false}.span()
	s.Attributes = []trace.KeyValue{{Key: "k", Value: trace.StringValue("v")}}
	data := encodeSpan(nil, &s)

	// The attribute's kind byte sits right after its one-byte key length and
	// the key itself, which is the last thing before the value "v" and the
	// empty resource list.
	kindAt := len(data) - 1 - 2 - 1
	data[kindAt] = 200

	if _, err := decodeSpan(data); !errors.Is(err, errCorruptSpan) {
		t.Errorf("err = %v, want errCorruptSpan", err)
	}
}
