package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// codecVersion is the first byte of every encoded span. It costs one byte a
// span and is the difference between a format change being a migration and
// being a data loss.
const codecVersion = 1

var errCorruptSpan = errors.New("storage: corrupt span record")

// encodeSpan appends a compact binary form of s to b.
//
// This is a hand-written format rather than protobuf or gob. The model is
// small and fixed, gob would write its type description into every segment,
// and protobuf would need a second schema kept in step with the model by
// hand. The price is this file, which the round-trip tests pin down.
func encodeSpan(b []byte, s *trace.Span) []byte {
	b = append(b, codecVersion)
	b = append(b, s.TraceID[:]...)
	b = append(b, s.SpanID[:]...)
	b = append(b, s.ParentSpanID[:]...)
	b = appendString(b, s.Name)
	b = append(b, byte(s.Kind))
	b = appendString(b, s.Service)
	b = binary.AppendVarint(b, s.Start.UnixNano())
	b = binary.AppendVarint(b, s.End.UnixNano())
	b = append(b, byte(s.Status.Code))
	b = appendString(b, s.Status.Message)
	b = appendAttributes(b, s.Attributes)
	b = appendAttributes(b, s.Resource)
	return b
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendAttributes(b []byte, kvs []trace.KeyValue) []byte {
	b = binary.AppendUvarint(b, uint64(len(kvs)))
	for _, kv := range kvs {
		b = appendString(b, kv.Key)
		b = append(b, byte(kv.Value.Kind()))
		switch kv.Value.Kind() {
		case trace.ValueString:
			s, _ := kv.Value.AsString()
			b = appendString(b, s)
		case trace.ValueBool:
			v, _ := kv.Value.AsBool()
			if v {
				b = append(b, 1)
			} else {
				b = append(b, 0)
			}
		case trace.ValueInt:
			v, _ := kv.Value.AsInt()
			b = binary.AppendVarint(b, v)
		case trace.ValueFloat:
			v, _ := kv.Value.AsFloat()
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
		}
	}
	return b
}

// decoder reads the format encodeSpan writes. The first failure sticks, so
// decodeSpan can read every field and check once at the end instead of after
// each one.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) fail(what string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: truncated reading %s", errCorruptSpan, what)
	}
}

func (d *decoder) bytes(n int, what string) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || len(d.buf) < n {
		d.fail(what)
		return nil
	}
	out := d.buf[:n]
	d.buf = d.buf[n:]
	return out
}

func (d *decoder) byte(what string) byte {
	b := d.bytes(1, what)
	if b == nil {
		return 0
	}
	return b[0]
}

func (d *decoder) uvarint(what string) uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.fail(what)
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) varint(what string) int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.buf)
	if n <= 0 {
		d.fail(what)
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) string(what string) string {
	n := d.uvarint(what)
	// Bound the length by what is left before converting, so a corrupt
	// length cannot ask for an allocation the size of the address space.
	if n > uint64(len(d.buf)) {
		d.fail(what)
		return ""
	}
	return string(d.bytes(int(n), what))
}

func (d *decoder) attributes(what string) []trace.KeyValue {
	n := d.uvarint(what)
	if d.err != nil || n == 0 {
		return nil
	}
	// Every attribute takes at least two bytes, which bounds a sane count.
	if n > uint64(len(d.buf)/2) {
		d.fail(what)
		return nil
	}
	out := make([]trace.KeyValue, 0, n)
	for range n {
		key := d.string(what)
		kind := trace.ValueKind(d.byte(what))
		var v trace.Value
		switch kind {
		case trace.ValueString:
			v = trace.StringValue(d.string(what))
		case trace.ValueBool:
			v = trace.BoolValue(d.byte(what) == 1)
		case trace.ValueInt:
			v = trace.IntValue(d.varint(what))
		case trace.ValueFloat:
			if b := d.bytes(8, what); b != nil {
				v = trace.FloatValue(math.Float64frombits(binary.LittleEndian.Uint64(b)))
			}
		case trace.ValueInvalid:
		default:
			if d.err == nil {
				d.err = fmt.Errorf("%w: unknown value kind %d in %s", errCorruptSpan, kind, what)
			}
		}
		if d.err != nil {
			return nil
		}
		out = append(out, trace.KeyValue{Key: key, Value: v})
	}
	return out
}

func decodeSpan(p []byte) (trace.Span, error) {
	d := &decoder{buf: p}
	var s trace.Span

	if v := d.byte("version"); d.err == nil && v != codecVersion {
		return s, fmt.Errorf("%w: unknown format version %d", errCorruptSpan, v)
	}
	copy(s.TraceID[:], d.bytes(len(s.TraceID), "trace id"))
	copy(s.SpanID[:], d.bytes(len(s.SpanID), "span id"))
	copy(s.ParentSpanID[:], d.bytes(len(s.ParentSpanID), "parent span id"))
	s.Name = d.string("name")
	s.Kind = trace.SpanKind(d.byte("kind"))
	s.Service = d.string("service")
	s.Start = time.Unix(0, d.varint("start")).UTC()
	s.End = time.Unix(0, d.varint("end")).UTC()
	s.Status.Code = trace.StatusCode(d.byte("status code"))
	s.Status.Message = d.string("status message")
	s.Attributes = d.attributes("attributes")
	s.Resource = d.attributes("resource")

	if d.err != nil {
		return trace.Span{}, d.err
	}
	if len(d.buf) != 0 {
		return trace.Span{}, fmt.Errorf("%w: %d trailing bytes", errCorruptSpan, len(d.buf))
	}
	return s, nil
}
