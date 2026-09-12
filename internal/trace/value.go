package trace

import (
	"math"
	"strconv"
)

// ValueKind says which of the typed accessors on a Value will succeed.
type ValueKind uint8

const (
	// ValueInvalid is the zero value, which is what you get from a Value
	// nobody filled in.
	ValueInvalid ValueKind = iota
	ValueString
	ValueBool
	ValueInt
	ValueFloat
)

func (k ValueKind) String() string {
	switch k {
	case ValueString:
		return "string"
	case ValueBool:
		return "bool"
	case ValueInt:
		return "int"
	case ValueFloat:
		return "float"
	default:
		return "invalid"
	}
}

// Value is one attribute value: a string, a bool, an integer or a float.
//
// It is a tagged union rather than an any, because attributes are the bulk of
// what this process holds. A trace worth keeping might carry a few thousand of
// them, and an any would put every integer and every float behind its own heap
// allocation. The three scalar kinds share one uint64 field instead.
//
// The struct is comparable, so == is attribute equality and a Value can be a
// map key. The query layer leans on that.
type Value struct {
	kind ValueKind
	num  uint64
	str  string
}

// StringValue returns a Value holding s.
func StringValue(s string) Value { return Value{kind: ValueString, str: s} }

// BoolValue returns a Value holding b.
func BoolValue(b bool) Value {
	var n uint64
	if b {
		n = 1
	}
	return Value{kind: ValueBool, num: n}
}

// IntValue returns a Value holding i.
func IntValue(i int64) Value { return Value{kind: ValueInt, num: uint64(i)} }

// FloatValue returns a Value holding f.
func FloatValue(f float64) Value { return Value{kind: ValueFloat, num: math.Float64bits(f)} }

// Kind reports which typed accessor will succeed.
func (v Value) Kind() ValueKind { return v.kind }

// AsString returns the string this Value holds, and false if it holds
// something else. It does not stringify other kinds; String does that.
func (v Value) AsString() (string, bool) {
	if v.kind != ValueString {
		return "", false
	}
	return v.str, true
}

// AsBool returns the bool this Value holds, and false if it holds something
// else.
func (v Value) AsBool() (bool, bool) {
	if v.kind != ValueBool {
		return false, false
	}
	return v.num == 1, true
}

// AsInt returns the integer this Value holds, and false if it holds something
// else. It does not convert from float, since an attribute that arrived as a
// float and reads back as an integer would quietly lose the distinction the
// sender made.
func (v Value) AsInt() (int64, bool) {
	if v.kind != ValueInt {
		return 0, false
	}
	return int64(v.num), true
}

// AsFloat returns the float this Value holds, and false if it holds something
// else.
func (v Value) AsFloat() (float64, bool) {
	if v.kind != ValueFloat {
		return 0, false
	}
	return math.Float64frombits(v.num), true
}

// String renders the value for humans: logs, the terminal waterfall, error
// messages. It is not a serialisation format and nothing should parse it.
func (v Value) String() string {
	switch v.kind {
	case ValueString:
		return v.str
	case ValueBool:
		return strconv.FormatBool(v.num == 1)
	case ValueInt:
		return strconv.FormatInt(int64(v.num), 10)
	case ValueFloat:
		return strconv.FormatFloat(math.Float64frombits(v.num), 'g', -1, 64)
	default:
		return "<invalid>"
	}
}

// KeyValue is a single named attribute.
type KeyValue struct {
	Key   string
	Value Value
}

// String renders the pair for humans, as key=value.
func (kv KeyValue) String() string { return kv.Key + "=" + kv.Value.String() }
