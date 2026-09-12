package trace

import (
	"math"
	"testing"
)

func TestValueRoundTrips(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		kind    ValueKind
		display string
	}{
		{name: "string", value: StringValue("checkout"), kind: ValueString, display: "checkout"},
		{name: "empty string", value: StringValue(""), kind: ValueString, display: ""},
		{name: "true", value: BoolValue(true), kind: ValueBool, display: "true"},
		{name: "false", value: BoolValue(false), kind: ValueBool, display: "false"},
		{name: "positive int", value: IntValue(503), kind: ValueInt, display: "503"},
		{name: "negative int", value: IntValue(-1), kind: ValueInt, display: "-1"},
		{name: "int min", value: IntValue(math.MinInt64), kind: ValueInt, display: "-9223372036854775808"},
		{name: "int max", value: IntValue(math.MaxInt64), kind: ValueInt, display: "9223372036854775807"},
		{name: "float", value: FloatValue(12.5), kind: ValueFloat, display: "12.5"},
		{name: "negative float", value: FloatValue(-0.125), kind: ValueFloat, display: "-0.125"},
		{name: "zero value", value: Value{}, kind: ValueInvalid, display: "<invalid>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.value.Kind(); got != tc.kind {
				t.Errorf("Kind() = %v, want %v", got, tc.kind)
			}
			if got := tc.value.String(); got != tc.display {
				t.Errorf("String() = %q, want %q", got, tc.display)
			}
		})
	}
}

func TestValueKindNames(t *testing.T) {
	kinds := map[ValueKind]string{
		ValueInvalid:   "invalid",
		ValueString:    "string",
		ValueBool:      "bool",
		ValueInt:       "int",
		ValueFloat:     "float",
		ValueKind(200): "invalid",
	}
	for kind, want := range kinds {
		if got := kind.String(); got != want {
			t.Errorf("ValueKind(%d).String() = %q, want %q", kind, got, want)
		}
	}
}

func TestTypedAccessors(t *testing.T) {
	if got, ok := StringValue("checkout").AsString(); !ok || got != "checkout" {
		t.Errorf("AsString() = %q, %v; want %q, true", got, ok, "checkout")
	}
	if got, ok := BoolValue(true).AsBool(); !ok || !got {
		t.Errorf("AsBool() = %v, %v; want true, true", got, ok)
	}
	if got, ok := IntValue(-7).AsInt(); !ok || got != -7 {
		t.Errorf("AsInt() = %d, %v; want -7, true", got, ok)
	}
	if got, ok := FloatValue(2.75).AsFloat(); !ok || got != 2.75 {
		t.Errorf("AsFloat() = %v, %v; want 2.75, true", got, ok)
	}
}

// An accessor must not guess. An attribute that arrived as a float and reads
// back as an integer would silently discard the distinction the sender made,
// and the query layer would then match on something the sender never sent.
func TestAccessorsDoNotConvertAcrossKinds(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{name: "string", value: StringValue("42")},
		{name: "bool", value: BoolValue(true)},
		{name: "int", value: IntValue(42)},
		{name: "float", value: FloatValue(42)},
		{name: "invalid", value: Value{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := tc.value.AsString(); ok != (tc.value.Kind() == ValueString) {
				t.Errorf("AsString() ok = %v on a %v", ok, tc.value.Kind())
			}
			if _, ok := tc.value.AsBool(); ok != (tc.value.Kind() == ValueBool) {
				t.Errorf("AsBool() ok = %v on a %v", ok, tc.value.Kind())
			}
			if _, ok := tc.value.AsInt(); ok != (tc.value.Kind() == ValueInt) {
				t.Errorf("AsInt() ok = %v on a %v", ok, tc.value.Kind())
			}
			if _, ok := tc.value.AsFloat(); ok != (tc.value.Kind() == ValueFloat) {
				t.Errorf("AsFloat() ok = %v on a %v", ok, tc.value.Kind())
			}
		})
	}
}

// The query layer compares attribute values with == and uses them as map
// keys, so this property needs a test guarding it rather than a comment
// hoping for the best.
func TestValueIsComparable(t *testing.T) {
	if StringValue("a") != StringValue("a") {
		t.Error("equal strings compared unequal")
	}
	if StringValue("a") == StringValue("b") {
		t.Error("different strings compared equal")
	}

	// Same underlying bits, different kinds. IntValue(1) and BoolValue(true)
	// both park a 1 in the shared field, and conflating them would make an
	// attribute search for true match a span that sent 1.
	if IntValue(1) == BoolValue(true) {
		t.Error("an int and a bool with the same bit pattern compared equal")
	}

	seen := map[Value]string{
		StringValue("checkout"): "a",
		IntValue(503):           "b",
	}
	if seen[StringValue("checkout")] != "a" {
		t.Error("a Value did not work as a map key")
	}
}

func TestFloatSpecialValues(t *testing.T) {
	nan := FloatValue(math.NaN())
	got, ok := nan.AsFloat()
	if !ok {
		t.Fatal("AsFloat() on a NaN reported the wrong kind")
	}
	if !math.IsNaN(got) {
		t.Errorf("AsFloat() = %v, want NaN", got)
	}

	inf := FloatValue(math.Inf(1))
	if got, ok := inf.AsFloat(); !ok || !math.IsInf(got, 1) {
		t.Errorf("AsFloat() = %v, %v; want +Inf, true", got, ok)
	}
}

func TestKeyValueString(t *testing.T) {
	kv := KeyValue{Key: "http.status_code", Value: IntValue(503)}
	if got, want := kv.String(), "http.status_code=503"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
