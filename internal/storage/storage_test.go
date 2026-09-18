package storage

import (
	"errors"
	"testing"
	"time"
)

func TestQueryValidate(t *testing.T) {
	base := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		q    Query
		ok   bool
	}{
		{"empty query", Query{}, true},
		{"service and operation", Query{Service: "checkout", Operation: "POST /pay"}, true},
		{"bounded window", Query{Start: base, End: base.Add(time.Hour)}, true},
		{"equal duration bounds", Query{MinDuration: time.Second, MaxDuration: time.Second}, true},
		{"maximum limit", Query{Limit: MaxLimit}, true},
		{"operation without service", Query{Operation: "GET /health"}, false},
		{"negative minimum", Query{MinDuration: -time.Second}, false},
		{"negative maximum", Query{MaxDuration: -time.Second}, false},
		{"maximum below minimum", Query{MinDuration: time.Second, MaxDuration: time.Millisecond}, false},
		{"window ends before it starts", Query{Start: base, End: base.Add(-time.Minute)}, false},
		{"negative limit", Query{Limit: -1}, false},
		{"limit above maximum", Query{Limit: MaxLimit + 1}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.q.Validate()
			if tc.ok && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
			if !tc.ok && !errors.Is(err, ErrBadQuery) {
				t.Errorf("Validate() = %v, want ErrBadQuery", err)
			}
		})
	}
}

func TestQueryLimitDefault(t *testing.T) {
	if got := (Query{}).limit(); got != DefaultLimit {
		t.Errorf("limit() = %d, want %d", got, DefaultLimit)
	}
	if got := (Query{Limit: 5}).limit(); got != 5 {
		t.Errorf("limit() = %d, want 5", got)
	}
}
