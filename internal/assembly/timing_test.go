package assembly

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestSelfTime(t *testing.T) {
	tests := []struct {
		name     string
		children []trace.Span
		want     time.Duration
	}{
		{
			name: "no children means all of it",
			want: ms(100),
		},
		{
			name: "one call in the middle",
			children: []trace.Span{
				span(2, 1, 20, 50),
			},
			want: ms(70),
		},
		{
			name: "calls one after another",
			children: []trace.Span{
				span(2, 1, 10, 30),
				span(3, 1, 40, 60),
			},
			want: ms(60),
		},
		{
			name: "parallel calls are counted once, not twice",
			children: []trace.Span{
				span(2, 1, 10, 60),
				span(3, 1, 20, 50),
				span(4, 1, 40, 70),
			},
			want: ms(40),
		},
		{
			name: "an async call running past the end only counts up to it",
			children: []trace.Span{
				span(2, 1, 80, 300),
			},
			want: ms(80),
		},
		{
			name: "a child that starts early from clock skew is clipped too",
			children: []trace.Span{
				span(2, 1, -20, 10),
			},
			want: ms(90),
		},
		{
			name: "a child entirely outside the parent takes nothing",
			children: []trace.Span{
				span(2, 1, 150, 200),
			},
			want: ms(100),
		},
		{
			name: "children covering all of it leave nothing",
			children: []trace.Span{
				span(2, 1, 0, 60),
				span(3, 1, 50, 100),
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := mustBuild(t, append(tt.children, span(1, 0, 0, 100))...)
			root, _ := tree.Node(sid(1))
			if got := root.SelfTime(); got != tt.want {
				t.Errorf("SelfTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelfTimeOfAPlaceholder(t *testing.T) {
	tree := mustBuild(t, span(3, 2, 10, 90))
	if got := tree.Missing()[0].SelfTime(); got != 0 {
		t.Errorf("placeholder SelfTime() = %v, want 0", got)
	}
}

// In a trace where every call is synchronous, each moment of the root's
// duration belongs to exactly one span: the deepest one running then. The
// self times therefore add up to the root's duration exactly.
func TestSelfTimesOfNestedCallsAddUp(t *testing.T) {
	for seed := range uint64(randomTraces) {
		rng := rand.New(rand.NewPCG(seed, 3))
		spans := sequentialTrace(rng, 2+rng.IntN(60))
		tree := mustBuild(t, spans...)

		var total time.Duration
		tree.Walk(func(n *Node, _ int) bool {
			self := n.SelfTime()
			if self < 0 || self > n.Span.Duration() {
				t.Errorf("seed %d: %s has self time %v out of %v", seed, n.Span.SpanID, self, n.Span.Duration())
			}
			total += self
			return true
		})
		if want := spans[0].Duration(); total != want {
			t.Fatalf("seed %d: self times add up to %v, want the root's %v", seed, total, want)
		}
	}
}

// sequentialTrace makes a trace in which no two siblings overlap, the shape a
// single-threaded service produces. Each span splits its time into a random
// number of child calls with gaps between them.
func sequentialTrace(rng *rand.Rand, n int) []trace.Span {
	spans := []trace.Span{{
		TraceID: testTrace,
		SpanID:  wideID(1),
		Name:    "root",
		Service: "svc",
		Start:   epoch,
		End:     epoch.Add(time.Second),
	}}
	for next := 0; next < len(spans) && len(spans) < n; next++ {
		p := spans[next]
		calls := 1 + rng.IntN(4)
		slot := p.Duration() / time.Duration(calls)
		for i := 0; i < calls && len(spans) < n; i++ {
			if slot < 2 {
				break
			}
			from := p.Start.Add(slot * time.Duration(i))
			start := from.Add(time.Duration(rng.Int64N(int64(slot / 2))))
			end := start.Add(time.Duration(rng.Int64N(int64(from.Add(slot).Sub(start)))))
			spans = append(spans, trace.Span{
				TraceID:      testTrace,
				SpanID:       wideID(len(spans) + 1),
				ParentSpanID: p.SpanID,
				Name:         "call",
				Service:      "svc",
				Start:        start,
				End:          end,
			})
		}
	}
	return spans
}
