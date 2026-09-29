package assembly

import (
	"fmt"
	"math/rand/v2"
	"strings"
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

// pathString renders a critical path as "span:start-end" in milliseconds.
func pathString(path []Segment) string {
	var b strings.Builder
	for i, s := range path {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d:%d-%d", s.Node.Span.SpanID[7], s.Start.Sub(epoch).Milliseconds(), s.End.Sub(epoch).Milliseconds())
	}
	return b.String()
}

func TestCriticalPath(t *testing.T) {
	tests := []struct {
		name     string
		children []trace.Span
		want     string
	}{
		{
			name: "a span with no children is its own path",
			want: "1:0-100",
		},
		{
			name: "calls one after another are all on the path",
			children: []trace.Span{
				span(2, 1, 10, 30),
				span(3, 1, 40, 60),
			},
			want: "1:0-10 2:10-30 1:30-40 3:40-60 1:60-100",
		},
		{
			name: "of parallel calls, the path follows the one that finished last",
			children: []trace.Span{
				span(2, 1, 10, 60),
				span(3, 1, 20, 80),
			},
			want: "1:0-10 2:10-20 3:20-80 1:80-100",
		},
		{
			name: "of two calls finishing together, the path follows the later start",
			children: []trace.Span{
				span(2, 1, 10, 60),
				span(3, 1, 30, 60),
			},
			want: "1:0-10 2:10-30 3:30-60 1:60-100",
		},
		{
			name: "a parallel call that finished early is off the path",
			children: []trace.Span{
				span(2, 1, 10, 90),
				span(3, 1, 20, 40),
			},
			want: "1:0-10 2:10-90 1:90-100",
		},
		{
			name: "the path goes down into grandchildren",
			children: []trace.Span{
				span(2, 1, 10, 90),
				span(3, 2, 20, 50),
				span(4, 3, 30, 40),
			},
			want: "1:0-10 2:10-20 3:20-30 4:30-40 3:40-50 2:50-90 1:90-100",
		},
		{
			name: "async work past the parent's end is cut at the end",
			children: []trace.Span{
				span(2, 1, 80, 300),
			},
			want: "1:0-80 2:80-100",
		},
		{
			name: "a child skewed to before its parent starts is cut at the start",
			children: []trace.Span{
				span(2, 1, -20, 30),
			},
			want: "2:0-30 1:30-100",
		},
		{
			name: "a call that took no time does not split the parent",
			children: []trace.Span{
				span(2, 1, 50, 50),
			},
			want: "1:0-100",
		},
		{
			name: "a call entirely after the parent ended is off the path",
			children: []trace.Span{
				span(2, 1, 150, 200),
			},
			want: "1:0-100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := mustBuild(t, append(tt.children, span(1, 0, 0, 100))...)
			root, _ := tree.Node(sid(1))
			if got := pathString(root.CriticalPath()); got != tt.want {
				t.Errorf("CriticalPath() = %s\nwant             %s", got, tt.want)
			}
		})
	}
}

func TestCriticalPathThroughAMissingParent(t *testing.T) {
	tree := mustBuildIn(t, Sealed,
		span(3, 2, 10, 30),
		span(4, 2, 50, 90),
	)

	path := tree.Missing()[0].CriticalPath()
	if got, want := pathString(path), "3:10-30 2:30-50 4:50-90"; got != want {
		t.Errorf("CriticalPath() = %s, want %s", got, want)
	}
	if !path[1].Node.Missing() {
		t.Error("the gap between the children is not attributed to the placeholder")
	}
	if got := path[1].Duration(); got != ms(20) {
		t.Errorf("gap segment lasts %v, want 20ms", got)
	}
}

// Whatever the shape of the trace, the path has to cover the root exactly
// once, with each segment inside the span it is charged to and that span
// somewhere under the root.
func TestCriticalPathCoversTheRoot(t *testing.T) {
	for seed := range uint64(randomTraces) {
		rng := rand.New(rand.NewPCG(seed, 4))
		spans := randomTrace(rng, 2+rng.IntN(80))

		// Let some calls run past their parent, as async work does.
		for i := range spans[1:] {
			if rng.IntN(4) == 0 {
				spans[i+1].End = spans[i+1].End.Add(time.Duration(rng.Int64N(int64(time.Second))))
			}
		}

		tree := mustBuild(t, shuffled(rng, spans)...)
		root := tree.Roots[0]
		path := root.CriticalPath()

		if len(path) == 0 {
			t.Fatalf("seed %d: empty path", seed)
		}
		if !path[0].Start.Equal(root.Span.Start) || !path[len(path)-1].End.Equal(root.Span.End) {
			t.Fatalf("seed %d: path runs %v to %v, want the root's %v to %v", seed,
				path[0].Start, path[len(path)-1].End, root.Span.Start, root.Span.End)
		}
		for i, s := range path {
			if !s.End.After(s.Start) {
				t.Fatalf("seed %d: segment %d is empty", seed, i)
			}
			if i > 0 && !s.Start.Equal(path[i-1].End) {
				t.Fatalf("seed %d: gap or overlap before segment %d", seed, i)
			}
			if i > 0 && s.Node == path[i-1].Node {
				t.Fatalf("seed %d: segments %d and %d are the same span and were not merged", seed, i-1, i)
			}
			if s.Start.Before(s.Node.Span.Start) || s.End.After(s.Node.Span.End) {
				t.Fatalf("seed %d: segment %d is outside the span it is charged to", seed, i)
			}
			n := s.Node
			for n != root && n != nil {
				n = n.Parent
			}
			if n != root {
				t.Fatalf("seed %d: segment %d is charged to a span outside the root", seed, i)
			}
		}
	}
}
