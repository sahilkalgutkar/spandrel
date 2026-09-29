package assembly

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

var (
	testTrace = trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36}
	epoch     = time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
)

// sid makes a span identifier out of a single byte, so tests can name spans
// 1, 2, 3 and still read the tree shape at a glance. Zero stays the zero
// identifier, which is how a root says it has no parent.
func sid(n byte) trace.SpanID {
	var id trace.SpanID
	id[7] = n
	return id
}

// span builds a span that runs from start to end milliseconds after epoch.
func span(id, parent byte, start, end int) trace.Span {
	return trace.Span{
		TraceID:      testTrace,
		SpanID:       sid(id),
		ParentSpanID: sid(parent),
		Name:         fmt.Sprintf("op-%d", id),
		Service:      "svc",
		Start:        epoch.Add(time.Duration(start) * time.Millisecond),
		End:          epoch.Add(time.Duration(end) * time.Millisecond),
	}
}

// render draws the tree one node per line, indented by depth, which makes a
// wrong shape obvious in a failure message. Placeholders are marked with
// their presence.
func render(t *Tree) string {
	var b strings.Builder
	t.Walk(func(n *Node, depth int) bool {
		fmt.Fprintf(&b, "%s%d", strings.Repeat("  ", depth), n.Span.SpanID[7])
		if n.Missing() {
			fmt.Fprintf(&b, " (%s)", n.Presence)
		}
		if n.CycleBroken {
			b.WriteString(" (cycle)")
		}
		b.WriteByte('\n')
		return true
	})
	return b.String()
}

// mustBuild assembles an open trace, which is what most tests want: whether
// a trace is sealed only matters once something is missing.
func mustBuild(t *testing.T, spans ...trace.Span) *Tree {
	t.Helper()
	return mustBuildIn(t, Open, spans...)
}

func mustBuildIn(t *testing.T, phase Phase, spans ...trace.Span) *Tree {
	t.Helper()
	tree, err := Build(spans, phase)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return tree
}

func TestBuildShape(t *testing.T) {
	tests := []struct {
		name  string
		spans []trace.Span
		want  string
	}{
		{
			name:  "a lone root",
			spans: []trace.Span{span(1, 0, 0, 10)},
			want:  "1\n",
		},
		{
			name: "children arrive before their parent",
			spans: []trace.Span{
				span(3, 2, 20, 30),
				span(2, 1, 10, 40),
				span(1, 0, 0, 50),
			},
			want: "1\n  2\n    3\n",
		},
		{
			name: "siblings come out in start order, not arrival order",
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(4, 1, 60, 70),
				span(2, 1, 10, 20),
				span(3, 1, 30, 40),
			},
			want: "1\n  2\n  3\n  4\n",
		},
		{
			name: "siblings that start together fall back to identifier order",
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(3, 1, 10, 20),
				span(2, 1, 10, 30),
			},
			want: "1\n  2\n  3\n",
		},
		{
			name: "two roots, as broken instrumentation sometimes sends",
			spans: []trace.Span{
				span(5, 0, 50, 60),
				span(1, 0, 0, 10),
			},
			want: "1\n5\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := mustBuild(t, tt.spans...)
			if got := render(tree); got != tt.want {
				t.Errorf("tree:\n%s\nwant:\n%s", got, tt.want)
			}
			if tree.Len() != len(tt.spans) {
				t.Errorf("Len() = %d, want %d", tree.Len(), len(tt.spans))
			}
		})
	}
}

func TestBuildLinksParents(t *testing.T) {
	tree := mustBuild(t, span(1, 0, 0, 50), span(2, 1, 10, 40))

	child, ok := tree.Node(sid(2))
	if !ok {
		t.Fatal("span 2 is missing from the tree")
	}
	if child.Parent == nil || child.Parent.Span.SpanID != sid(1) {
		t.Errorf("span 2's parent = %v, want span 1", child.Parent)
	}
	if _, ok := tree.Node(sid(9)); ok {
		t.Error("Node found a span that was never added")
	}
}

func TestBuildKeepsTheLastDuplicate(t *testing.T) {
	first := span(2, 1, 10, 20)
	retried := span(2, 1, 10, 25)
	retried.Name = "retried"

	tree := mustBuild(t, span(1, 0, 0, 50), first, retried)

	if tree.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", tree.Len())
	}
	n, _ := tree.Node(sid(2))
	if n.Span.Name != "retried" {
		t.Errorf("span 2 is %q, want the retried copy", n.Span.Name)
	}
	root, _ := tree.Node(sid(1))
	if len(root.Children) != 1 {
		t.Errorf("root has %d children, want the duplicate counted once", len(root.Children))
	}
}

func TestBuildRejects(t *testing.T) {
	other := span(2, 0, 0, 10)
	other.TraceID[0] ^= 0xff

	tests := []struct {
		name  string
		spans []trace.Span
		want  error
	}{
		{name: "nothing", spans: nil, want: ErrNoSpans},
		{name: "two traces", spans: []trace.Span{span(1, 0, 0, 10), other}, want: ErrMixedTraces},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Build(tt.spans, Open); !errors.Is(err, tt.want) {
				t.Errorf("Build error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestWalkStopsEarly(t *testing.T) {
	tree := mustBuild(t,
		span(1, 0, 0, 100),
		span(2, 1, 10, 20),
		span(3, 1, 30, 40),
		span(4, 0, 200, 300),
	)

	var seen []byte
	tree.Walk(func(n *Node, _ int) bool {
		seen = append(seen, n.Span.SpanID[7])
		return n.Span.SpanID != sid(2)
	})

	if want := []byte{1, 2}; string(seen) != string(want) {
		t.Errorf("visited %v, want %v", seen, want)
	}
}

func TestBuildMissingParents(t *testing.T) {
	tests := []struct {
		name        string
		phase       Phase
		spans       []trace.Span
		want        string
		wantMissing []byte
	}{
		{
			name:  "a parent that has not arrived yet is awaited",
			phase: Open,
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(3, 2, 20, 30),
			},
			want:        "1\n2 (awaited)\n  3\n",
			wantMissing: []byte{2},
		},
		{
			name:  "the same parent in a sealed trace is lost",
			phase: Sealed,
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(3, 2, 20, 30),
			},
			want:        "1\n2 (lost)\n  3\n",
			wantMissing: []byte{2},
		},
		{
			name:  "siblings with the same missing parent stay together",
			phase: Sealed,
			spans: []trace.Span{
				span(4, 2, 40, 50),
				span(1, 0, 0, 100),
				span(3, 2, 20, 30),
			},
			want:        "1\n2 (lost)\n  3\n  4\n",
			wantMissing: []byte{2},
		},
		{
			name:  "a missing root leaves the rest of the trace under its placeholder",
			phase: Sealed,
			spans: []trace.Span{
				span(2, 1, 10, 90),
				span(3, 2, 20, 30),
				span(4, 1, 5, 95),
			},
			want:        "1 (lost)\n  4\n  2\n    3\n",
			wantMissing: []byte{1},
		},
		{
			name:  "separate gaps get separate placeholders, earliest first",
			phase: Open,
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(8, 7, 60, 70),
				span(6, 5, 20, 30),
			},
			want:        "1\n5 (awaited)\n  6\n7 (awaited)\n  8\n",
			wantMissing: []byte{5, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := mustBuildIn(t, tt.phase, tt.spans...)
			if got := render(tree); got != tt.want {
				t.Errorf("tree:\n%s\nwant:\n%s", got, tt.want)
			}

			var missing []byte
			for _, m := range tree.Missing() {
				missing = append(missing, m.Span.SpanID[7])
			}
			if string(missing) != string(tt.wantMissing) {
				t.Errorf("Missing() = %v, want %v", missing, tt.wantMissing)
			}
			if tree.Len() != len(tt.spans) {
				t.Errorf("Len() = %d, want %d: placeholders are not spans", tree.Len(), len(tt.spans))
			}
			if tree.Complete() {
				t.Error("Complete() = true for a tree with a missing parent")
			}
		})
	}
}

func TestPlaceholderCoversItsChildren(t *testing.T) {
	tree := mustBuildIn(t, Sealed,
		span(3, 2, 20, 30),
		span(4, 2, 10, 25),
	)

	p := tree.Missing()[0]
	if want := epoch.Add(10 * time.Millisecond); !p.Span.Start.Equal(want) {
		t.Errorf("placeholder starts at %v, want %v", p.Span.Start, want)
	}
	if want := epoch.Add(30 * time.Millisecond); !p.Span.End.Equal(want) {
		t.Errorf("placeholder ends at %v, want %v", p.Span.End, want)
	}
	if p.Span.TraceID != testTrace {
		t.Error("placeholder does not carry the trace identifier")
	}
	if _, ok := tree.Node(sid(2)); ok {
		t.Error("Node returned a placeholder as if it had arrived")
	}
}

func TestComplete(t *testing.T) {
	if !mustBuild(t, span(1, 0, 0, 100), span(2, 1, 10, 20)).Complete() {
		t.Error("a whole trace is not Complete")
	}
	if mustBuild(t, span(1, 0, 0, 10), span(2, 0, 20, 30)).Complete() {
		t.Error("a trace with two roots is Complete")
	}
}

func TestPresenceString(t *testing.T) {
	for p, want := range map[Presence]string{Received: "received", Awaited: "awaited", Lost: "lost"} {
		if got := p.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", p, got, want)
		}
	}
}

func TestBuildBreaksCycles(t *testing.T) {
	tests := []struct {
		name       string
		spans      []trace.Span
		want       string
		wantCycles []byte
	}{
		{
			name: "two spans naming each other",
			spans: []trace.Span{
				span(2, 3, 10, 20),
				span(3, 2, 15, 25),
			},
			want:       "2 (cycle)\n  3\n",
			wantCycles: []byte{2},
		},
		{
			name: "a loop of three is cut at its earliest span, not its first to arrive",
			spans: []trace.Span{
				span(4, 3, 30, 40),
				span(2, 4, 5, 50),
				span(3, 2, 20, 45),
			},
			want:       "2 (cycle)\n  3\n    4\n",
			wantCycles: []byte{2},
		},
		{
			name: "spans hanging off a loop come back with it",
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(2, 3, 10, 60),
				span(3, 2, 20, 50),
				span(4, 3, 30, 40),
				span(5, 4, 32, 38),
			},
			want:       "1\n2 (cycle)\n  3\n    4\n      5\n",
			wantCycles: []byte{2},
		},
		{
			name: "a span that is its own parent",
			spans: []trace.Span{
				span(1, 0, 0, 100),
				span(2, 2, 10, 20),
			},
			want:       "1\n2 (cycle)\n",
			wantCycles: []byte{2},
		},
		{
			name: "separate loops are each cut once",
			spans: []trace.Span{
				span(6, 7, 60, 70),
				span(7, 6, 65, 75),
				span(2, 3, 10, 20),
				span(3, 2, 15, 25),
			},
			want:       "2 (cycle)\n  3\n6 (cycle)\n  7\n",
			wantCycles: []byte{2, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := mustBuild(t, tt.spans...)
			if got := render(tree); got != tt.want {
				t.Errorf("tree:\n%s\nwant:\n%s", got, tt.want)
			}

			var cycles []byte
			for _, c := range tree.Cycles() {
				cycles = append(cycles, c.Span.SpanID[7])
			}
			if string(cycles) != string(tt.wantCycles) {
				t.Errorf("Cycles() = %v, want %v", cycles, tt.wantCycles)
			}
			if tree.Complete() {
				t.Error("Complete() = true for a tree that needed repair")
			}

			visited := 0
			tree.Walk(func(*Node, int) bool { visited++; return true })
			if visited != len(tt.spans) {
				t.Errorf("Walk visited %d nodes, want all %d", visited, len(tt.spans))
			}
		})
	}
}

func TestBrokenCycleKeepsItsParentID(t *testing.T) {
	tree := mustBuild(t, span(2, 3, 10, 20), span(3, 2, 15, 25))

	cut := tree.Cycles()[0]
	if cut.Parent != nil {
		t.Error("the cut span still has a parent node")
	}
	if cut.Span.ParentSpanID != sid(3) {
		t.Errorf("the cut span's ParentSpanID = %s, want it left as sent", cut.Span.ParentSpanID)
	}
	other, _ := tree.Node(sid(3))
	if len(other.Children) != 0 {
		t.Errorf("span 3 still lists %d children after the cut", len(other.Children))
	}
}
