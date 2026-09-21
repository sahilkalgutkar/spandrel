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
// wrong shape obvious in a failure message.
func render(t *Tree) string {
	var b strings.Builder
	t.Walk(func(n *Node, depth int) bool {
		fmt.Fprintf(&b, "%s%d\n", strings.Repeat("  ", depth), n.Span.SpanID[7])
		return true
	})
	return b.String()
}

func mustBuild(t *testing.T, spans ...trace.Span) *Tree {
	t.Helper()
	tree, err := Build(spans)
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
			if _, err := Build(tt.spans); !errors.Is(err, tt.want) {
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
