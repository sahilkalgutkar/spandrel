package assembly

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

// The tests in this file check properties rather than shapes. Tests written
// by hand only cover arrival orders someone thought of. The network delivers
// spans in any order and drops some of them, so these run the builder
// against many random traces delivered that way.

const randomTraces = 200

// wideID numbers spans past the 255 that sid can name.
func wideID(n int) trace.SpanID {
	var id trace.SpanID
	id[6], id[7] = byte(n>>8), byte(n)
	return id
}

// randomTrace makes a well-formed trace of n spans. Each span after the root
// hangs off a random earlier one and runs inside it, the way a synchronous
// call would.
func randomTrace(rng *rand.Rand, n int) []trace.Span {
	spans := make([]trace.Span, 0, n)
	spans = append(spans, trace.Span{
		TraceID: testTrace,
		SpanID:  wideID(1),
		Name:    "root",
		Service: "svc",
		Start:   epoch,
		End:     epoch.Add(time.Second),
	})
	for i := 2; i <= n; i++ {
		p := spans[rng.IntN(len(spans))]
		// The +1 keeps the draw legal when the parent itself took no time.
		start := p.Start.Add(time.Duration(rng.Int64N(int64(p.Duration()) + 1)))
		end := start.Add(time.Duration(rng.Int64N(int64(p.End.Sub(start)) + 1)))
		spans = append(spans, trace.Span{
			TraceID:      testTrace,
			SpanID:       wideID(i),
			ParentSpanID: p.SpanID,
			Name:         fmt.Sprintf("op-%d", i),
			Service:      "svc",
			Start:        start,
			End:          end,
		})
	}
	return spans
}

// canonical renders a tree with full identifiers and every marker, so two
// trees render the same only if they are the same tree.
func canonical(t *Tree) string {
	var b strings.Builder
	t.Walk(func(n *Node, depth int) bool {
		fmt.Fprintf(&b, "%s%s %s cycle=%v\n", strings.Repeat(" ", depth), n.Span.SpanID, n.Presence, n.CycleBroken)
		return true
	})
	return b.String()
}

func shuffled(rng *rand.Rand, spans []trace.Span) []trace.Span {
	out := append([]trace.Span(nil), spans...)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func TestArrivalOrderDoesNotMatter(t *testing.T) {
	for seed := range uint64(randomTraces) {
		rng := rand.New(rand.NewPCG(seed, 1))
		spans := randomTrace(rng, 2+rng.IntN(60))

		// Drop a few spans and splice in a loop, so the orders being compared
		// also exercise placeholders and cycle breaking.
		spans = spans[:len(spans)-rng.IntN(len(spans)/2+1)]
		for i := range spans[1:] {
			if rng.IntN(5) == 0 {
				spans[i+1].ParentSpanID = wideID(5000 + i)
			}
		}
		spans = append(spans,
			span(0xf1, 0xf3, 10, 20),
			span(0xf2, 0xf1, 11, 21),
			span(0xf3, 0xf2, 12, 22),
		)

		want := canonical(mustBuildIn(t, Sealed, spans...))
		for range 10 {
			if got := canonical(mustBuildIn(t, Sealed, shuffled(rng, spans)...)); got != want {
				t.Fatalf("seed %d: a different arrival order built a different tree:\n%s\nwant:\n%s", seed, got, want)
			}
		}
	}
}

func TestDroppedSpansAreAccountedFor(t *testing.T) {
	for seed := range uint64(randomTraces) {
		rng := rand.New(rand.NewPCG(seed, 2))
		all := randomTrace(rng, 2+rng.IntN(80))

		var kept []trace.Span
		dropped := make(map[trace.SpanID]bool)
		for _, s := range all {
			if rng.IntN(3) == 0 {
				dropped[s.SpanID] = true
			} else {
				kept = append(kept, s)
			}
		}
		if len(kept) == 0 {
			continue
		}

		tree := mustBuildIn(t, Sealed, shuffled(rng, kept)...)

		if tree.Len() != len(kept) {
			t.Fatalf("seed %d: Len() = %d, want %d", seed, tree.Len(), len(kept))
		}
		if len(tree.Cycles()) != 0 {
			t.Fatalf("seed %d: a trace with no loops had %d cycles broken", seed, len(tree.Cycles()))
		}

		// Every span that arrived is in the tree exactly once, under the
		// parent it named or under that parent's placeholder.
		seen := make(map[trace.SpanID]int)
		tree.Walk(func(n *Node, _ int) bool {
			if n.Missing() {
				return true
			}
			seen[n.Span.SpanID]++
			switch {
			case n.Span.IsRoot():
				if n.Parent != nil {
					t.Errorf("seed %d: root %s has a parent", seed, n.Span.SpanID)
				}
			case n.Parent == nil:
				t.Errorf("seed %d: %s names a parent but has none in the tree", seed, n.Span.SpanID)
			case n.Parent.Span.SpanID != n.Span.ParentSpanID:
				t.Errorf("seed %d: %s hangs under %s, want %s", seed, n.Span.SpanID, n.Parent.Span.SpanID, n.Span.ParentSpanID)
			}
			return true
		})
		for _, s := range kept {
			if seen[s.SpanID] != 1 {
				t.Fatalf("seed %d: span %s appears %d times", seed, s.SpanID, seen[s.SpanID])
			}
		}

		// A placeholder exists only for a span that really was dropped, and
		// in a sealed trace it is lost, not awaited.
		for _, m := range tree.Missing() {
			if !dropped[m.Span.SpanID] {
				t.Errorf("seed %d: placeholder for %s, which arrived", seed, m.Span.SpanID)
			}
			if m.Presence != Lost {
				t.Errorf("seed %d: placeholder for %s is %s in a sealed trace", seed, m.Span.SpanID, m.Presence)
			}
		}

		// A dropped leaf leaves no evidence behind, since nothing that
		// arrived names it. The tree can only be incomplete in a way it can
		// see: some span that arrived names a parent that did not.
		orphaned := false
		for _, s := range kept {
			orphaned = orphaned || dropped[s.ParentSpanID]
		}
		if tree.Complete() == orphaned {
			t.Errorf("seed %d: Complete() = %v, but a kept span lost its parent: %v", seed, tree.Complete(), orphaned)
		}
	}
}
