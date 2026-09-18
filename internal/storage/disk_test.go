package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

func openDisk(t *testing.T, dir string, retention time.Duration, c *clock) *Disk {
	t.Helper()
	d, err := OpenDisk(dir, retention, c.now)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	return d
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"+segmentSuffix))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

func TestDiskSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}

	d := openDisk(t, dir, 0, c)
	rich := richSpan()
	if err := d.Write(ctx, []trace.Span{rich}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	write(t, d, spec{1, 7, 2, "db", "UPDATE", 20 * time.Millisecond, time.Millisecond, false})
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d = openDisk(t, dir, 0, c)
	defer d.Close()

	spans, err := d.Trace(ctx, tid(1))
	if err != nil {
		t.Fatalf("Trace after reopen: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("got %d spans after reopen, want 2", len(spans))
	}

	// Indexes are rebuilt, not just the raw spans.
	found, err := d.Find(ctx, Query{Service: "checkout", Operation: "POST /pay", MinDuration: 200 * time.Millisecond})
	if err != nil || len(found) != 1 {
		t.Errorf("Find after reopen = %v, %v; want the trace", found, err)
	}
}

// A crash part way through an append leaves a partial record at the end of
// the newest segment. That write was never acknowledged, so the store must
// cut it off and open with everything before it.
func TestDiskRepairsATornTail(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}

	d := openDisk(t, dir, 0, c)
	write(t, d, spec{1, 1, 0, "api", "a", 0, time.Millisecond, false})
	write(t, d, spec{2, 1, 0, "api", "b", 0, time.Millisecond, false})
	d.Close()

	seg := segmentFiles(t, dir)[0]
	info, _ := os.Stat(seg)
	if err := os.Truncate(seg, info.Size()-3); err != nil {
		t.Fatalf("simulating a torn write: %v", err)
	}

	d = openDisk(t, dir, 0, c)
	if d.Len() != 1 {
		t.Errorf("Len = %d after repair, want the one intact trace", d.Len())
	}

	// New writes must land after the repaired end, not after the garbage.
	write(t, d, spec{3, 1, 0, "api", "c", 0, time.Millisecond, false})
	d.Close()

	d = openDisk(t, dir, 0, c)
	defer d.Close()
	if d.Len() != 2 {
		t.Errorf("Len = %d after writing past a repair and reopening, want 2", d.Len())
	}
}

// A bad record in an older segment cannot be a crash, because nothing appends
// to an older segment. Opening anyway would quietly lose data.
func TestDiskRefusesCorruptionInASealedSegment(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}

	d := openDisk(t, dir, 0, c)
	write(t, d, spec{1, 1, 0, "api", "a", 0, time.Millisecond, false})
	c.set(base.Add(time.Hour))
	write(t, d, spec{2, 1, 0, "api", "b", 0, time.Millisecond, false})
	d.Close()

	segs := segmentFiles(t, dir)
	if len(segs) != 2 {
		t.Fatalf("got %d segments, want one per hour", len(segs))
	}

	data, _ := os.ReadFile(segs[0])
	data[recordHeader+5] ^= 0xff
	if err := os.WriteFile(segs[0], data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenDisk(dir, 0, c.now); !errors.Is(err, ErrCorruptSegment) {
		t.Errorf("OpenDisk = %v, want ErrCorruptSegment", err)
	}
}

func TestDiskRejectsAnAbsurdRecordLength(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}

	d := openDisk(t, dir, 0, c)
	write(t, d, spec{1, 1, 0, "api", "a", 0, time.Millisecond, false})
	c.set(base.Add(time.Hour))
	write(t, d, spec{2, 1, 0, "api", "b", 0, time.Millisecond, false})
	d.Close()

	seg := segmentFiles(t, dir)[0]
	data, _ := os.ReadFile(seg)
	binary.LittleEndian.PutUint32(data[0:4], maxRecord+1)
	os.WriteFile(seg, data, 0o644)

	if _, err := OpenDisk(dir, 0, c.now); !errors.Is(err, ErrCorruptSegment) {
		t.Errorf("OpenDisk = %v, want ErrCorruptSegment", err)
	}
}

func TestDiskRetentionDeletesWholeSegments(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}
	d := openDisk(t, dir, 2*time.Hour, c)
	defer d.Close()

	for h := range 5 {
		c.set(base.Add(time.Duration(h) * time.Hour))
		write(t, d, spec{byte(h + 1), 1, 0, "api", "op", time.Duration(h) * time.Hour, time.Minute, false})
	}
	if n := len(segmentFiles(t, dir)); n != 5 {
		t.Fatalf("got %d segments, want 5", n)
	}

	// At 04:30 the cutoff is 02:30. Traces are judged by when they ended, so
	// the ones ending at 00:01, 01:01 and 02:01 all go.
	c.set(base.Add(4*time.Hour + 30*time.Minute))
	if n := d.EvictExpired(); n != 3 {
		t.Errorf("EvictExpired dropped %d traces, want 3", n)
	}

	// Segments are judged by the hour they cover, which is coarser. The 02:00
	// segment could have taken writes up to 03:00, after the cutoff, so it
	// stays on disk even though the only trace in it has expired.
	if n := len(segmentFiles(t, dir)); n != 3 {
		t.Errorf("got %d segments after eviction, want 3", n)
	}

	// Reopening replays that segment and then evicts again, so what a restart
	// serves agrees with what the running process served.
	d.Close()
	d = openDisk(t, dir, 2*time.Hour, c)
	if d.Len() != 2 {
		t.Errorf("Len after reopen = %d, want 2", d.Len())
	}
}

func TestDiskIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"README", "spans-notanumber.seg", "spans-12.tmp"} {
		os.WriteFile(filepath.Join(dir, name), []byte("junk"), 0o644)
	}
	os.Mkdir(filepath.Join(dir, "spans-1.seg"), 0o755)

	d := openDisk(t, dir, 0, &clock{t: base})
	defer d.Close()
	if d.Len() != 0 {
		t.Errorf("Len = %d, want 0", d.Len())
	}
}

func TestDiskWriteValidatesBeforeAppending(t *testing.T) {
	dir := t.TempDir()
	d := openDisk(t, dir, 0, &clock{t: base})
	defer d.Close()

	bad := spec{1, 1, 0, "api", "", 0, time.Millisecond, false}.span()
	if err := d.Write(ctx, []trace.Span{bad}); !errors.Is(err, trace.ErrNoName) {
		t.Fatalf("Write = %v, want ErrNoName", err)
	}
	// Nothing reaches disk, so a restart cannot resurrect a rejected span
	// and then refuse to open over it.
	for _, seg := range segmentFiles(t, dir) {
		if info, _ := os.Stat(seg); info.Size() != 0 {
			t.Errorf("%s has %d bytes after a rejected write", seg, info.Size())
		}
	}
}

func TestDiskClosed(t *testing.T) {
	d := openDisk(t, t.TempDir(), 0, &clock{t: base})
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if err := d.Accept(ctx, []trace.Span{spec{1, 1, 0, "a", "b", 0, 0, false}.span()}); !errors.Is(err, ErrClosed) {
		t.Errorf("Accept after Close = %v, want ErrClosed", err)
	}
}

func TestOpenDiskFailsOnAFileWhereTheDirectoryShouldBe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "taken")
	os.WriteFile(path, nil, 0o644)
	if _, err := OpenDisk(path, 0, nil); err == nil {
		t.Error("OpenDisk succeeded on a path that is a regular file")
	}
}

// Syncing every batch is what makes an acknowledged export durable, and this
// shows what it costs next to BenchmarkMemoryWrite.
func BenchmarkDiskWrite(b *testing.B) {
	d, err := OpenDisk(b.TempDir(), 0, func() time.Time { return base })
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	batch := make([]trace.Span, 100)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := range batch {
			batch[i] = richSpan()
			batch[i].SpanID[7] = byte(i)
			batch[i].TraceID[15] = byte(n)
			batch[i].TraceID[14] = byte(n >> 8)
		}
		if err := d.Write(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(batch))/b.Elapsed().Seconds(), "spans/s")
}

// flakyFile fails one operation on demand, then behaves.
type flakyFile struct {
	segmentFile
	failWrite    int // write this many bytes of the next Write, then fail it
	failSync     bool
	failTruncate bool
}

func (f *flakyFile) Write(b []byte) (int, error) {
	if f.failWrite > 0 {
		n, _ := f.segmentFile.Write(b[:f.failWrite])
		f.failWrite = 0
		return n, errors.New("no space left on device")
	}
	return f.segmentFile.Write(b)
}

func (f *flakyFile) Sync() error {
	if f.failSync {
		f.failSync = false
		return errors.New("input/output error")
	}
	return f.segmentFile.Sync()
}

func (f *flakyFile) Truncate(size int64) error {
	if f.failTruncate {
		f.failTruncate = false
		return errors.New("input/output error")
	}
	return f.segmentFile.Truncate(size)
}

// A failed append must leave the segment exactly as it was. If the partial
// record stayed, the next good write would land after it, and on restart
// replay would cut the segment at the partial record and drop that good,
// acknowledged write along with it.
func TestDiskFailedAppendLosesNothingElse(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(*flakyFile)
	}{
		{"write fails part way", func(f *flakyFile) { f.failWrite = 5 }},
		{"sync fails after the write", func(f *flakyFile) { f.failSync = true }},
		{"write fails and so does the rollback", func(f *flakyFile) { f.failWrite = 5; f.failTruncate = true }},
		{"sync fails and so does the rollback", func(f *flakyFile) { f.failSync = true; f.failTruncate = true }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			c := &clock{t: base}
			d := openDisk(t, dir, 0, c)

			var flaky *flakyFile
			d.open = func(path string) (segmentFile, error) {
				f, err := openSegmentFile(path)
				if err != nil {
					return nil, err
				}
				flaky = &flakyFile{segmentFile: f}
				return flaky, nil
			}

			write(t, d, spec{1, 1, 0, "api", "before", 0, time.Millisecond, false})

			tc.break_(flaky)
			failed := spec{2, 1, 0, "api", "failed", 0, time.Millisecond, false}.span()
			if err := d.Write(ctx, []trace.Span{failed}); err == nil {
				t.Fatal("Write succeeded despite the injected failure")
			}
			if _, err := d.Trace(ctx, tid(2)); !errors.Is(err, ErrNotFound) {
				t.Error("a span whose append failed became visible")
			}

			write(t, d, spec{3, 1, 0, "api", "after", 0, time.Millisecond, false})
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			d = openDisk(t, dir, 0, c)
			defer d.Close()
			for _, id := range []byte{1, 3} {
				if _, err := d.Trace(ctx, tid(id)); err != nil {
					t.Errorf("acknowledged trace %d is missing after restart: %v", id, err)
				}
			}
			if _, err := d.Trace(ctx, tid(2)); !errors.Is(err, ErrNotFound) {
				t.Error("the failed span came back after restart")
			}
		})
	}
}

func TestDiskRejectsAnOversizedSpan(t *testing.T) {
	d := openDisk(t, t.TempDir(), 0, &clock{t: base})
	defer d.Close()

	s := spec{1, 1, 0, "api", "op", 0, time.Millisecond, false}.span()
	s.Attributes = []trace.KeyValue{{Key: "blob", Value: trace.StringValue(string(make([]byte, maxRecord)))}}
	if err := d.Write(ctx, []trace.Span{s}); err == nil {
		t.Error("a span larger than a record may be was written")
	}
}

// The hour can change between a failed rollback and the next write. The dirty
// segment is the old hour's, and it has to be repaired even though the next
// append goes to a new file, or a restart finds garbage in a sealed segment
// and refuses to open.
func TestDiskRepairsTheDirtySegmentAfterAnHourChange(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}
	d := openDisk(t, dir, 0, c)

	var flaky *flakyFile
	d.open = func(path string) (segmentFile, error) {
		f, err := openSegmentFile(path)
		if err != nil {
			return nil, err
		}
		flaky = &flakyFile{segmentFile: f}
		return flaky, nil
	}

	write(t, d, spec{1, 1, 0, "api", "before", 0, time.Millisecond, false})
	flaky.failWrite, flaky.failTruncate = 5, true
	if err := d.Write(ctx, []trace.Span{spec{2, 1, 0, "api", "failed", 0, time.Millisecond, false}.span()}); err == nil {
		t.Fatal("Write succeeded despite the injected failure")
	}

	c.set(base.Add(time.Hour))
	write(t, d, spec{3, 1, 0, "api", "after", 0, time.Millisecond, false})
	d.Close()

	d, err := OpenDisk(dir, 0, c.now)
	if err != nil {
		t.Fatalf("OpenDisk after a repaired sealed segment: %v", err)
	}
	defer d.Close()
	if d.Len() != 2 {
		t.Errorf("Len = %d, want the two acknowledged traces", d.Len())
	}
}

// If the repair itself fails, the write must fail too rather than append past
// bytes nobody vouched for. The next write gets another go at the repair.
func TestDiskFailedRepairFailsTheWrite(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: base}
	d := openDisk(t, dir, 0, c)
	defer d.Close()

	write(t, d, spec{1, 1, 0, "api", "before", 0, time.Millisecond, false})

	// Point the dirty state at a path that cannot be truncated: a directory.
	d.mu.Lock()
	_ = d.seg.Close()
	d.seg, d.segHour = nil, -1
	d.dirty, d.dirtyHour, d.dirtySize = true, 999, 0
	d.mu.Unlock()
	if err := os.Mkdir(d.path(999), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := d.Write(ctx, []trace.Span{spec{2, 1, 0, "api", "x", 0, time.Millisecond, false}.span()}); err == nil {
		t.Error("Write appended even though the dirty segment could not be repaired")
	}
	if !d.dirty {
		t.Error("a failed repair cleared the dirty flag")
	}
}
