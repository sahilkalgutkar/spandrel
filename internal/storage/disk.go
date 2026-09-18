package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sahilkalgutkar/spandrel/internal/trace"
)

const (
	segmentPrefix = "spans-"
	segmentSuffix = ".seg"

	// recordHeader is a little-endian uint32 payload length followed by a
	// CRC-32C of the payload.
	recordHeader = 8

	// maxRecord bounds one span on disk. Anything bigger is not a span the
	// ingest path would have accepted, so reading a length above it means
	// the header is garbage.
	maxRecord = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// segmentFile is the part of *os.File the append path uses. It exists so tests
// can make a write, sync or truncate fail on demand. The rollback those
// failures exercise is otherwise reachable only with a full or failing disk.
type segmentFile interface {
	io.Writer
	io.Seeker
	Sync() error
	Truncate(size int64) error
	Close() error
}

func openSegmentFile(path string) (segmentFile, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
}

// ErrCorruptSegment means a sealed segment holds a record that does not
// check out. Unlike a torn tail in the newest segment, this cannot be the
// result of a crash, so the store refuses to open rather than guess.
var ErrCorruptSegment = errors.New("storage: corrupt segment")

// Disk is a Store that survives a restart.
//
// Spans are appended to one segment file per hour of wall-clock write time,
// and every search is answered by an in-memory store that Disk rebuilds by
// replaying the segments on open. Writes reach disk and are synced before
// they become visible, so a span a client was told was accepted is a span
// that will be there after a crash.
//
// Hourly segments make retention cheap. Expiring an hour of spans deletes a
// file instead of rewriting one, so there is no compaction step and nothing
// on disk is ever modified after it is written - apart from truncating a torn
// final record, which only happens on open.
type Disk struct {
	mem       *Memory
	dir       string
	now       func() time.Time
	retention time.Duration

	mu      sync.Mutex
	closed  bool
	open    func(path string) (segmentFile, error)
	seg     segmentFile
	segHour int64
	segSize int64
	buf     []byte

	// dirty means a failed append could not be rolled back, so the segment
	// for dirtyHour may end in bytes no client was told about. dirtySize is
	// where its last acknowledged record ended. The next write cuts the file
	// back to exactly that before appending anything.
	dirty     bool
	dirtyHour int64
	dirtySize int64
}

// OpenDisk opens or creates a store in dir and replays whatever it holds.
func OpenDisk(dir string, retention time.Duration, now func() time.Time) (*Disk, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating %s: %w", dir, err)
	}

	d := &Disk{
		mem:       NewMemory(retention, now),
		dir:       dir,
		now:       now,
		retention: retention,
		open:      openSegmentFile,
		segHour:   -1,
	}

	hours, err := d.segments()
	if err != nil {
		return nil, err
	}
	for i, hour := range hours {
		last := i == len(hours)-1
		if err := d.replay(hour, last); err != nil {
			return nil, err
		}
	}

	d.EvictExpired()
	return d, nil
}

// segments lists segment hours on disk, oldest first.
func (d *Disk) segments() ([]int64, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, fmt.Errorf("storage: listing %s: %w", d.dir, err)
	}
	var hours []int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		hour, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix), 10, 64)
		if err != nil {
			continue
		}
		hours = append(hours, hour)
	}
	slices.Sort(hours)
	return hours, nil
}

func (d *Disk) path(hour int64) string {
	return filepath.Join(d.dir, segmentPrefix+strconv.FormatInt(hour, 10)+segmentSuffix)
}

// replay loads one segment into memory.
//
// A bad record at the end of the newest segment is what a crash mid-append
// leaves behind. That tail was never acknowledged to a client, so it is cut
// off and the store opens. A bad record anywhere else is corruption, and the
// store refuses to open instead of silently skipping data.
func (d *Disk) replay(hour int64, last bool) error {
	path := d.path(hour)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("storage: reading %s: %w", path, err)
	}

	spans, good, err := intactPrefix(data)
	if err != nil {
		if !last {
			return fmt.Errorf("%w: %s at offset %d: %v", ErrCorruptSegment, path, good, err)
		}
		if err := os.Truncate(path, int64(good)); err != nil {
			return fmt.Errorf("storage: truncating torn tail of %s: %w", path, err)
		}
	}

	if len(spans) == 0 {
		return nil
	}
	if err := d.mem.Write(context.Background(), spans); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrCorruptSegment, path, err)
	}
	return nil
}

// intactPrefix decodes records from the start of data until one fails. It
// returns the spans read, the length of the intact prefix, and the error that
// stopped it, which is nil when every byte was a good record.
func intactPrefix(data []byte) ([]trace.Span, int, error) {
	var (
		spans []trace.Span
		good  int
	)
	for good < len(data) {
		span, n, err := readRecord(data[good:])
		if err != nil {
			return spans, good, err
		}
		spans = append(spans, span)
		good += n
	}
	return spans, good, nil
}

// readRecord decodes the record at the start of data and reports its size.
func readRecord(data []byte) (trace.Span, int, error) {
	if len(data) < recordHeader {
		return trace.Span{}, 0, io.ErrUnexpectedEOF
	}
	size := binary.LittleEndian.Uint32(data[0:4])
	sum := binary.LittleEndian.Uint32(data[4:8])
	if size > maxRecord {
		return trace.Span{}, 0, fmt.Errorf("record length %d exceeds %d", size, maxRecord)
	}
	end := recordHeader + int(size)
	if len(data) < end {
		return trace.Span{}, 0, io.ErrUnexpectedEOF
	}
	payload := data[recordHeader:end]
	if crc32.Checksum(payload, castagnoli) != sum {
		return trace.Span{}, 0, errors.New("checksum mismatch")
	}
	span, err := decodeSpan(payload)
	if err != nil {
		return trace.Span{}, 0, err
	}
	return span, end, nil
}

// Write appends spans to the current segment, syncs it, and only then makes
// them visible to searches.
func (d *Disk) Write(ctx context.Context, spans []trace.Span) error {
	for i := range spans {
		if err := spans[i].Validate(); err != nil {
			return fmt.Errorf("storage: span %d: %w", i, err)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrClosed
	}
	if err := d.roll(); err != nil {
		return err
	}

	d.buf = d.buf[:0]
	for i := range spans {
		start := len(d.buf)
		d.buf = append(d.buf, make([]byte, recordHeader)...)
		d.buf = encodeSpan(d.buf, &spans[i])
		payload := d.buf[start+recordHeader:]
		if len(payload) > maxRecord {
			return fmt.Errorf("storage: span %d encodes to %d bytes, over the %d limit", i, len(payload), maxRecord)
		}
		binary.LittleEndian.PutUint32(d.buf[start:], uint32(len(payload)))
		binary.LittleEndian.PutUint32(d.buf[start+4:], crc32.Checksum(payload, castagnoli))
	}

	if err := d.append(d.buf); err != nil {
		return err
	}
	return d.mem.Write(ctx, spans)
}

// append writes and syncs b, and on any failure truncates the segment back to
// where it was. Without the rollback, a failed write would leave a partial
// record in the middle of the segment once later appends succeed, and replay
// would read that as corruption rather than a torn tail.
func (d *Disk) append(b []byte) error {
	if _, err := d.seg.Write(b); err != nil {
		return d.rollback(fmt.Errorf("storage: appending to segment: %w", err))
	}
	if err := d.seg.Sync(); err != nil {
		return d.rollback(fmt.Errorf("storage: syncing segment: %w", err))
	}
	d.segSize += int64(len(b))
	return nil
}

func (d *Disk) rollback(cause error) error {
	err := d.seg.Truncate(d.segSize)
	if err == nil {
		_, err = d.seg.Seek(d.segSize, io.SeekStart)
	}
	if err != nil {
		// The segment may now end in bytes no client was told about.
		// Appending after them would put them in the middle of the file,
		// where replay reads them as a torn tail and discards every good
		// record written afterwards. Close the segment and remember where
		// its acknowledged data ended, so the next write can cut it back.
		_ = d.seg.Close()
		d.dirty, d.dirtyHour, d.dirtySize = true, d.segHour, d.segSize
		d.seg = nil
		d.segHour = -1
		return errors.Join(cause, fmt.Errorf("storage: rolling back failed append: %w", err))
	}
	return cause
}

// roll makes sure the open segment is the one for the current hour.
func (d *Disk) roll() error {
	hour := d.now().Unix() / 3600
	if d.seg != nil && hour == d.segHour {
		return nil
	}
	if d.seg != nil {
		if err := d.seg.Close(); err != nil {
			return fmt.Errorf("storage: closing segment: %w", err)
		}
		d.seg = nil
	}

	if d.dirty {
		if err := d.repair(); err != nil {
			return err
		}
	}

	path := d.path(hour)
	f, err := d.open(path)
	if err != nil {
		return fmt.Errorf("storage: opening %s: %w", path, err)
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return fmt.Errorf("storage: seeking %s: %w", path, err)
	}
	d.seg, d.segHour, d.segSize = f, hour, size
	return nil
}

// repair cuts the dirty segment back to the end of its last acknowledged
// record.
//
// It truncates to a remembered size rather than scanning for records that
// decode cleanly. A sync can fail after every byte of a record is in the file,
// leaving a record that checks out perfectly and was still reported to the
// client as a failure. Keeping it would bring that span back after a restart.
// Only the size recorded at the last successful append says what was actually
// acknowledged.
//
// This repairs the segment that failed, not the current hour's, so an hour
// rolling over between the failure and the next write does not leave a dirty
// file sealed behind it. If the truncate fails again, the write fails and the
// segment stays dirty for the next attempt.
func (d *Disk) repair() error {
	path := d.path(d.dirtyHour)
	if err := os.Truncate(path, d.dirtySize); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storage: repairing %s: %w", path, err)
	}
	d.dirty = false
	return nil
}

// Accept lets the store sit directly behind the ingest path.
func (d *Disk) Accept(ctx context.Context, spans []trace.Span) error { return d.Write(ctx, spans) }

// Trace returns every span of one trace, ordered by start time.
func (d *Disk) Trace(ctx context.Context, id trace.TraceID) ([]trace.Span, error) {
	return d.mem.Trace(ctx, id)
}

// Find returns summaries of matching traces, newest first.
func (d *Disk) Find(ctx context.Context, q Query) ([]Summary, error) {
	return d.mem.Find(ctx, q)
}

// EvictExpired drops expired traces from memory and deletes every segment
// written entirely before the retention window. It reports how many traces
// were dropped.
//
// Segments are judged by write time and traces by end time, and the two can
// disagree for a trace that runs longer than an hour: its early spans can sit
// in a segment old enough to delete while the trace itself is still kept.
// Such a trace stays whole until the next restart and comes back without its
// earliest spans after one. Tracking per-trace segment membership would fix
// that at the cost of an index nobody else needs; a trace running for hours
// is rare enough that I would rather write the limitation down.
func (d *Disk) EvictExpired() int {
	evicted := d.mem.EvictExpired()
	if d.retention <= 0 {
		return evicted
	}

	cutoffHour := d.now().Add(-d.retention).Unix() / 3600

	d.mu.Lock()
	defer d.mu.Unlock()

	hours, err := d.segments()
	if err != nil {
		return evicted
	}
	for _, hour := range hours {
		// A segment for hour h holds writes up to the end of h, so it is
		// wholly expired only once the cutoff has passed h+1.
		if hour+1 > cutoffHour || (d.seg != nil && hour == d.segHour) {
			continue
		}
		_ = os.Remove(d.path(hour))
	}
	return evicted
}

// Len reports how many traces are stored.
func (d *Disk) Len() int { return d.mem.Len() }

// Close syncs and closes the open segment. Every later call fails with
// ErrClosed.
func (d *Disk) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}
	d.closed = true

	var err error
	if d.seg != nil {
		err = errors.Join(d.seg.Sync(), d.seg.Close())
		d.seg = nil
	}
	return errors.Join(err, d.mem.Close())
}
