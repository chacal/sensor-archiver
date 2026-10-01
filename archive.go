package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	dayLayout  = "2006-01-02"
	timeLayout = "2006-01-02T15:04:05.000Z07:00"
	rawExt     = ".jsonl"
	zstExt     = ".jsonl.zst"
	tmpExt     = ".tmp"
)

// Record is one received MQTT message.
type Record struct {
	ReceivedAt time.Time
	Topic      string
	Payload    []byte
	Retained   bool
}

// Archive appends records to an uncompressed JSON Lines file per UTC day
// (<dir>/YYYY/YYYY-MM-DD.jsonl). When the day changes, the previous file is sealed and
// compressed in the background to YYYY-MM-DD.jsonl.zst.
//
// Records are durable only after Sync returns. A sealed file is never reopened: if the
// clock goes backwards, records keep going to the newest file, so files may contain a
// few records whose received_at belongs to a neighbouring day.
type Archive struct {
	dir string
	log *slog.Logger

	day string // UTC day of the open file, "" if none open
	f   *os.File
	w   *bufio.Writer
	buf bytes.Buffer

	toCompress chan string
	compressWg sync.WaitGroup
}

// OpenArchive recovers state left by a previous run: leftover temp files are removed,
// raw files of past days are queued for compression and the newest raw file, if it is
// from today or later, is reopened for appending.
func OpenArchive(dir string, now time.Time, log *slog.Logger) (*Archive, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	a := &Archive{dir: dir, log: log, toCompress: make(chan string, 1000)}

	var raws []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		switch {
		case strings.HasSuffix(path, tmpExt):
			log.Info("removing leftover temp file", "path", path)
			return os.Remove(path)
		case strings.HasSuffix(path, rawExt):
			raws = append(raws, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(raws) // Names sort chronologically

	today := now.UTC().Format(dayLayout)
	if n := len(raws); n > 0 {
		if day := dayOf(raws[n-1]); day >= today {
			if err := a.open(day); err != nil {
				return nil, err
			}
			raws = raws[:n-1]
		}
	}

	a.compressWg.Add(1)
	go a.compressLoop()
	for _, raw := range raws {
		a.toCompress <- raw
	}
	return a, nil
}

// Append buffers a record. Call Sync to make it durable.
func (a *Archive) Append(r Record) error {
	if day := r.ReceivedAt.UTC().Format(dayLayout); day > a.day {
		if err := a.seal(); err != nil {
			return err
		}
		if err := a.open(day); err != nil {
			return err
		}
	}
	a.buf.Reset()
	encodeRecord(&a.buf, r)
	_, err := a.w.Write(a.buf.Bytes())
	return err
}

// Sync flushes buffered records and fsyncs the open file.
func (a *Archive) Sync() error {
	if a.f == nil {
		return nil
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	return a.f.Sync()
}

// Close syncs and closes the open file (it is left uncompressed and reopened on next
// start) and waits for queued compressions to finish.
func (a *Archive) Close() error {
	err := a.Sync()
	if a.f != nil {
		err = errors.Join(err, a.f.Close())
		a.f = nil
	}
	close(a.toCompress)
	a.compressWg.Wait()
	return err
}

func (a *Archive) rawPath(day string) string {
	return filepath.Join(a.dir, day[:4], day+rawExt)
}

func (a *Archive) open(day string) error {
	path := a.rawPath(day)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	// A crash may have left a partially written last line; drop it
	end, err := endOfLastLine(f)
	if err == nil {
		err = f.Truncate(end)
	}
	if err == nil {
		_, err = f.Seek(end, io.SeekStart)
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	if err != nil {
		f.Close()
		return fmt.Errorf("open %s: %w", path, err)
	}
	a.log.Info("archiving to", "path", path)
	a.day, a.f, a.w = day, f, bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (a *Archive) seal() error {
	if a.f == nil {
		return nil
	}
	path := a.f.Name()
	if err := a.Sync(); err != nil {
		return err
	}
	if err := a.f.Close(); err != nil {
		return err
	}
	a.f, a.w = nil, nil
	a.toCompress <- path
	return nil
}

func (a *Archive) compressLoop() {
	defer a.compressWg.Done()
	for raw := range a.toCompress {
		start := time.Now()
		dst, err := compressFile(raw)
		if err != nil {
			// The raw file is kept and retried on next start
			a.log.Error("compression failed", "path", raw, "err", err)
			continue
		}
		a.log.Info("compressed", "path", dst, "duration", time.Since(start).Round(time.Millisecond))
	}
}

// compressFile writes raw as zstd next to it and removes raw. If the target already
// exists (e.g. a crash between rename and remove), a numbered name is used instead:
// a duplicated day is better than a lost one.
func compressFile(raw string) (string, error) {
	base := strings.TrimSuffix(raw, rawExt)
	dst := base + zstExt
	for i := 1; fileExists(dst); i++ {
		dst = fmt.Sprintf("%s.%d%s", base, i, zstExt)
	}
	tmp := dst + tmpExt

	in, err := os.Open(raw)
	if err != nil {
		return "", err
	}
	defer in.Close()
	out, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp) // No-op after a successful rename

	enc, err := zstd.NewWriter(out,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1))
	if err == nil {
		_, err = io.Copy(enc, in)
		err = errors.Join(err, enc.Close())
	}
	if err == nil {
		err = out.Sync()
	}
	err = errors.Join(err, out.Close())
	if err == nil {
		err = os.Rename(tmp, dst)
	}
	if err == nil {
		err = syncDir(filepath.Dir(dst))
	}
	if err == nil {
		err = os.Remove(raw)
	}
	if err != nil {
		return "", err
	}
	return dst, syncDir(filepath.Dir(dst))
}

type jsonRecord struct {
	ReceivedAt    string          `json:"received_at"`
	Topic         string          `json:"topic"`
	Retained      bool            `json:"retained,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	PayloadBase64 string          `json:"payload_base64,omitempty"`
}

// encodeRecord writes r as one JSON line. A JSON payload is embedded as is (compacted),
// other UTF-8 text as a string and anything else as base64.
func encodeRecord(buf *bytes.Buffer, r Record) {
	jr := jsonRecord{
		ReceivedAt: r.ReceivedAt.UTC().Format(timeLayout),
		Topic:      r.Topic,
		Retained:   r.Retained,
	}
	var compact bytes.Buffer
	switch {
	case json.Valid(r.Payload) && json.Compact(&compact, r.Payload) == nil:
		jr.Payload = compact.Bytes()
	case utf8.Valid(r.Payload):
		jr.Payload, _ = json.Marshal(string(r.Payload))
	default:
		jr.PayloadBase64 = base64.StdEncoding.EncodeToString(r.Payload)
	}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false) // Keep payload text as published
	// Can't fail: all fields are strings, bools or valid JSON
	_ = enc.Encode(jr) // Encode appends the newline
}

// endOfLastLine returns the offset just past the last '\n' in f, or 0 if there is none.
func endOfLastLine(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	chunk := make([]byte, 64*1024)
	for end := info.Size(); end > 0; {
		start := max(end-int64(len(chunk)), 0)
		n, err := f.ReadAt(chunk[:end-start], start)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if i := bytes.LastIndexByte(chunk[:n], '\n'); i >= 0 {
			return start + int64(i) + 1, nil
		}
		end = start
	}
	return 0, nil
}

func dayOf(rawPath string) string {
	return strings.TrimSuffix(filepath.Base(rawPath), rawExt)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
