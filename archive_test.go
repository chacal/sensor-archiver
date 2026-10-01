package main

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

var (
	day1 = time.Date(2026, 10, 1, 23, 59, 59, 123_000_000, time.UTC)
	day2 = time.Date(2026, 10, 2, 0, 0, 1, 0, time.UTC)
)

func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func rec(at time.Time, payload string) Record {
	return Record{ReceivedAt: at, Topic: "/sensor/A/t/state", Payload: []byte(payload)}
}

func mustOpen(t *testing.T, dir string, now time.Time) *Archive {
	t.Helper()
	a, err := OpenArchive(dir, now, testLog())
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustAppend(t *testing.T, a *Archive, rs ...Record) {
	t.Helper()
	for _, r := range rs {
		if err := a.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}
}

func readZst(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	b, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(dir, path)
			files = append(files, rel)
		}
		return err
	})
	slices.Sort(files)
	return files
}

func TestEncodeRecord(t *testing.T) {
	cases := []struct {
		name string
		r    Record
		want string
	}{
		{"json payload is embedded and compacted",
			Record{ReceivedAt: day1, Topic: "/sensor/A/t/state", Payload: []byte("{\n \"a\": \"<b>\" }")},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"/sensor/A/t/state","payload":{"a":"<b>"}}`},
		{"retained flag",
			Record{ReceivedAt: day1, Topic: "t", Payload: []byte("1"), Retained: true},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"t","retained":true,"payload":1}`},
		{"text payload is a string",
			Record{ReceivedAt: day1, Topic: "t", Payload: []byte("not json\n")},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"t","payload":"not json\n"}`},
		{"empty payload is an empty string",
			Record{ReceivedAt: day1, Topic: "t", Payload: nil},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"t","payload":""}`},
		{"binary payload is base64",
			Record{ReceivedAt: day1, Topic: "t", Payload: []byte{0xff, 0x00}},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"t","payload_base64":"/wA="}`},
		{"local time is stored as UTC",
			Record{ReceivedAt: day1.In(time.FixedZone("EEST", 3*3600)), Topic: "t", Payload: []byte("1")},
			`{"received_at":"2026-10-01T23:59:59.123Z","topic":"t","payload":1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			encodeRecord(&buf, c.r)
			if got := buf.String(); got != c.want+"\n" {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}

func TestRolloverCompressesPreviousDay(t *testing.T) {
	dir := t.TempDir()
	a := mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day1, `{"n":1}`), rec(day1, `{"n":2}`), rec(day2, `{"n":3}`))
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := listFiles(t, dir), []string{"2026/2026-10-01.jsonl.zst", "2026/2026-10-02.jsonl"}; !slices.Equal(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
	if got := readZst(t, filepath.Join(dir, "2026/2026-10-01.jsonl.zst")); strings.Count(got, "\n") != 2 || !strings.Contains(got, `{"n":2}`) {
		t.Errorf("day 1 content: %q", got)
	}
}

func TestClockGoingBackwardsKeepsWritingNewestFile(t *testing.T) {
	dir := t.TempDir()
	a := mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day2, `{"n":1}`), rec(day1, `{"n":2}`))
	a.Close()

	if got, want := listFiles(t, dir), []string{"2026/2026-10-02.jsonl"}; !slices.Equal(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
}

func TestRestartSameDayAppendsAndDropsPartialLine(t *testing.T) {
	dir := t.TempDir()
	a := mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day1, `{"n":1}`))
	a.Close()

	// Simulate a crash in the middle of writing a line
	raw := filepath.Join(dir, "2026/2026-10-01.jsonl")
	f, _ := os.OpenFile(raw, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`{"received_at":"2026-10-01T`)
	f.Close()

	a = mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day1, `{"n":2}`))
	a.Close()

	b, _ := os.ReadFile(raw)
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `{"n":1}`) || !strings.Contains(lines[1], `{"n":2}`) {
		t.Errorf("content: %q", b)
	}
}

func TestStartupCompressesPastDaysAndCleansTempFiles(t *testing.T) {
	dir := t.TempDir()
	a := mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day1, `{"n":1}`))
	a.Close()
	os.WriteFile(filepath.Join(dir, "2026/2026-09-30.jsonl.zst.tmp"), []byte("junk"), 0o644)

	a = mustOpen(t, dir, day2)
	a.Close()

	if got, want := listFiles(t, dir), []string{"2026/2026-10-01.jsonl.zst"}; !slices.Equal(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
}

func TestCompressionNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "2026/2026-10-01.jsonl.zst")
	os.MkdirAll(filepath.Dir(existing), 0o755)
	os.WriteFile(existing, []byte("old"), 0o644)

	a := mustOpen(t, dir, day1)
	mustAppend(t, a, rec(day1, `{"n":1}`), rec(day2, `{"n":2}`))
	a.Close()

	if b, _ := os.ReadFile(existing); string(b) != "old" {
		t.Errorf("existing file overwritten")
	}
	if got := readZst(t, filepath.Join(dir, "2026/2026-10-01.1.jsonl.zst")); !strings.Contains(got, `{"n":1}`) {
		t.Errorf("numbered file content: %q", got)
	}
}

func TestEndOfLastLine(t *testing.T) {
	big := strings.Repeat("x", 200_000)
	cases := map[string]int64{
		"":                    0,
		"abc":                 0,
		"a\n":                 2,
		"a\nb":                2,
		"a\n" + big:           2,
		big + "\n" + big:      200_001,
		"a\nb\n" + big + "\n": int64(5 + len(big)),
	}
	for content, want := range cases {
		f, _ := os.CreateTemp(t.TempDir(), "")
		f.WriteString(content)
		got, err := endOfLastLine(f)
		f.Close()
		if err != nil || got != want {
			t.Errorf("len %d: got %d, %v; want %d", len(content), got, err, want)
		}
	}
}
