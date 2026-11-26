package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func appendFile(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func line(id, status string) string {
	return `{"ts":"2026-11-26T10:00:00Z","service":"finance-worker","instance":"W1","scenario":"work","event_id":"` +
		id + `","status":"` + status + `"}` + "\n"
}

func TestTailerReadsOnlyNewCompleteLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "finance-worker-W1.jsonl")
	tl := newTailer(dir)

	appendFile(t, path, line("ev-aaaa-001", "received"))
	// Dòng thứ hai đang ghi dở: chưa được trả về.
	second := line("ev-aaaa-001", "processed")
	appendFile(t, path, second[:20])

	recs, reset, err := tl.poll()
	if err != nil || reset || len(recs) != 1 || recs[0].Status != "received" {
		t.Fatalf("lần 1: recs=%v reset=%v err=%v", recs, reset, err)
	}

	appendFile(t, path, second[20:])
	recs, reset, err = tl.poll()
	if err != nil || reset || len(recs) != 1 || recs[0].Status != "processed" {
		t.Fatalf("lần 2: recs=%v reset=%v err=%v", recs, reset, err)
	}

	recs, _, _ = tl.poll()
	if len(recs) != 0 {
		t.Fatalf("không có gì mới nhưng vẫn trả về %d bản ghi", len(recs))
	}
}

func TestTailerResetsWhenLogsDeleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "academic.jsonl")
	tl := newTailer(dir)
	appendFile(t, path, line("ev-aaaa-001", "published")+line("ev-aaaa-002", "published"))
	if recs, _, _ := tl.poll(); len(recs) != 2 {
		t.Fatalf("đọc lần đầu được %d bản ghi", len(recs))
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, line("ev-bbbb-001", "published"))
	recs, reset, err := tl.poll()
	if err != nil || !reset || len(recs) != 1 || !strings.HasPrefix(recs[0].EventID, "ev-bbbb") {
		t.Fatalf("sau khi xóa log: recs=%v reset=%v err=%v", recs, reset, err)
	}
}
