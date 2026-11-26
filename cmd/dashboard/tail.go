package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"nmtht/chude07/internal/evidence"
)

// tailer đọc phần mới được ghi thêm vào các file logs/*.jsonl (giống lệnh `tail -f`).
type tailer struct {
	dir     string
	offsets map[string]int64  // đã đọc tới byte thứ mấy của từng file
	partial map[string][]byte // dòng đang ghi dở (chưa có ký tự xuống dòng)
}

func newTailer(dir string) *tailer {
	return &tailer{dir: dir, offsets: map[string]int64{}, partial: map[string][]byte{}}
}

// poll trả về các bản ghi mới, sắp theo thời gian.
// reset = true khi có file bị xóa hoặc ngắn đi (người dùng xóa logs/ để chạy lại từ đầu);
// khi đó mọi file được đọc lại từ đầu.
func (t *tailer) poll() (recs []evidence.Record, reset bool, err error) {
	files, err := filepath.Glob(filepath.Join(t.dir, "*.jsonl"))
	if err != nil {
		return nil, false, err
	}
	sizes := make(map[string]int64, len(files))
	for _, p := range files {
		if fi, err := os.Stat(p); err == nil {
			sizes[p] = fi.Size()
		}
	}
	for p, off := range t.offsets {
		if size, ok := sizes[p]; !ok || size < off {
			reset = true
			break
		}
	}
	if reset {
		t.offsets = map[string]int64{}
		t.partial = map[string][]byte{}
	}

	for p, size := range sizes {
		rs, readErr := t.readNew(p, size)
		if readErr != nil {
			err = readErr
			continue
		}
		recs = append(recs, rs...)
	}
	sort.SliceStable(recs, func(i, j int) bool { return recs[i].Time.Before(recs[j].Time) })
	return recs, reset, err
}

func (t *tailer) readNew(path string, size int64) ([]evidence.Record, error) {
	off := t.offsets[path]
	if size <= off {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data := make([]byte, size-off)
	n, err := f.ReadAt(data, off)
	if err != nil && err != io.EOF {
		return nil, err
	}
	t.offsets[path] = off + int64(n)

	buf := append(t.partial[path], data[:n]...)
	lastNL := bytes.LastIndexByte(buf, '\n')
	if lastNL < 0 {
		t.partial[path] = buf
		return nil, nil
	}
	t.partial[path] = append([]byte(nil), buf[lastNL+1:]...)

	var out []evidence.Record
	for _, line := range bytes.Split(buf[:lastNL], []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r evidence.Record
		if json.Unmarshal(line, &r) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

func runTailer(ctx context.Context, dir string, h *hub) {
	t := newTailer(dir)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for {
		recs, reset, err := t.poll()
		if err != nil {
			log.Printf("đọc log: %v", err)
		}
		if reset {
			h.resetRecords()
		}
		if len(recs) > 0 {
			h.publishRecords(recs)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
