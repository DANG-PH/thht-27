// Package evidence ghi nhật ký bằng chứng dạng JSON Lines (mỗi dòng một bản ghi) vào thư mục logs/.
//
// Mỗi tiến trình ghi một file riêng. cmd/verify đọc lại toàn bộ để đối chiếu:
// sự kiện nào đã gửi, ai nhận, ai xử lý xong, có bị giao lại hay không.
package evidence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Status string

const (
	Published    Status = "published"     // Academic gửi thành công (broker đã xác nhận)
	Received     Status = "received"      // consumer nhận, bắt đầu xử lý
	Processed    Status = "processed"     // xử lý xong và đã ack
	Rejected     Status = "rejected"      // sai hợp đồng → dead-letter queue
	Requeued     Status = "requeued"      // lỗi xử lý lần đầu → trả lại queue để thử lại
	DeadLettered Status = "dead-lettered" // lỗi xử lý lặp lại → dead-letter queue
)

const (
	ScenarioWork   = "work"   // kịch bản (i): một queue, nhiều worker
	ScenarioPubSub = "pubsub" // kịch bản (ii): mỗi subscriber một queue

	NoteMalformed = "malformed" // Academic đánh dấu thông điệp cố ý gửi sai hợp đồng
)

type Record struct {
	Time        time.Time `json:"ts"`
	Service     string    `json:"service"`  // ứng dụng: academic, finance-worker, finance, ...
	Instance    string    `json:"instance"` // tiến trình: W1, W2, ... (bằng Service nếu chỉ chạy một)
	Scenario    string    `json:"scenario"`
	Queue       string    `json:"queue,omitempty"`
	EventID     string    `json:"event_id"`
	Status      Status    `json:"status"`
	Redelivered bool      `json:"redelivered,omitempty"` // RabbitMQ giao lại (lần trước chưa được ack)
	Note        string    `json:"note,omitempty"`
}

// Dir là thư mục chứa log, đổi bằng biến môi trường LOG_DIR.
func Dir() string {
	if d := os.Getenv("LOG_DIR"); d != "" {
		return d
	}
	return "logs"
}

type Log struct {
	mu sync.Mutex
	f  *os.File
}

// Open mở (hoặc tạo) file logs/<name>.jsonl ở chế độ ghi nối tiếp.
func Open(name string) (*Log, error) {
	if err := os.MkdirAll(Dir(), 0o755); err != nil {
		return nil, fmt.Errorf("tạo thư mục log: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(Dir(), name+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("mở file log: %w", err)
	}
	return &Log{f: f}, nil
}

// Write ghi thẳng xuống file (os.File không có bộ đệm), nên kể cả khi tiến trình
// chết ngay sau đó – như trong test "worker chết giữa chừng" – bản ghi vẫn còn.
func (l *Log) Write(r Record) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.f.Write(append(b, '\n'))
}

func (l *Log) Close() error { return l.f.Close() }

// ReadAll đọc mọi file *.jsonl trong dir, sắp xếp theo thời gian.
func ReadAll(dir string) ([]Record, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, path := range files {
		recs, err := readFile(path)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}

func readFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			out = append(out, r)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("đọc %s: %w", path, err)
	}
	return out, nil
}
