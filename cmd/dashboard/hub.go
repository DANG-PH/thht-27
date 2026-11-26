package main

import (
	"bytes"
	"encoding/json"
	"sync"

	"nmtht/chude07/internal/evidence"
)

// maxHistory: số bản ghi gần nhất gửi cho trình duyệt khi vừa mở trang.
const maxHistory = 3000

// hub giữ lịch sử bản ghi và danh sách trình duyệt đang kết nối, phát mỗi bản ghi mới tới tất cả.
type hub struct {
	mu      sync.Mutex
	records []evidence.Record
	queues  []byte // JSON số liệu queue gần nhất
	clients map[chan []byte]struct{}
}

func newHub() *hub {
	return &hub{clients: map[chan []byte]struct{}{}}
}

func (h *hub) publishRecords(rs []evidence.Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, rs...)
	if over := len(h.records) - maxHistory; over > 0 {
		h.records = append([]evidence.Record(nil), h.records[over:]...)
	}
	for _, r := range rs {
		b, err := json.Marshal(r)
		if err == nil {
			h.broadcastLocked(sseFrame("record", b))
		}
	}
}

// resetRecords: thư mục logs/ bị xóa để chạy lại từ đầu → trình duyệt cũng xóa dữ liệu cũ.
func (h *hub) resetRecords() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = nil
	h.broadcastLocked(sseFrame("reset", []byte("{}")))
}

func (h *hub) publishQueues(payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if bytes.Equal(payload, h.queues) {
		return // không đổi thì không gửi lại
	}
	h.queues = payload
	h.broadcastLocked(sseFrame("queues", payload))
}

func (h *hub) currentQueues() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.queues
}

// subscribe đăng ký một trình duyệt mới. Ảnh chụp lịch sử và việc đăng ký nằm trong cùng
// một lần khóa, nên không bản ghi nào lọt giữa "lịch sử" và "bản ghi mới".
func (h *hub) subscribe() (ch chan []byte, initial [][]byte, cancel func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	records := h.records
	if records == nil {
		records = []evidence.Record{}
	}
	snap, _ := json.Marshal(records)
	initial = [][]byte{sseFrame("snapshot", snap)}
	if h.queues != nil {
		initial = append(initial, sseFrame("queues", h.queues))
	}

	ch = make(chan []byte, 512)
	h.clients[ch] = struct{}{}
	cancel = func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.clients, ch)
	}
	return ch, initial, cancel
}

// broadcastLocked gửi không chặn: trình duyệt nào đọc chậm (bộ đệm đầy) thì bỏ khung đó,
// để một trình duyệt chậm không làm treo cả dashboard.
func (h *hub) broadcastLocked(frame []byte) {
	for c := range h.clients {
		select {
		case c <- frame:
		default:
		}
	}
}

// sseFrame đóng gói một sự kiện theo định dạng Server-Sent Events.
// JSON từ json.Marshal không chứa ký tự xuống dòng nên nằm gọn trên một dòng "data:".
func sseFrame(event string, data []byte) []byte {
	var b bytes.Buffer
	b.WriteString("event: ")
	b.WriteString(event)
	b.WriteString("\ndata: ")
	b.Write(data)
	b.WriteString("\n\n")
	return b.Bytes()
}
