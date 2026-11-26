// Package academic chứa nghiệp vụ phát sự kiện của Academic (Producer).
//
// Dùng chung cho hai cách chạy: dòng lệnh (gửi một lần rồi thoát) và dịch vụ HTTP (trang web bấm nút gửi).
// Academic chỉ biết NƠI GỬI (queue finance.work hoặc exchange academic.events),
// không biết phía sau có bao nhiêu worker hay subscriber.
package academic

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"nmtht/chude07/internal/event"
	"nmtht/chude07/internal/evidence"
	"nmtht/chude07/internal/mq"
)

// MaxPerRequest giới hạn số sự kiện mỗi lần gửi, tránh bấm nhầm gửi quá nhiều từ trang web.
const MaxPerRequest = 200

// ErrInvalidRequest: lỗi do yêu cầu sai (mode lạ, số lượng quá lớn), không phải lỗi broker.
var ErrInvalidRequest = errors.New("yêu cầu không hợp lệ")

// Enrollment là một dòng dữ liệu giả lập trong data/enrollments.csv.
type Enrollment struct {
	StudentID, FullName, CourseID, CourseName, Semester string
	Credits                                             int
}

// Request mô tả một lần gửi.
type Request struct {
	Mode     string        `json:"mode"` // "work" = kịch bản (i), "pubsub" = kịch bản (ii)
	N        int           `json:"n"`    // số sự kiện; 0 = gửi hết dữ liệu giả lập
	Bad      bool          `json:"bad"`  // gửi thêm 1 thông điệp sai hợp đồng
	Interval time.Duration `json:"-"`    // nghỉ giữa 2 lần gửi (chỉ dùng ở dòng lệnh)
}

// Result trả về mã lần gửi và các event ID đã được broker xác nhận.
type Result struct {
	Run      string   `json:"run"`
	Mode     string   `json:"mode"`
	Target   string   `json:"target"`
	EventIDs []string `json:"event_ids"`
}

// Sender phát sự kiện qua một channel AMQP có bật publisher confirm.
type Sender struct {
	mu    sync.Mutex // gửi tuần tự trên một channel, kể cả khi nhiều yêu cầu HTTP tới cùng lúc
	pub   *mq.Publisher
	evlog *evidence.Log
	rows  []Enrollment
	next  int // dòng dữ liệu tiếp theo: mỗi lần gửi lấy các sinh viên kế tiếp
}

func NewSender(pub *mq.Publisher, evlog *evidence.Log, rows []Enrollment) (*Sender, error) {
	if len(rows) == 0 {
		return nil, errors.New("không có dữ liệu giả lập")
	}
	return &Sender{pub: pub, evlog: evlog, rows: rows}, nil
}

// Send phát req.N sự kiện student.enrolled theo kịch bản req.Mode.
// Nếu lỗi giữa chừng, Result vẫn chứa các sự kiện đã gửi thành công trước đó.
func (s *Sender) Send(ctx context.Context, req Request) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var exchange, routingKey, target string
	var err error
	switch req.Mode {
	case evidence.ScenarioWork:
		// Default exchange: routing key chính là tên queue đích.
		exchange, routingKey, target = "", mq.WorkQueue, "queue "+mq.WorkQueue
		// Khai báo queue trước khi gửi: worker chưa bật thì thông điệp vẫn nằm chờ trong queue.
		err = mq.DeclareQueue(s.pub.Channel(), mq.WorkQueue)
	case evidence.ScenarioPubSub:
		exchange, routingKey = mq.EventsExchange, event.TypeStudentEnrolled
		target = "exchange " + mq.EventsExchange + " (routing key " + routingKey + ")"
		// Chỉ khai báo exchange. Queue của từng subscriber do subscriber tự tạo.
		err = mq.DeclareEventsExchange(s.pub.Channel())
	default:
		return Result{}, fmt.Errorf("%w: mode phải là %q hoặc %q", ErrInvalidRequest, evidence.ScenarioWork, evidence.ScenarioPubSub)
	}
	if err != nil {
		return Result{}, err
	}

	count := req.N
	if count <= 0 {
		count = len(s.rows)
	}
	if count > MaxPerRequest {
		return Result{}, fmt.Errorf("%w: mỗi lần gửi tối đa %d sự kiện", ErrInvalidRequest, MaxPerRequest)
	}

	res := Result{Run: newRunID(), Mode: req.Mode, Target: target, EventIDs: []string{}}
	log.Printf("lần gửi %s: %d sự kiện → %s", res.Run, count, target)

	for i := 0; i < count; i++ {
		row := s.rows[s.next%len(s.rows)]
		s.next++
		ev := event.StudentEnrolled{
			EventID:    fmt.Sprintf("ev-%s-%03d", res.Run, i+1),
			EventType:  event.TypeStudentEnrolled,
			OccurredAt: time.Now().UTC(),
			StudentID:  row.StudentID,
			FullName:   row.FullName,
			CourseID:   row.CourseID,
			CourseName: row.CourseName,
			Credits:    row.Credits,
			Semester:   row.Semester,
		}
		body, err := json.Marshal(ev)
		if err != nil {
			return res, err
		}
		rec := evidence.Record{EventID: ev.EventID, StudentID: ev.StudentID, CourseID: ev.CourseID}
		if err := s.publish(ctx, req.Mode, exchange, routingKey, body, rec); err != nil {
			return res, err
		}
		res.EventIDs = append(res.EventIDs, ev.EventID)
		log.Printf("↑ %s  SV %s  %s (%d TC) – broker đã xác nhận", ev.EventID, ev.StudentID, ev.CourseID, ev.Credits)

		if req.Interval > 0 && i < count-1 {
			select {
			case <-ctx.Done():
				return res, ctx.Err()
			case <-time.After(req.Interval):
			}
		}
	}

	if req.Bad {
		id := fmt.Sprintf("ev-%s-bad", res.Run)
		// Thiếu student_id, course_id và số tín chỉ âm: vi phạm hợp đồng thông điệp.
		body := []byte(fmt.Sprintf(`{"event_id":%q,"event_type":%q,"credits":-1}`, id, event.TypeStudentEnrolled))
		if err := s.publish(ctx, req.Mode, exchange, routingKey, body, evidence.Record{EventID: id, Note: evidence.NoteMalformed}); err != nil {
			return res, err
		}
		res.EventIDs = append(res.EventIDs, id)
		log.Printf("↑ %s  (thông điệp SAI hợp đồng – để kiểm thử tình huống lỗi)", id)
	}
	return res, nil
}

// publish gửi một thông điệp, chờ broker xác nhận, rồi mới ghi bản ghi "published".
func (s *Sender) publish(ctx context.Context, mode, exchange, routingKey string, body []byte, rec evidence.Record) error {
	if err := s.pub.Publish(ctx, exchange, routingKey, event.TypeStudentEnrolled, rec.EventID, body); err != nil {
		return fmt.Errorf("gửi %s thất bại: %w", rec.EventID, err)
	}
	rec.Service, rec.Instance, rec.Scenario, rec.Status = "academic", "academic", mode, evidence.Published
	s.evlog.Write(rec)
	return nil
}

// LoadEnrollments đọc file CSV dữ liệu giả lập.
func LoadEnrollments(path string) ([]Enrollment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mở dữ liệu giả lập (chạy lệnh từ thư mục gốc dự án): %w", err)
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("đọc %s: %w", path, err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("%s không có dữ liệu", path)
	}
	var out []Enrollment
	for i, r := range rows[1:] { // bỏ dòng tiêu đề
		if len(r) != 6 {
			return nil, fmt.Errorf("%s dòng %d: cần 6 cột, có %d", path, i+2, len(r))
		}
		credits, err := strconv.Atoi(strings.TrimSpace(r[4]))
		if err != nil {
			return nil, fmt.Errorf("%s dòng %d: credits không phải số: %w", path, i+2, err)
		}
		out = append(out, Enrollment{
			StudentID: r[0], FullName: r[1], CourseID: r[2], CourseName: r[3], Credits: credits, Semester: r[5],
		})
	}
	return out, nil
}

// newRunID tạo mã 4 ký tự hex cho mỗi lần gửi, ghép vào event ID: ev-<run>-<số thứ tự>.
func newRunID() string {
	b := make([]byte, 2)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().Unix()%0xffff, 16)
	}
	return hex.EncodeToString(b)
}
