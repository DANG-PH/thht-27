// Academic – ứng dụng phát sự kiện student.enrolled (Producer).
//
// Academic chỉ biết NƠI GỬI (queue finance.work hoặc exchange academic.events),
// không biết phía sau có bao nhiêu worker hay subscriber.
package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"nmtht/chude07/internal/event"
	"nmtht/chude07/internal/evidence"
	"nmtht/chude07/internal/mq"
)

type enrollment struct {
	StudentID, FullName, CourseID, CourseName, Semester string
	Credits                                             int
}

func main() {
	mode := flag.String("mode", evidence.ScenarioPubSub,
		"work = kịch bản (i): gửi vào queue finance.work; pubsub = kịch bản (ii): publish lên exchange academic.events")
	n := flag.Int("n", 0, "số sự kiện cần gửi (0 = gửi hết dữ liệu giả lập)")
	bad := flag.Bool("bad", false, "gửi thêm 1 thông điệp sai hợp đồng để thử tình huống lỗi")
	dataPath := flag.String("data", "data/enrollments.csv", "file CSV dữ liệu đăng ký giả lập")
	interval := flag.Duration("interval", 0, "nghỉ giữa 2 lần gửi, vd 300ms")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("[academic] ")

	var exchange, routingKey string
	switch *mode {
	case evidence.ScenarioWork:
		exchange, routingKey = "", mq.WorkQueue // default exchange: routing key chính là tên queue đích
	case evidence.ScenarioPubSub:
		exchange, routingKey = mq.EventsExchange, event.TypeStudentEnrolled
	default:
		log.Fatalf("-mode phải là %q hoặc %q", evidence.ScenarioWork, evidence.ScenarioPubSub)
	}

	rows, err := loadEnrollments(*dataPath)
	if err != nil {
		log.Fatal(err)
	}
	count := *n
	if count <= 0 {
		count = len(rows)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := mq.Dial(ctx, mq.URL())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		log.Fatal(err)
	}
	if *mode == evidence.ScenarioWork {
		// Khai báo queue trước khi gửi: worker chưa bật thì thông điệp vẫn nằm chờ trong queue.
		err = mq.DeclareQueue(pub.Channel(), mq.WorkQueue)
	} else {
		// Chỉ khai báo exchange. Queue của từng subscriber do subscriber tự tạo.
		err = mq.DeclareEventsExchange(pub.Channel())
	}
	if err != nil {
		log.Fatal(err)
	}

	evlog, err := evidence.Open("academic")
	if err != nil {
		log.Fatal(err)
	}
	defer evlog.Close()

	run := newRunID()
	target := "queue " + routingKey
	if exchange != "" {
		target = "exchange " + exchange + " (routing key " + routingKey + ")"
	}
	log.Printf("lần gửi %s: %d sự kiện → %s", run, count, target)

	send := func(id string, body []byte, note string) {
		if err := pub.Publish(ctx, exchange, routingKey, event.TypeStudentEnrolled, id, body); err != nil {
			log.Fatalf("gửi %s thất bại: %v", id, err)
		}
		evlog.Write(evidence.Record{
			Service: "academic", Instance: "academic", Scenario: *mode,
			EventID: id, Status: evidence.Published, Note: note,
		})
	}

	for i := 0; i < count; i++ {
		row := rows[i%len(rows)]
		ev := event.StudentEnrolled{
			EventID:    fmt.Sprintf("ev-%s-%03d", run, i+1),
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
			log.Fatal(err)
		}
		send(ev.EventID, body, "")
		log.Printf("↑ %s  SV %s  %s (%d TC) – broker đã xác nhận", ev.EventID, ev.StudentID, ev.CourseID, ev.Credits)

		if *interval > 0 {
			select {
			case <-ctx.Done():
				log.Fatal("dừng giữa chừng")
			case <-time.After(*interval):
			}
		}
	}

	if *bad {
		id := fmt.Sprintf("ev-%s-bad", run)
		// Thiếu student_id, course_id và số tín chỉ âm: vi phạm hợp đồng thông điệp.
		body := []byte(fmt.Sprintf(`{"event_id":%q,"event_type":%q,"credits":-1}`, id, event.TypeStudentEnrolled))
		send(id, body, evidence.NoteMalformed)
		log.Printf("↑ %s  (thông điệp SAI hợp đồng – để kiểm thử tình huống lỗi)", id)
	}

	log.Printf("xong. Đối chiếu: go run ./cmd/verify -scenario %s -run %s", *mode, run)
}

func loadEnrollments(path string) ([]enrollment, error) {
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
	var out []enrollment
	for i, r := range rows[1:] { // bỏ dòng tiêu đề
		if len(r) != 6 {
			return nil, fmt.Errorf("%s dòng %d: cần 6 cột, có %d", path, i+2, len(r))
		}
		credits, err := strconv.Atoi(strings.TrimSpace(r[4]))
		if err != nil {
			return nil, fmt.Errorf("%s dòng %d: credits không phải số: %w", path, i+2, err)
		}
		out = append(out, enrollment{
			StudentID: r[0], FullName: r[1], CourseID: r[2], CourseName: r[3], Credits: credits, Semester: r[5],
		})
	}
	return out, nil
}

// newRunID tạo mã 4 ký tự hex cho mỗi lần chạy, ghép vào event ID: ev-<run>-<số thứ tự>.
func newRunID() string {
	b := make([]byte, 2)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().Unix()%0xffff, 16)
	}
	return hex.EncodeToString(b)
}
