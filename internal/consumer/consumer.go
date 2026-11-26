// Package consumer chạy vòng lặp nhận thông điệp với ACK THỦ CÔNG.
//
// Mỗi service (Finance Worker, Finance, Notification, Analytics, ...) chỉ cần viết một hàm Handler
// rồi gọi Run. Phần kết nối, khai báo queue, prefetch, ack/nack và ghi bằng chứng nằm ở đây.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"nmtht/chude07/internal/event"
	"nmtht/chude07/internal/evidence"
	"nmtht/chude07/internal/mq"
)

// Handler xử lý một sự kiện. Trả về nil = xử lý xong (thông điệp được ack).
type Handler func(ev event.StudentEnrolled) error

type Options struct {
	Service  string // tên ứng dụng, vd "finance-worker", "notification"
	Instance string // tên tiến trình, vd "W1"; để trống = Service
	Queue    string // queue sẽ đọc

	// BindingKey để trống  → kịch bản (i): đọc chung một queue với các worker khác.
	// BindingKey có giá trị → kịch bản (ii): tạo queue riêng và bind vào exchange academic.events.
	BindingKey string

	// Prefetch: số thông điệp tối đa broker giao cho tiến trình này mà chưa được ack. Mặc định 1.
	Prefetch int
}

// Run chặn cho đến khi nhấn Ctrl+C hoặc mất kết nối tới RabbitMQ.
func Run(opts Options, handle Handler) error {
	if opts.Service == "" || opts.Queue == "" {
		return errors.New("consumer: thiếu Service hoặc Queue")
	}
	if opts.Instance == "" {
		opts.Instance = opts.Service
	}
	if opts.Prefetch <= 0 {
		opts.Prefetch = 1
	}
	scenario := evidence.ScenarioWork
	if opts.BindingKey != "" {
		scenario = evidence.ScenarioPubSub
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("[%s] ", opts.Instance))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logName := opts.Service
	if opts.Instance != opts.Service {
		logName += "-" + opts.Instance
	}
	evlog, err := evidence.Open(logName)
	if err != nil {
		return err
	}
	defer evlog.Close()

	conn, err := mq.Dial(ctx, mq.URL())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("mở channel: %w", err)
	}

	if scenario == evidence.ScenarioWork {
		err = mq.DeclareQueue(ch, opts.Queue)
	} else {
		err = mq.DeclareSubscription(ch, opts.Queue, opts.BindingKey)
	}
	if err != nil {
		return err
	}

	// Prefetch = 1: broker chỉ giao thông điệp tiếp theo khi tiến trình đã ack thông điệp trước.
	// Worker bận không bị dồn việc, worker rảnh nhận việc tiếp (fair dispatch).
	if err := ch.Qos(opts.Prefetch, 0, false); err != nil {
		return fmt.Errorf("đặt prefetch: %w", err)
	}

	// autoAck = false: thông điệp ở trạng thái "Unacked" cho tới khi tiến trình gửi ack.
	// Nếu tiến trình chết trước khi ack, broker giao lại thông điệp cho consumer khác (Redelivered = true).
	msgs, err := ch.Consume(opts.Queue, opts.Instance, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("đăng ký consume %s: %w", opts.Queue, err)
	}

	if scenario == evidence.ScenarioWork {
		log.Printf("sẵn sàng – đọc chung queue %q, prefetch=%d, ack thủ công (Ctrl+C để dừng)", opts.Queue, opts.Prefetch)
	} else {
		log.Printf("sẵn sàng – queue riêng %q ← exchange %q (key %q), ack thủ công (Ctrl+C để dừng)",
			opts.Queue, mq.EventsExchange, opts.BindingKey)
	}

	w := &worker{opts: opts, scenario: scenario, handle: handle, evlog: evlog}
	for {
		select {
		case <-ctx.Done():
			log.Printf("dừng consumer")
			return nil
		case d, ok := <-msgs:
			if !ok {
				return errors.New("kênh nhận bị đóng: mất kết nối tới RabbitMQ")
			}
			w.process(d)
		}
	}
}

type worker struct {
	opts     Options
	scenario string
	handle   Handler
	evlog    *evidence.Log
}

func (w *worker) process(d amqp.Delivery) {
	ev, decodeErr := event.Decode(d.Body)
	id := d.MessageId
	if id == "" {
		id = ev.EventID
	}
	if id == "" {
		id = "(không có id)"
	}
	mark := ""
	if d.Redelivered {
		mark = " [GIAO LẠI]"
	}
	record := func(s evidence.Status, note string) {
		w.evlog.Write(evidence.Record{
			Service: w.opts.Service, Instance: w.opts.Instance, Scenario: w.scenario, Queue: w.opts.Queue,
			EventID: id, Status: s, Redelivered: d.Redelivered, Note: note,
			StudentID: ev.StudentID, CourseID: ev.CourseID,
		})
	}

	// 1. Sai hợp đồng: thử lại cũng vô ích → nack, requeue=false → RabbitMQ chuyển sang dead-letter queue.
	if decodeErr != nil {
		log.Printf("✗ từ chối %s%s: %v → %s", id, mark, decodeErr, mq.DeadLetterQueue)
		record(evidence.Rejected, decodeErr.Error())
		if err := d.Nack(false, false); err != nil {
			log.Printf("nack lỗi: %v", err)
		}
		return
	}

	log.Printf("→ nhận %s%s: SV %s, học phần %s", id, mark, ev.StudentID, ev.CourseID)
	record(evidence.Received, "")

	// 2. Lỗi khi xử lý: lần đầu trả lại queue để thử lại; nếu đã là lần giao lại thì chuyển dead-letter,
	//    tránh một thông điệp lỗi bị giao đi giao lại mãi.
	if err := w.handle(ev); err != nil {
		if !d.Redelivered {
			log.Printf("! lỗi xử lý %s: %v → trả lại queue để thử lại", id, err)
			record(evidence.Requeued, err.Error())
			_ = d.Nack(false, true)
			return
		}
		log.Printf("✗ lỗi lặp lại %s: %v → %s", id, err, mq.DeadLetterQueue)
		record(evidence.DeadLettered, err.Error())
		_ = d.Nack(false, false)
		return
	}

	// 3. Thành công: ack để broker xóa thông điệp khỏi queue.
	if err := d.Ack(false); err != nil {
		log.Printf("ack thất bại %s: %v (broker sẽ giao lại)", id, err)
		return
	}
	record(evidence.Processed, "")
	log.Printf("✓ xong %s (đã ack)", id)
}
