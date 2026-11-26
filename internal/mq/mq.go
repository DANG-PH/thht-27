// Package mq gói các thao tác RabbitMQ dùng chung: kết nối, khai báo topology và publish có xác nhận.
package mq

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Kịch bản (i) – Message Queue (Point-to-Point Channel):
	// Academic gửi thẳng vào MỘT queue, nhiều Finance Worker cùng đọc, mỗi thông điệp chỉ tới một worker.
	WorkQueue = "finance.work"

	// Kịch bản (ii) – Publish/Subscribe:
	// Academic publish lên exchange; mỗi subscriber có queue RIÊNG bind vào exchange và nhận một bản sao.
	EventsExchange = "academic.events"

	// Dead-letter: thông điệp bị từ chối (sai hợp đồng, lỗi lặp lại) được RabbitMQ chuyển về đây,
	// không mất mà nằm chờ người kiểm tra.
	DeadLetterExchange = "academic.dlx"
	DeadLetterQueue    = "academic.dead-letter"
)

// URL đọc địa chỉ RabbitMQ từ biến môi trường AMQP_URL (cấu hình nằm ngoài mã nguồn).
func URL() string {
	if v := os.Getenv("AMQP_URL"); v != "" {
		return v
	}
	return "amqp://demo:demo@localhost:5672/"
}

// Dial kết nối có thử lại, vì container RabbitMQ cần vài giây sau khi khởi động mới nhận kết nối.
func Dial(ctx context.Context, url string) (*amqp.Connection, error) {
	const maxAttempts = 15
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		log.Printf("chưa kết nối được RabbitMQ (lần %d/%d): %v", attempt, maxAttempts, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("không kết nối được RabbitMQ: %w", lastErr)
}

// queueArgs: mọi queue nghiệp vụ đều trỏ dead-letter về academic.dlx.
// Tham số này phải GIỐNG HỆT ở mọi nơi khai báo cùng một queue,
// nếu khác RabbitMQ trả lỗi PRECONDITION_FAILED, nên chỉ khai báo qua các hàm trong file này.
func queueArgs() amqp.Table {
	return amqp.Table{"x-dead-letter-exchange": DeadLetterExchange}
}

func declareDeadLetter(ch *amqp.Channel) error {
	// durable=true: exchange/queue còn tồn tại sau khi RabbitMQ khởi động lại.
	if err := ch.ExchangeDeclare(DeadLetterExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("khai báo exchange %s: %w", DeadLetterExchange, err)
	}
	if _, err := ch.QueueDeclare(DeadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("khai báo queue %s: %w", DeadLetterQueue, err)
	}
	if err := ch.QueueBind(DeadLetterQueue, "", DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", DeadLetterQueue, err)
	}
	return nil
}

// DeclareQueue khai báo một queue bền (durable) có dead-letter.
// Khai báo là idempotent: Academic và Finance Worker đều gọi, bên nào chạy trước cũng được,
// và thông điệp gửi trước khi worker bật vẫn nằm chờ trong queue.
func DeclareQueue(ch *amqp.Channel, name string) error {
	if err := declareDeadLetter(ch); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, queueArgs()); err != nil {
		return fmt.Errorf("khai báo queue %s: %w", name, err)
	}
	return nil
}

// DeclareEventsExchange khai báo exchange kiểu topic của kịch bản (ii).
// Academic chỉ biết exchange này, KHÔNG biết có những subscriber nào phía sau.
func DeclareEventsExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(EventsExchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("khai báo exchange %s: %w", EventsExchange, err)
	}
	return nil
}

// DeclareSubscription: subscriber TỰ tạo queue riêng và TỰ đăng ký (bind) vào exchange.
// Vì vậy thêm một subscriber mới chỉ cần một service mới gọi hàm này; code publisher không đổi.
func DeclareSubscription(ch *amqp.Channel, queue, bindingKey string) error {
	if err := DeclareEventsExchange(ch); err != nil {
		return err
	}
	if err := DeclareQueue(ch, queue); err != nil {
		return err
	}
	if err := ch.QueueBind(queue, bindingKey, EventsExchange, false, nil); err != nil {
		return fmt.Errorf("bind %s ← %s (%s): %w", queue, EventsExchange, bindingKey, err)
	}
	return nil
}

// Publisher gửi thông điệp và chờ broker xác nhận (publisher confirm).
type Publisher struct {
	ch *amqp.Channel
}

func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mở channel: %w", err)
	}
	// Bật chế độ confirm: broker báo ack/nack cho từng thông điệp publisher gửi lên.
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("bật publisher confirm: %w", err)
	}
	return &Publisher{ch: ch}, nil
}

// Channel trả về channel để khai báo topology trước khi gửi.
func (p *Publisher) Channel() *amqp.Channel { return p.ch }

// Publish gửi một thông điệp bền và CHỜ broker xác nhận đã nhận.
//
//   - exchange = "" (default exchange): routingKey là tên queue đích – dùng cho kịch bản (i).
//   - exchange = academic.events: routingKey là tên sự kiện – dùng cho kịch bản (ii).
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey, msgType, messageID string, body []byte) error {
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent, // ghi xuống đĩa: còn nguyên nếu broker khởi động lại
		MessageId:    messageID,       // consumer đọc được ID kể cả khi body hỏng
		Type:         msgType,
		Timestamp:    time.Now(),
		AppId:        "academic",
		Body:         body,
	})
	if err != nil {
		return err
	}
	if !dc.Wait() {
		return fmt.Errorf("broker không xác nhận thông điệp %s", messageID)
	}
	return nil
}
