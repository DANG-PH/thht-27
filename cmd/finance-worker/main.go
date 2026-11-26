// Finance Worker – consumer của kịch bản (i).
//
// Chạy nhiều tiến trình (W1, W2, W3) cùng đọc MỘT queue finance.work.
// RabbitMQ giao mỗi thông điệp cho đúng một worker → chia tải (competing consumers).
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"nmtht/chude07/internal/consumer"
	"nmtht/chude07/internal/event"
	"nmtht/chude07/internal/mq"
)

const donGiaTinChi = 450_000 // VND / tín chỉ – số liệu giả lập

func main() {
	id := flag.String("id", "W1", "tên worker, vd W1, W2, W3")
	delay := flag.Duration("delay", time.Second, "thời gian giả lập xử lý mỗi thông điệp")
	crashOn := flag.Int("crash-on", 0, "giả lập worker CHẾT khi đang xử lý thông điệp thứ N, trước khi ack (0 = tắt)")
	flag.Parse()

	handled := 0
	err := consumer.Run(consumer.Options{
		Service:  "finance-worker",
		Instance: *id,
		Queue:    mq.WorkQueue,
		Prefetch: 1,
	}, func(ev event.StudentEnrolled) error {
		handled++
		if *crashOn > 0 && handled == *crashOn {
			time.Sleep(*delay / 2) // đang xử lý dở: trên trang quản trị thấy thông điệp ở trạng thái Unacked
			log.Printf("SỰ CỐ GIẢ LẬP: tiến trình chết khi đang xử lý %s, chưa kịp ack", ev.EventID)
			os.Exit(1)
		}
		time.Sleep(*delay)
		log.Printf("  tạo khoản học phí: SV %s – %s, %d TC × %d = %d đ",
			ev.StudentID, ev.CourseID, ev.Credits, donGiaTinChi, ev.Credits*donGiaTinChi)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
