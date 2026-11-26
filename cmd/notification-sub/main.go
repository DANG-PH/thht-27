// Notification – subscriber của kịch bản (ii).
//
// Gửi thông báo xác nhận đăng ký cho sinh viên. Việc gửi được giả lập bằng cách in ra màn hình.
package main

import (
	"flag"
	"log"
	"time"

	"nmtht/chude07/internal/consumer"
	"nmtht/chude07/internal/event"
)

func main() {
	delay := flag.Duration("delay", 300*time.Millisecond, "thời gian giả lập gửi một thông báo")
	flag.Parse()

	err := consumer.Run(consumer.Options{
		Service:    "notification",
		Queue:      "notification.events", // queue riêng của Notification
		BindingKey: event.TypeStudentEnrolled,
	}, func(ev event.StudentEnrolled) error {
		time.Sleep(*delay)
		log.Printf("  [thông báo giả lập] tới %s (%s): đăng ký %s – %s thành công, %d tín chỉ",
			ev.FullName, ev.StudentID, ev.CourseID, ev.CourseName, ev.Credits)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
