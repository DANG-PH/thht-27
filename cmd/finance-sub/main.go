// Finance – subscriber của kịch bản (ii).
//
// ĐÂY LÀ MẪU để viết Notification, Analytics và subscriber mới:
// chép cả thư mục này sang cmd/<tên-mới>, rồi sửa 3 chỗ có ghi "ĐỔI".
package main

import (
	"log"

	"nmtht/chude07/internal/consumer"
	"nmtht/chude07/internal/event"
	"nmtht/chude07/internal/tuition"
)

func main() {
	err := consumer.Run(consumer.Options{
		Service:    "finance",                 // ĐỔI: tên ứng dụng
		Queue:      "finance.events",          // ĐỔI: mỗi subscriber một queue riêng
		BindingKey: event.TypeStudentEnrolled, // đăng ký nhận sự kiện student.enrolled
	}, func(ev event.StudentEnrolled) error {
		// ĐỔI: nghiệp vụ của ứng dụng. Trả về nil = xử lý xong, thông điệp được ack.
		log.Printf("  ghi nhận nghĩa vụ học phí: SV %s – %s, %d TC = %d đ",
			ev.StudentID, ev.CourseID, ev.Credits, tuition.Fee(ev.Credits))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
