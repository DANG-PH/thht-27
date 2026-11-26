// Game – subscriber THỨ TƯ, được thêm sau mà KHÔNG sửa Academic.
//
// Mỗi sinh viên đăng ký học phần thì một nhân vật xuất hiện trước tòa nhà của học phần đó
// trên bản đồ trường ở trang web. Service này chỉ cần tự khai báo queue game.events
// và bind vào exchange academic.events; Academic không biết Game tồn tại.
package main

import (
	"log"

	"nmtht/chude07/internal/consumer"
	"nmtht/chude07/internal/event"
)

func main() {
	err := consumer.Run(consumer.Options{
		Service:    "game",
		Queue:      "game.events",
		BindingKey: event.TypeStudentEnrolled,
	}, func(ev event.StudentEnrolled) error {
		log.Printf("  nhân vật %s (%s) xuất hiện trước tòa %s", ev.StudentID, ev.FullName, ev.CourseID)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
