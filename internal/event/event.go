// Package event định nghĩa HỢP ĐỒNG THÔNG ĐIỆP (message contract) giữa Academic và các bên nhận.
//
// Các service không gọi code của nhau. Thứ duy nhất chúng dùng chung là cấu trúc JSON dưới đây,
// nên đổi code bên trong một service không ảnh hưởng service khác, miễn là giữ đúng hợp đồng.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TypeStudentEnrolled là tên sự kiện, đồng thời là routing key khi publish lên exchange.
const TypeStudentEnrolled = "student.enrolled"

// StudentEnrolled: sinh viên vừa đăng ký một học phần (dữ liệu giả lập).
type StudentEnrolled struct {
	EventID    string    `json:"event_id"` // duy nhất cho mỗi sự kiện, dùng để đối chiếu log
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	StudentID  string    `json:"student_id"`
	FullName   string    `json:"full_name"`
	CourseID   string    `json:"course_id"`
	CourseName string    `json:"course_name"`
	Credits    int       `json:"credits"`
	Semester   string    `json:"semester"`
}

// Validate kiểm tra các trường bắt buộc của hợp đồng.
func (e StudentEnrolled) Validate() error {
	var errs []error
	if strings.TrimSpace(e.EventID) == "" {
		errs = append(errs, errors.New("thiếu event_id"))
	}
	if e.EventType != TypeStudentEnrolled {
		errs = append(errs, fmt.Errorf("event_type phải là %q", TypeStudentEnrolled))
	}
	if strings.TrimSpace(e.StudentID) == "" {
		errs = append(errs, errors.New("thiếu student_id"))
	}
	if strings.TrimSpace(e.CourseID) == "" {
		errs = append(errs, errors.New("thiếu course_id"))
	}
	if e.Credits <= 0 {
		errs = append(errs, errors.New("credits phải lớn hơn 0"))
	}
	return errors.Join(errs...)
}

// Decode đọc body thông điệp và kiểm tra hợp đồng.
//
// Cố ý KHÔNG từ chối trường lạ (không dùng DisallowUnknownFields): nếu Academic thêm trường mới,
// các consumer cũ vẫn chạy bình thường. Đây là nguyên tắc "tolerant reader" giúp giảm phụ thuộc.
func Decode(body []byte) (StudentEnrolled, error) {
	var e StudentEnrolled
	if err := json.Unmarshal(body, &e); err != nil {
		return e, fmt.Errorf("JSON không hợp lệ: %w", err)
	}
	return e, e.Validate()
}
