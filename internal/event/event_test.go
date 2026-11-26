package event

import "testing"

func TestDecodeValid(t *testing.T) {
	body := []byte(`{"event_id":"ev-a1b2-001","event_type":"student.enrolled","student_id":"SV001",
		"course_id":"TH301","credits":3,"truong_moi":"consumer cũ phải bỏ qua"}`)
	ev, err := Decode(body)
	if err != nil {
		t.Fatalf("thông điệp hợp lệ bị từ chối: %v", err)
	}
	if ev.StudentID != "SV001" || ev.CourseID != "TH301" || ev.Credits != 3 {
		t.Fatalf("đọc sai dữ liệu: %+v", ev)
	}
}

func TestDecodeRejectsBrokenContract(t *testing.T) {
	cases := map[string]string{
		"không phải JSON":  `not-json`,
		"thiếu event_id":   `{"event_type":"student.enrolled","student_id":"SV001","course_id":"TH301","credits":3}`,
		"sai event_type":   `{"event_id":"e1","event_type":"student.dropped","student_id":"SV001","course_id":"TH301","credits":3}`,
		"thiếu student_id": `{"event_id":"e1","event_type":"student.enrolled","course_id":"TH301","credits":3}`,
		"thiếu course_id":  `{"event_id":"e1","event_type":"student.enrolled","student_id":"SV001","credits":3}`,
		"tín chỉ âm":       `{"event_id":"e1","event_type":"student.enrolled","student_id":"SV001","course_id":"TH301","credits":-1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode([]byte(body)); err == nil {
				t.Fatal("phải bị từ chối nhưng lại được chấp nhận")
			}
		})
	}
}
