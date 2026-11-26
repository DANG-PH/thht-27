package academic

import "testing"

func TestBuildCatalogUniqueInOrder(t *testing.T) {
	rows := []Enrollment{
		{StudentID: "SV001", FullName: "A", CourseID: "TH301", CourseName: "Tích hợp", Credits: 3, Semester: "2026-1"},
		{StudentID: "SV002", FullName: "B", CourseID: "TH301", CourseName: "Tích hợp", Credits: 3, Semester: "2026-1"},
		{StudentID: "SV001", FullName: "A", CourseID: "MA101", CourseName: "Giải tích", Credits: 4, Semester: "2026-1"},
	}
	c := buildCatalog(rows)
	if len(c.Students) != 2 || c.Students[0].ID != "SV001" || c.Students[1].ID != "SV002" {
		t.Fatalf("danh sách sinh viên sai: %+v", c.Students)
	}
	if len(c.Courses) != 2 || c.Courses[1].ID != "MA101" || c.Courses[1].Fee != 4*450_000 {
		t.Fatalf("danh sách học phần sai: %+v", c.Courses)
	}
	if c.Semester != "2026-1" || c.PricePerCredit != 450_000 {
		t.Fatalf("thông tin chung sai: %+v", c)
	}
}
