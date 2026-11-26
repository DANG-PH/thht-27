// Analytics – subscriber của kịch bản (ii).
//
// Thống kê số lượt đăng ký và tổng tín chỉ theo học phần. Số liệu giữ trong bộ nhớ:
// đây là dữ liệu dẫn xuất từ luồng sự kiện, khởi động lại thì đếm lại từ các sự kiện nhận sau đó.
package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"text/tabwriter"

	"nmtht/chude07/internal/consumer"
	"nmtht/chude07/internal/event"
)

type courseStat struct {
	name    string
	count   int
	credits int
}

func main() {
	stats := map[string]*courseStat{}
	total := 0

	err := consumer.Run(consumer.Options{
		Service:    "analytics",
		Queue:      "analytics.events", // queue riêng của Analytics
		BindingKey: event.TypeStudentEnrolled,
	}, func(ev event.StudentEnrolled) error {
		s, ok := stats[ev.CourseID]
		if !ok {
			s = &courseStat{name: ev.CourseName}
			stats[ev.CourseID] = s
		}
		s.count++
		s.credits += ev.Credits
		total++
		log.Printf("  thống kê: %s có %d lượt (%d TC) | tổng %d lượt", ev.CourseID, s.count, s.credits, total)
		return nil
	})

	printSummary(stats, total)
	if err != nil {
		log.Fatal(err)
	}
}

// printSummary in bảng thống kê khi dừng Analytics (Ctrl+C).
func printSummary(stats map[string]*courseStat, total int) {
	ids := make([]string, 0, len(stats))
	for id := range stats {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Printf("\nTHỐNG KÊ ĐĂNG KÝ (tổng %d lượt)\n", total)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "HỌC PHẦN\tTÊN\tSỐ LƯỢT\tTỔNG TÍN CHỈ")
	for _, id := range ids {
		s := stats[id]
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\n", id, s.name, s.count, s.credits)
	}
	tw.Flush()
}
