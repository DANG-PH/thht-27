// Verify – đọc logs/*.jsonl và đối chiếu kết quả một lần gửi của Academic.
//
//	Kịch bản (i):  mỗi sự kiện hợp lệ phải được xử lý thành công ĐÚNG MỘT LẦN, bởi một worker.
//	Kịch bản (ii): mỗi subscriber phải nhận và xử lý MỖI sự kiện (mỗi bên một bản sao).
//
// Mã thoát: 0 = đạt, 1 = không đạt, 2 = lỗi đầu vào.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"nmtht/chude07/internal/evidence"
)

func main() {
	scenario := flag.String("scenario", evidence.ScenarioWork, "kịch bản cần kiểm tra: work | pubsub")
	run := flag.String("run", "", "mã lần gửi (vd a3f9); để trống = lần gửi gần nhất của kịch bản")
	flag.Parse()

	recs, err := evidence.ReadAll(evidence.Dir())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Lỗi:", err)
		os.Exit(2)
	}

	var ok bool
	switch *scenario {
	case evidence.ScenarioWork:
		ok, err = reportWork(os.Stdout, recs, *run)
	case evidence.ScenarioPubSub:
		ok, err = reportPubSub(os.Stdout, recs, *run)
	default:
		err = fmt.Errorf("-scenario phải là %q hoặc %q", evidence.ScenarioWork, evidence.ScenarioPubSub)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Lỗi:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

// runOf lấy mã lần gửi từ event ID dạng ev-<run>-<số thứ tự>.
func runOf(eventID string) string {
	parts := strings.SplitN(eventID, "-", 3)
	if len(parts) != 3 || parts[0] != "ev" {
		return ""
	}
	return parts[1]
}

type batch struct {
	run       string
	normal    []string                     // event ID hợp lệ, theo thứ tự gửi
	malformed []string                     // event ID cố ý gửi sai hợp đồng
	byEvent   map[string][]evidence.Record // bản ghi phía consumer, theo event ID
}

func selectBatch(recs []evidence.Record, scenario, run string) (*batch, error) {
	isPublished := func(r evidence.Record) bool {
		return r.Service == "academic" && r.Scenario == scenario && r.Status == evidence.Published
	}
	if run == "" {
		var latest *evidence.Record
		for i := range recs {
			if isPublished(recs[i]) && (latest == nil || recs[i].Time.After(latest.Time)) {
				latest = &recs[i]
			}
		}
		if latest == nil {
			return nil, fmt.Errorf("chưa có lần gửi nào của kịch bản %q trong thư mục %q", scenario, evidence.Dir())
		}
		run = runOf(latest.EventID)
	}

	b := &batch{run: run, byEvent: map[string][]evidence.Record{}}
	for _, r := range recs {
		if r.Scenario != scenario || runOf(r.EventID) != run {
			continue
		}
		switch {
		case isPublished(r) && r.Note == evidence.NoteMalformed:
			b.malformed = append(b.malformed, r.EventID)
		case isPublished(r):
			b.normal = append(b.normal, r.EventID)
		case r.Service != "academic":
			b.byEvent[r.EventID] = append(b.byEvent[r.EventID], r)
		}
	}
	if len(b.normal)+len(b.malformed) == 0 {
		return nil, fmt.Errorf("không tìm thấy lần gửi %q của kịch bản %q", run, scenario)
	}
	return b, nil
}

func step(r evidence.Record) string {
	s := r.Instance + ":" + string(r.Status)
	if r.Redelivered {
		s += "(giao lại)"
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "–"
	}
	return s
}

// reportMalformed in kết quả của các thông điệp cố ý gửi sai hợp đồng.
func reportMalformed(w io.Writer, b *batch) {
	for _, id := range b.malformed {
		var steps []string
		for _, r := range b.byEvent[id] {
			steps = append(steps, step(r))
		}
		fmt.Fprintf(w, "Thông điệp sai hợp đồng %s: %s (kỳ vọng: rejected → academic.dead-letter)\n",
			id, orDash(strings.Join(steps, ", ")))
	}
}

func reportWork(w io.Writer, recs []evidence.Record, run string) (bool, error) {
	b, err := selectBatch(recs, evidence.ScenarioWork, run)
	if err != nil {
		return false, err
	}
	fmt.Fprintf(w, "KỊCH BẢN (i) – MESSAGE QUEUE – lần gửi %s: %d sự kiện hợp lệ, %d sai hợp đồng\n\n",
		b.run, len(b.normal), len(b.malformed))

	perWorker := map[string]int{}
	var missing, dup, redelivered []string

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "EVENT ID\tXỬ LÝ XONG BỞI\tDIỄN BIẾN")
	for _, id := range b.normal {
		var doneBy, steps []string
		wasRedelivered := false
		for _, r := range b.byEvent[id] {
			steps = append(steps, step(r))
			wasRedelivered = wasRedelivered || r.Redelivered
			if r.Status == evidence.Processed {
				doneBy = append(doneBy, r.Instance)
				perWorker[r.Instance]++
			}
		}
		switch len(doneBy) {
		case 0:
			missing = append(missing, id)
		case 1:
		default:
			dup = append(dup, id)
		}
		if wasRedelivered {
			redelivered = append(redelivered, id)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", id, orDash(strings.Join(doneBy, ", ")), orDash(strings.Join(steps, " → ")))
	}
	tw.Flush()

	workers := make([]string, 0, len(perWorker))
	for k := range perWorker {
		workers = append(workers, k)
	}
	sort.Strings(workers)
	fmt.Fprintln(w, "\nPhân tải:")
	for _, k := range workers {
		fmt.Fprintf(w, "  %-6s %d sự kiện\n", k, perWorker[k])
	}

	fmt.Fprintf(w, "\nXử lý thành công đúng 1 lần: %d/%d\n", len(b.normal)-len(missing)-len(dup), len(b.normal))
	fmt.Fprintf(w, "Chưa được xử lý: %s\n", orDash(strings.Join(missing, ", ")))
	fmt.Fprintf(w, "Xử lý nhiều hơn 1 lần: %s\n", orDash(strings.Join(dup, ", ")))
	fmt.Fprintf(w, "Được RabbitMQ giao lại: %s\n", orDash(strings.Join(redelivered, ", ")))
	reportMalformed(w, b)

	ok := len(missing) == 0 && len(dup) == 0
	if ok {
		fmt.Fprintln(w, "\nKẾT LUẬN: ĐẠT – mỗi thông điệp được đúng một worker xử lý thành công.")
	} else {
		fmt.Fprintln(w, "\nKẾT LUẬN: CHƯA ĐẠT – xem các sự kiện liệt kê ở trên (worker còn chạy chưa? queue còn thông điệp không?).")
	}
	return ok, nil
}

func reportPubSub(w io.Writer, recs []evidence.Record, run string) (bool, error) {
	b, err := selectBatch(recs, evidence.ScenarioPubSub, run)
	if err != nil {
		return false, err
	}

	// Danh sách subscriber = mọi ứng dụng từng ghi log ở kịch bản (ii), kể cả ở lần gửi khác.
	// Nhờ vậy subscriber đang tắt hoặc mới thêm vẫn có cột riêng và lộ ra ô thiếu.
	seen := map[string]bool{}
	for _, r := range recs {
		if r.Scenario == evidence.ScenarioPubSub && r.Service != "academic" {
			seen[r.Service] = true
		}
	}
	if len(seen) == 0 {
		return false, fmt.Errorf("chưa có subscriber nào ghi log – hãy chạy subscriber trước")
	}
	subs := make([]string, 0, len(seen))
	for s := range seen {
		subs = append(subs, s)
	}
	sort.Strings(subs)

	fmt.Fprintf(w, "KỊCH BẢN (ii) – PUBLISH/SUBSCRIBE – lần gửi %s: %d sự kiện hợp lệ, %d sai hợp đồng\n\n",
		b.run, len(b.normal), len(b.malformed))

	got := map[string]int{}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "EVENT ID\t%s\n", strings.Join(subs, "\t"))
	for _, id := range b.normal {
		processed := map[string]int{}
		received := map[string]bool{}
		for _, r := range b.byEvent[id] {
			received[r.Service] = true
			if r.Status == evidence.Processed {
				processed[r.Service]++
			}
		}
		cells := make([]string, len(subs))
		for i, s := range subs {
			switch {
			case processed[s] == 1:
				cells[i] = "✓"
				got[s]++
			case processed[s] > 1:
				cells[i] = fmt.Sprintf("✓×%d", processed[s])
				got[s]++
			case received[s]:
				cells[i] = "… (nhận, chưa xong)"
			default:
				cells[i] = "✗"
			}
		}
		fmt.Fprintf(tw, "%s\t%s\n", id, strings.Join(cells, "\t"))
	}
	tw.Flush()

	fmt.Fprintln(w, "\nSố sự kiện mỗi subscriber đã xử lý:")
	ok := true
	for _, s := range subs {
		fmt.Fprintf(w, "  %-14s %d/%d\n", s, got[s], len(b.normal))
		if got[s] != len(b.normal) {
			ok = false
		}
	}
	reportMalformed(w, b)

	if ok {
		fmt.Fprintln(w, "\nKẾT LUẬN: ĐẠT – mỗi subscriber nhận đủ một bản sao của từng sự kiện.")
	} else {
		fmt.Fprintln(w, "\nKẾT LUẬN: CÓ Ô ✗. Hai khả năng:\n"+
			"  - subscriber đang tắt: queue riêng của nó vẫn giữ thông điệp, bật lại sẽ nhận đủ;\n"+
			"  - subscriber được thêm SAU lần gửi này: lúc publish chưa có queue của nó nên không có bản sao (đúng thiết kế Pub/Sub).")
	}
	return ok, nil
}
