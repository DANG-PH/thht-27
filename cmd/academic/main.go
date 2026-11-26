// Academic – ứng dụng phát sự kiện student.enrolled (Producer).
//
// Hai cách chạy:
//
//	go run ./cmd/academic -mode work -n 12          gửi một lần rồi thoát (dùng trong terminal)
//	go run ./cmd/academic -serve 127.0.0.1:8081     chạy như dịch vụ HTTP để trang web bấm nút gửi
//
// Nghiệp vụ gửi nằm ở internal/academic, dùng chung cho cả hai cách.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nmtht/chude07/internal/academic"
	"nmtht/chude07/internal/evidence"
	"nmtht/chude07/internal/mq"
)

func main() {
	mode := flag.String("mode", evidence.ScenarioPubSub,
		"work = kịch bản (i): gửi vào queue finance.work; pubsub = kịch bản (ii): publish lên exchange academic.events")
	n := flag.Int("n", 0, "số sự kiện cần gửi (0 = gửi hết dữ liệu giả lập)")
	bad := flag.Bool("bad", false, "gửi thêm 1 thông điệp sai hợp đồng để thử tình huống lỗi")
	dataPath := flag.String("data", "data/enrollments.csv", "file CSV dữ liệu đăng ký giả lập")
	interval := flag.Duration("interval", 0, "nghỉ giữa 2 lần gửi, vd 300ms")
	serve := flag.String("serve", "", "chạy như dịch vụ HTTP tại địa chỉ này, vd 127.0.0.1:8081")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("[academic] ")

	rows, err := academic.LoadEnrollments(*dataPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := mq.Dial(ctx, mq.URL())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		log.Fatal(err)
	}
	evlog, err := evidence.Open("academic")
	if err != nil {
		log.Fatal(err)
	}
	defer evlog.Close()

	sender, err := academic.NewSender(pub, evlog, rows)
	if err != nil {
		log.Fatal(err)
	}

	if *serve != "" {
		if err := serveHTTP(ctx, *serve, sender); err != nil {
			log.Fatal(err)
		}
		return
	}

	res, err := sender.Send(ctx, academic.Request{Mode: *mode, N: *n, Bad: *bad, Interval: *interval})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("xong. Đối chiếu: go run ./cmd/verify -scenario %s -run %s", res.Mode, res.Run)
}

// serveHTTP mở API POST /api/enroll, body JSON {"mode":"work|pubsub","n":5,"bad":false}.
func serveHTTP(ctx context.Context, addr string, sender *academic.Sender) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/enroll", func(w http.ResponseWriter, r *http.Request) {
		var req academic.Request
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON không hợp lệ: " + err.Error()})
			return
		}
		res, err := sender.Send(r.Context(), req)
		switch {
		case errors.Is(err, academic.ErrInvalidRequest):
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		case err != nil:
			// Lỗi phía broker (mất kết nối...). Trả kèm các sự kiện đã gửi được trước khi lỗi.
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error(), "partial": res})
		default:
			writeJSON(w, http.StatusOK, res)
		}
	})
	mux.HandleFunc("GET /api/catalog", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, sender.Catalog())
	})
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("chế độ HTTP: POST http://%s/api/enroll  body {\"mode\":\"work|pubsub\",\"n\":5,\"bad\":false}", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
