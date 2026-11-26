// Dashboard – dịch vụ CHỈ ĐỌC phục vụ trang web demo.
//
//	GET  /api/stream   Server-Sent Events: lịch sử + bản ghi mới từ logs/*.jsonl, số liệu queue mỗi giây
//	GET  /api/queues   số liệu queue hiện tại (JSON)
//	POST /api/send     chuyển tiếp yêu cầu gửi sang Academic (Academic chạy với -serve)
//
// Dashboard không publish hay consume thông điệp nào: nó đọc file log mà các service vốn đã ghi
// và hỏi RabbitMQ Management API. Tắt dashboard không ảnh hưởng luồng tích hợp.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"nmtht/chude07/internal/evidence"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "địa chỉ lắng nghe")
	academicURL := flag.String("academic", envOr("ACADEMIC_URL", "http://127.0.0.1:8081"), "địa chỉ Academic (chạy với -serve)")
	mgmtURL := flag.String("mgmt", envOr("MGMT_URL", "http://127.0.0.1:15672"), "địa chỉ RabbitMQ Management API")
	allowOrigin := flag.String("allow-origin", "*", "giá trị header Access-Control-Allow-Origin cho trang web")
	maxN := flag.Int("max-n", 200, "số sự kiện tối đa cho mỗi lần gửi từ trang web")
	perMinute := flag.Int("send-per-minute", 0, "số lần gửi tối đa mỗi phút cho toàn trang (0 = không giới hạn); bật khi trang công khai")
	flag.Parse()
	mgmtUser, mgmtPass := envOr("MGMT_USER", "demo"), envOr("MGMT_PASS", "demo")

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("[dashboard] ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := newHub()
	go runTailer(ctx, evidence.Dir(), h)
	go runQueuePoller(ctx, h, *mgmtURL, mgmtUser, mgmtPass)

	s := &server{
		hub:         h,
		academicURL: strings.TrimRight(*academicURL, "/"),
		client:      &http.Client{Timeout: 60 * time.Second},
		maxN:        *maxN,
		limit:       newLimiter(*perMinute),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stream", s.stream)
	mux.HandleFunc("GET /api/queues", s.queues)
	mux.HandleFunc("POST /api/send", s.send)
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           withCORS(*allowOrigin, mux),
		ReadHeaderTimeout: 5 * time.Second,
		// Request context kế thừa ctx: khi nhấn Ctrl+C, các kết nối SSE đang mở tự kết thúc.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("đang chạy tại http://%s (đọc %s/, Academic %s, Management API %s)",
		*addr, evidence.Dir(), s.academicURL, *mgmtURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

type server struct {
	hub         *hub
	academicURL string
	client      *http.Client
	maxN        int
	limit       *limiter
}

// stream giữ kết nối mở và đẩy sự kiện xuống trình duyệt theo chuẩn Server-Sent Events.
func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming không được hỗ trợ", http.StatusInternalServerError)
		return
	}
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("Connection", "keep-alive")

	ch, initial, cancel := s.hub.subscribe()
	defer cancel()
	for _, frame := range initial {
		if _, err := w.Write(frame); err != nil {
			return
		}
	}
	flusher.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case frame := <-ch:
			if _, err := w.Write(frame); err != nil {
				return
			}
			flusher.Flush()
		case <-ping.C:
			// Dòng chú thích giữ kết nối không bị proxy hay trình duyệt đóng vì im lặng quá lâu.
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *server) queues(w http.ResponseWriter, r *http.Request) {
	payload := s.hub.currentQueues()
	if payload == nil {
		payload = []byte(`{"queues":[],"error":"chưa lấy được số liệu queue"}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(payload)
}

// send chuyển tiếp yêu cầu gửi sang Academic. Trang web chỉ cần biết một địa chỉ là dashboard.
func (s *server) send(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var payload struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON không hợp lệ"})
		return
	}
	if payload.N > s.maxN {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("mỗi lần gửi tối đa %d sự kiện", s.maxN)})
		return
	}
	if !s.limit.allow() {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "gửi quá nhiều lần, chờ vài giây rồi thử lại"})
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.academicURL+"/api/enroll", bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "không gọi được Academic tại " + s.academicURL + " – đã chạy `go run ./cmd/academic -serve 127.0.0.1:8081` chưa?",
		})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func withCORS(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
