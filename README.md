# Chủ đề 07 – Tích hợp theo mô hình Message Queue và Publish/Subscribe

Academic phát sự kiện `student.enrolled` qua RabbitMQ theo **hai kịch bản độc lập**:

- **(i) Message Queue:** một queue `finance.work`, nhiều Finance Worker cùng đọc để chia tải.
- **(ii) Publish/Subscribe:** exchange `academic.events` phân phối bản sao sự kiện tới Finance, Notification, Analytics (và Game – subscriber thứ tư thêm sau); mỗi ứng dụng có queue riêng.

Backend viết bằng **Go**, trang web demo viết bằng **Next.js**. Toàn bộ dữ liệu trong `data/enrollments.csv` là **dữ liệu giả lập**.

- Báo cáo: [docs/bao-cao.md](docs/bao-cao.md)
- Minh chứng kiểm thử (chạy thật với RabbitMQ): [docs/evidence/](docs/evidence/) – tóm tắt ở [summary.txt](docs/evidence/summary.txt)

![Trang web demo](docs/images/web-tong-quan.png)

## Đối chiếu với yêu cầu của đề

| Yêu cầu | Cách đáp ứng | Minh chứng |
|---|---|---|
| (i) Một queue, nhiều Finance Worker chia tải | Queue `finance.work`; 3 tiến trình `cmd/finance-worker`; prefetch = 1; ack thủ công | [T1](docs/evidence/T1-chia-tai.txt), [T9](docs/evidence/T9-mo-rong.txt) |
| (ii) Pub/Sub tới Finance, Notification, Analytics, mỗi bên có queue riêng | Exchange topic `academic.events`; mỗi subscriber tự tạo queue `<tên>.events` và bind key `student.enrolled` | [T4](docs/evidence/T4-pubsub.txt) |
| Thể hiện ID sự kiện và nhật ký nhận tin | `event_id` trong body và thuộc tính AMQP `message_id`; log có giờ ở terminal, `logs/*.jsonl`, trang web | Mọi file minh chứng |
| Một thông điệp chỉ một worker xử lý (khi thành công) | Ack thủ công; `cmd/verify` đếm số lần `processed` của từng sự kiện | [T1](docs/evidence/T1-chia-tai.txt), [T2](docs/evidence/T2-worker-chet.txt) |
| Subscriber nhận theo đăng ký của mình | Mỗi subscriber bind queue của mình với routing key nó cần | Danh sách binding trong [T4](docs/evidence/T4-pubsub.txt) |
| Thêm subscriber mà không sửa mã publisher | `cmd/game-sub` thêm sau, chỉ tự khai báo queue và binding | [T6](docs/evidence/T6-them-subscriber.txt) |
| Ít nhất một tình huống lỗi | Worker chết giữa chừng, thông điệp sai hợp đồng, subscriber tạm tắt, RabbitMQ khởi động lại | [T2](docs/evidence/T2-worker-chet.txt), [T3](docs/evidence/T3-sai-hop-dong.txt), [T5](docs/evidence/T5-subscriber-tam-tat.txt), [T7–T8](docs/evidence/T7-T8-ben-vung.txt) |

## Kiến trúc

```
                                                    ┌─► Finance Worker W1
Academic ──► default exchange ──► queue finance.work ┼─► Finance Worker W2     (i) mỗi thông điệp → ĐÚNG MỘT worker
   ▲                                                └─► Finance Worker W3
   │ HTTP                                ┌─► finance.events      ─► Finance
   │        exchange academic.events ────┼─► notification.events ─► Notification   (ii) mỗi subscriber → MỘT BẢN SAO
   │        (topic, key student.enrolled)├─► analytics.events    ─► Analytics
   │                                     └─► game.events         ─► Game (thêm sau)
   │
Dashboard API (Go, chỉ đọc) ◄── đọc logs/*.jsonl + RabbitMQ Management API
   ▲ SSE
Trình duyệt (Next.js)

Thông điệp bị từ chối ──► exchange academic.dlx (fanout) ──► queue academic.dead-letter
```

Trang web **không** kết nối RabbitMQ. Nó chỉ gọi Dashboard API; Dashboard đọc file log mà các service vốn đã ghi và hỏi Management API. Tắt web hay Dashboard không ảnh hưởng luồng tích hợp.

| Thiết lập độ tin cậy | Ở đâu | Tác dụng |
|---|---|---|
| Queue durable, message persistent | `internal/mq` | Queue và thông điệp còn nguyên khi RabbitMQ khởi động lại |
| Publisher confirm | `mq.Publisher` | Academic chỉ báo "đã gửi" khi broker xác nhận |
| Ack thủ công | `internal/consumer` | Consumer chết trước khi ack thì broker giao lại thông điệp |
| Prefetch = 1 | `internal/consumer` | Worker bận không bị dồn việc |
| Dead-letter queue | `internal/mq` | Thông điệp sai hợp đồng không mất, nằm chờ kiểm tra |

## Cấu trúc thư mục

```
docker-compose.yml        RabbitMQ 4.1 + trang quản trị (rabbitmq/demo.conf: cập nhật số liệu mỗi giây)
data/enrollments.csv      12 lượt đăng ký giả lập
internal/event            hợp đồng thông điệp student.enrolled + kiểm tra trường bắt buộc
internal/mq               kết nối, khai báo queue/exchange, publish có xác nhận
internal/consumer         vòng lặp nhận: prefetch, ack/nack, ghi nhật ký       ← file quan trọng nhất
internal/academic         nghiệp vụ gửi của Academic (dùng chung cho dòng lệnh và HTTP)
internal/evidence         ghi/đọc nhật ký logs/*.jsonl
cmd/academic              Producer: gửi từ dòng lệnh, hoặc chạy HTTP với -serve
cmd/finance-worker        Consumer kịch bản (i); cờ -crash-on để giả lập worker chết
cmd/finance-sub           Subscriber kịch bản (ii) – mẫu cho các subscriber khác
cmd/notification-sub      Subscriber kịch bản (ii): thông báo cho sinh viên (giả lập)
cmd/analytics-sub         Subscriber kịch bản (ii): thống kê theo học phần
cmd/game-sub              Subscriber thứ tư, thêm sau mà không sửa Academic
cmd/verify                Đối chiếu nhật ký, kết luận đạt/không đạt
cmd/dashboard             API chỉ đọc cho trang web: SSE + số liệu queue + chuyển tiếp lệnh gửi
web/                      Trang web Next.js
scripts/                  start-all.sh, stop.sh, e2e.sh
docs/                     báo cáo, minh chứng, ảnh chụp
```

## Cài đặt

Cần Docker, Go ≥ 1.22, Node.js ≥ 20.9 (cho web). **Mọi lệnh chạy từ thư mục gốc của dự án.**

```bash
docker compose up -d --wait        # RabbitMQ, trang quản trị http://localhost:15672 (demo / demo)
go test ./...                      # kiểm thử đơn vị
cd web && npm install && cd ..     # cài web (một lần)
```

Đổi địa chỉ broker bằng biến môi trường `AMQP_URL` (mặc định `amqp://demo:demo@localhost:5672/`).

## Cách 1 – Demo bằng trang web (Linux/macOS/Git Bash)

```bash
scripts/start-all.sh               # RabbitMQ, Academic (HTTP), Dashboard API, 3 worker, 4 subscriber
cd web && npm run dev              # mở http://localhost:3000
```

Trên trang web, chọn kịch bản rồi bấm **Academic: gửi student.enrolled**. Các tình huống để trình bày:

| Muốn cho thấy | Thao tác | Quan sát trên web |
|---|---|---|
| Chia tải | Kịch bản (i), gửi 12 | Mỗi worker một làn, mỗi ô sự kiện chỉ ở một làn, nhãn ĐẠT |
| Worker chết khi chưa ack | Kịch bản (i), gửi 12, ngay sau đó `scripts/stop.sh -9 W1` | Ô của W1 bị gạch, cùng sự kiện hiện ở làn khác với `↻` |
| Bản sao cho mọi subscriber | Kịch bản (ii), gửi 6 | Ma trận toàn ✓ |
| Subscriber tạm tắt | `scripts/stop.sh notification`, gửi 6 | Thẻ Notification "đang tắt", Ready = 6, cột ✗; `scripts/start-all.sh` → chuyển ✓ |
| Thông điệp sai hợp đồng | Tick "Kèm 1 thông điệp sai hợp đồng" | Ô đỏ `bad`, số Dead-letter ở góc phải tăng |
| Thêm subscriber | Game đã chạy; trên bản đồ trường có nhân vật | Code Academic không đổi (`git log --stat` commit thêm `cmd/game-sub`) |

Dừng tất cả: `scripts/stop.sh`. Sau khi khởi động lại RabbitMQ, chạy lại `scripts/stop.sh && scripts/start-all.sh` vì các tiến trình không tự kết nối lại.

## Cách 2 – Demo bằng terminal (mọi hệ điều hành)

Kịch bản (i) – 3 terminal chạy worker, 1 terminal gửi rồi đối chiếu:

```bash
go run ./cmd/finance-worker -id W1          # tương tự W2, W3
go run ./cmd/academic -mode work
go run ./cmd/verify -scenario work
```

Worker chết giữa chừng: chạy W1 với `-crash-on 2 -delay 3s`, gửi lại như trên. Thông điệp sai hợp đồng: `go run ./cmd/academic -mode work -n 3 -bad`, rồi xem queue `academic.dead-letter` trên trang quản trị.

Kịch bản (ii) – mỗi subscriber một terminal:

```bash
go run ./cmd/finance-sub
go run ./cmd/notification-sub
go run ./cmd/analytics-sub
go run ./cmd/academic -mode pubsub -n 5
go run ./cmd/verify -scenario pubsub
```

Thêm subscriber mới: chạy `go run ./cmd/game-sub` rồi gửi lại. Subscriber mới chỉ nhận sự kiện gửi **sau khi** nó tạo queue; `verify` của lần gửi trước đó hiện ✗ ở cột game – đúng thiết kế Pub/Sub.

## Triển khai lên VPS (pm2) và CI/CD

Trên VPS, các chương trình Go và web chạy bằng **pm2** (namespace `thht27`, mọi app có tiền tố `thht27-`), RabbitMQ chạy bằng Docker. Chỉ trang web mở ra ngoài (cổng `3027`); RabbitMQ, Academic và Dashboard API chỉ lắng nghe trong máy (`127.0.0.1`).

```
Internet ──► :3027 thht27-web (Next.js) ──/api──► 127.0.0.1:18028 thht27-dashboard ──► 127.0.0.1:18027 thht27-academic
                                                                                    └─► RabbitMQ 127.0.0.1:5672 ◄── 7 worker/subscriber
```

**Không build trên VPS.** VPS dùng chung RAM với nhiều dịch vụ khác, nên mọi thứ được build sẵn (`make release`: file chạy Go cho Linux amd64 + web Next.js dạng tự chứa) rồi chép sang; VPS không cần cài Go hay chạy `npm install`.

| Cách | Lệnh |
|---|---|
| Deploy thủ công từ máy cá nhân | `make deploy VPS=root@<ip>` (dùng khóa `~/.ssh/id_ed25519_vps`) |
| Tự động (CI/CD) | Push lên `main` → GitHub Actions chạy `make test`, `make release`, chép `release.tar.gz` sang VPS, chạy `scripts/deploy-remote.sh` |

CI/CD cần khai báo secrets trong repo GitHub (**Settings → Secrets and variables → Actions**): `VPS_HOST`, `VPS_SSH_KEY` (khóa riêng tư đã được VPS chấp nhận); tùy chọn `VPS_USER` (mặc định `root`), `VPS_PORT` (22), `VPS_APP_PATH` (`/root/thht-27`). Chưa có secrets thì job deploy tự bỏ qua.

Lần deploy đầu tiên, `scripts/deploy-remote.sh` tạo file `.env` trên VPS với mật khẩu RabbitMQ ngẫu nhiên. File này nằm trong `.gitignore`; quy trình deploy chỉ `git reset --hard`, **không** `git clean`, nên `.env` được giữ nguyên.

Thao tác trình diễn trên VPS (sau khi `ssh` vào):

| Muốn cho thấy | Lệnh |
|---|---|
| Trạng thái các tiến trình | `pm2 ls` |
| Subscriber tạm tắt rồi bật lại | `pm2 stop thht27-notification` … `pm2 start thht27-notification` |
| Worker chết khi đang xử lý | Gửi 12 sự kiện kịch bản (i) trên web, ngay sau đó `pm2 sendSignal SIGKILL thht27-W1` (pm2 tự bật lại W1 sau 3 giây) |
| Xem log một tiến trình | `pm2 logs thht27-W2` |
| Trang quản trị RabbitMQ | Trên máy cá nhân: `ssh -L 15672:127.0.0.1:15672 root@<ip>`, rồi mở http://localhost:15672 (tài khoản trong `/root/thht-27/.env`) |

## Kiểm thử tự động (T1–T9)

```bash
scripts/stop.sh        # nếu đang chạy demo
scripts/e2e.sh         # khoảng 1 phút; XÓA dữ liệu RabbitMQ của demo và thư mục logs/
```

Script chạy lần lượt mọi ca kiểm thử với RabbitMQ thật, ghi kết quả vào `docs/evidence/`. Kết quả lần chạy gần nhất: [docs/evidence/summary.txt](docs/evidence/summary.txt).

## Viết một subscriber mới

```go
err := consumer.Run(consumer.Options{
    Service:    "audit",
    Queue:      "audit.events",              // queue riêng
    BindingKey: event.TypeStudentEnrolled,   // đăng ký nhận sự kiện nào
}, func(ev event.StudentEnrolled) error {
    log.Printf("  ghi sổ: %s đăng ký %s", ev.StudentID, ev.CourseID)
    return nil // nil = xử lý xong → ack
})
```

Ack, nack, prefetch và ghi nhật ký nằm trong `internal/consumer/consumer.go` (khoảng 200 dòng). Cả nhóm nên đọc file này để trả lời được câu hỏi về ack.

## Chạy lại từ đầu

`scripts/stop.sh`, `docker compose down -v` (xóa queue và thông điệp), `rm -rf logs`.

## Công cụ hỗ trợ

Mã nguồn, kiểm thử và bản nháp báo cáo được tạo với sự hỗ trợ của Claude Code (Anthropic). Nhóm tự chạy kiểm thử, đọc hiểu và chỉnh sửa toàn bộ mã nộp.
