# Chủ đề 07 – Tích hợp theo mô hình Message Queue và Publish/Subscribe

Academic phát sự kiện `student.enrolled` qua RabbitMQ theo **hai kịch bản độc lập**:

- **(i) Message Queue:** một queue `finance.work`, nhiều Finance Worker cùng đọc để chia tải.
- **(ii) Publish/Subscribe:** exchange `academic.events` phân phối bản sao sự kiện tới Finance, Notification và Analytics; mỗi ứng dụng có queue riêng.

Toàn bộ dữ liệu trong `data/enrollments.csv` là **dữ liệu giả lập**.

Báo cáo (bài toán, kiến thức, kiến trúc, luồng dữ liệu, kiểm thử, nhận xét): [docs/bao-cao.md](docs/bao-cao.md).

## Đối chiếu với yêu cầu của đề

| Yêu cầu | Cách đáp ứng | Kiểm chứng |
|---|---|---|
| (i) Một queue, nhiều Finance Worker chia tải | Queue `finance.work`; chạy `cmd/finance-worker` 3 lần (W1, W2, W3); prefetch = 1; ack thủ công | `go run ./cmd/verify -scenario work` |
| (ii) Pub/Sub tới Finance, Notification, Analytics, mỗi bên có queue riêng | Exchange topic `academic.events`; mỗi subscriber tự tạo queue `<tên>.events` và bind với key `student.enrolled` | `go run ./cmd/verify -scenario pubsub` |
| Thể hiện ID sự kiện và nhật ký nhận tin | `event_id` nằm trong body và trong thuộc tính AMQP `message_id`; log có giờ ở terminal và log máy đọc được ở `logs/*.jsonl` | Terminal, thư mục `logs/` |
| Một thông điệp trong queue chỉ một worker xử lý (khi xử lý thành công) | Ack thủ công: broker chỉ xóa thông điệp khi nhận ack | `verify` đếm số lần `processed` của từng sự kiện |
| Subscriber nhận sự kiện theo đăng ký của mình | Mỗi subscriber tự bind queue của mình với routing key nó quan tâm | Trang quản trị → Exchanges → `academic.events` → Bindings |
| Thêm subscriber mà không sửa mã publisher | Subscriber tự khai báo queue và binding; Academic không có danh sách subscriber | `git diff -- cmd/academic` rỗng (mục 5) |
| Ít nhất một tình huống lỗi | Worker chết giữa chừng; thông điệp sai hợp đồng vào dead-letter queue; subscriber tạm tắt rồi bật lại | Mục 2, 3, 4 |

## Kiến trúc

```
Kịch bản (i) – Message Queue (Point-to-Point)
                                                    ┌─► Finance Worker W1
Academic ──► default exchange ──► queue finance.work ┼─► Finance Worker W2     mỗi thông điệp → ĐÚNG MỘT worker
                                                    └─► Finance Worker W3

Kịch bản (ii) – Publish/Subscribe
                                        ┌─► queue finance.events      ─► Finance
Academic ──► exchange academic.events ──┼─► queue notification.events ─► Notification   mỗi subscriber → MỘT BẢN SAO
            (topic, key student.enrolled)└─► queue analytics.events    ─► Analytics

Thông điệp bị từ chối ──► exchange academic.dlx (fanout) ──► queue academic.dead-letter
```

Thiết lập độ tin cậy dùng chung:

| Thiết lập | Ở đâu | Tác dụng |
|---|---|---|
| Queue durable, message persistent | `internal/mq` | Queue và thông điệp còn nguyên khi RabbitMQ khởi động lại |
| Publisher confirm | `mq.Publisher` | Academic chỉ báo "đã gửi" khi broker xác nhận đã nhận |
| Ack thủ công | `internal/consumer` | Consumer chết trước khi ack thì broker giao lại thông điệp |
| Prefetch = 1 | `internal/consumer` | Worker bận không bị dồn việc; worker rảnh nhận việc tiếp |
| Dead-letter queue | `internal/mq` | Thông điệp sai hợp đồng không bị mất, nằm chờ kiểm tra |

## Cấu trúc thư mục

```
docker-compose.yml        RabbitMQ + trang quản trị
data/enrollments.csv      12 lượt đăng ký giả lập
internal/event            hợp đồng thông điệp student.enrolled + kiểm tra trường bắt buộc
internal/mq               kết nối, khai báo queue/exchange, publish có xác nhận
internal/consumer         vòng lặp nhận: prefetch, ack/nack, ghi bằng chứng
internal/evidence         ghi/đọc nhật ký logs/*.jsonl
cmd/academic              Producer: gửi sự kiện (kịch bản i hoặc ii)
cmd/finance-worker        Consumer kịch bản (i)
cmd/finance-sub           Subscriber kịch bản (ii) – cũng là MẪU cho subscriber khác
cmd/verify                Đối chiếu log, in bảng kết quả, kết luận đạt/không đạt
```

## Cài đặt

Cần Docker và Go ≥ 1.22. **Mọi lệnh chạy từ thư mục gốc của dự án.**

```bash
docker compose up -d          # khởi động RabbitMQ
go mod download               # tải thư viện amqp091-go
go test ./...                 # kiểm thử đơn vị
```

Trang quản trị RabbitMQ: <http://localhost:15672>, tài khoản `demo` / mật khẩu `demo`.
Đổi địa chỉ broker bằng biến môi trường `AMQP_URL` (mặc định `amqp://demo:demo@localhost:5672/`).

## 1. Kịch bản (i): một queue, nhiều worker

Mở 3 terminal, mỗi terminal chạy một worker:

```bash
go run ./cmd/finance-worker -id W1
go run ./cmd/finance-worker -id W2
go run ./cmd/finance-worker -id W3
```

Terminal thứ 4 gửi 12 sự kiện, rồi đối chiếu:

```bash
go run ./cmd/academic -mode work
go run ./cmd/verify -scenario work
```

Kỳ vọng: mỗi worker xử lý khoảng 4 sự kiện, `Xử lý thành công đúng 1 lần: 12/12`, kết luận ĐẠT.

## 2. Lỗi: worker chết khi đang xử lý

```bash
go run ./cmd/finance-worker -id W1 -crash-on 2 -delay 3s   # W1 chết khi đang xử lý thông điệp thứ 2
go run ./cmd/finance-worker -id W2
go run ./cmd/finance-worker -id W3
go run ./cmd/academic -mode work
go run ./cmd/verify -scenario work
```

Kỳ vọng: thông điệp W1 đang xử lý dở được RabbitMQ giao lại cho W2 hoặc W3, có nhãn `[GIAO LẠI]` ở terminal.
`verify` hiện diễn biến dạng `W1:received → W2:received(giao lại) → W2:processed(giao lại)` và vẫn ĐẠT, vì sự kiện được xử lý thành công đúng một lần.

**Điểm cần hiểu khi vấn đáp:** nếu worker chết *sau khi đã xử lý xong nhưng trước khi gửi ack*, thông điệp cũng được giao lại và sẽ bị xử lý hai lần. RabbitMQ đảm bảo *ít nhất một lần* (at-least-once), không đảm bảo *đúng một lần*; muốn tránh xử lý trùng thì consumer phải kiểm tra `event_id` đã xử lý chưa (idempotent consumer).

## 3. Lỗi: thông điệp sai hợp đồng

```bash
go run ./cmd/academic -mode work -n 3 -bad
go run ./cmd/verify -scenario work
```

Kỳ vọng: thông điệp `ev-xxxx-bad` bị từ chối (`rejected`) và nằm trong queue `academic.dead-letter`.
Xem nội dung: trang quản trị → Queues → `academic.dead-letter` → Get messages.
Thông điệp này không được thử lại, vì thử lại một thông điệp sai định dạng bao nhiêu lần cũng vẫn sai.

## 4. Kịch bản (ii): Publish/Subscribe

Chạy các subscriber (mỗi cái một terminal), sau đó gửi và đối chiếu:

```bash
go run ./cmd/finance-sub
go run ./cmd/notification-sub     # bạn B viết, xem mục "Việc còn lại"
go run ./cmd/analytics-sub        # bạn C viết

go run ./cmd/academic -mode pubsub -n 5
go run ./cmd/verify -scenario pubsub
```

Kỳ vọng: cùng một `event_id` xuất hiện ở log của **mọi** subscriber; bảng của `verify` toàn dấu ✓.

**Lỗi: subscriber tạm tắt.** Dừng Notification (Ctrl+C), gửi thêm 5 sự kiện. Trên trang quản trị, queue `notification.events` có `Ready = 5`. Bật lại Notification: nó nhận đủ 5 sự kiện, chạy `verify` lại sẽ ĐẠT.
(Queue phải đã được tạo, tức subscriber đã chạy ít nhất một lần trước đó.)

## 5. Thêm subscriber mới mà không sửa publisher

1. Chép `cmd/finance-sub` sang `cmd/<tên-mới>` (ví dụ `cmd/audit-sub`), đổi `Service`, `Queue` và nội dung hàm xử lý.
2. Chạy subscriber mới, rồi `go run ./cmd/academic -mode pubsub -n 3`.
3. Chạy `git diff --stat -- cmd/academic internal/mq` để chứng minh publisher không đổi.

Subscriber mới nhận các sự kiện gửi **sau khi** nó bind queue. Sự kiện gửi trước đó không tới nó, vì lúc publish chưa có queue của nó; `verify` của lần gửi cũ sẽ hiện ✗ ở cột mới. Đây là đặc điểm của Pub/Sub, không phải lỗi.

## Việc còn lại (theo phân công)

| Người | Việc |
|---|---|
| Bạn B | `cmd/notification-sub`: chép từ `cmd/finance-sub`, queue `notification.events`, in nội dung thông báo gửi sinh viên |
| Bạn C | `cmd/analytics-sub`: queue `analytics.events`, đếm số lượt đăng ký theo học phần và in bảng thống kê |

Mẫu tối thiểu cho một subscriber:

```go
err := consumer.Run(consumer.Options{
    Service:    "notification",
    Queue:      "notification.events",
    BindingKey: event.TypeStudentEnrolled,
}, func(ev event.StudentEnrolled) error {
    log.Printf("  gửi thông báo tới SV %s: đăng ký %s thành công", ev.StudentID, ev.CourseID)
    return nil // nil = xử lý xong → ack
})
```

Ack, nack, prefetch và ghi bằng chứng nằm trong `internal/consumer/consumer.go` (khoảng 200 dòng). Cả nhóm nên đọc file này để trả lời được câu hỏi về ack.

## Bằng chứng và chạy lại từ đầu

- Mỗi tiến trình ghi `logs/<service>[-<instance>].jsonl`. Mỗi lần gửi có mã riêng nằm trong event ID (`ev-<mã>-<stt>`). `verify` mặc định kiểm tra lần gửi gần nhất; dùng `-run <mã>` để chọn lần khác.
- Xóa sạch để chạy lại: `docker compose down -v` (xóa queue và thông điệp), `rm -rf logs`.

## Công cụ hỗ trợ

Khung mã ban đầu được tạo với sự hỗ trợ của Claude Code (Anthropic). Nhóm tự chạy kiểm thử, đọc hiểu và chỉnh sửa toàn bộ mã nộp.
