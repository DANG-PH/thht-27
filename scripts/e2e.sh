#!/usr/bin/env bash
# Chạy toàn bộ kiểm thử tích hợp T1–T9 với RabbitMQ thật và lưu minh chứng vào docs/evidence/.
# Dùng trên Linux/macOS (hoặc Git Bash trên Windows). Cần Docker, Go, curl, python3.
#
#   scripts/e2e.sh
#
# CẢNH BÁO: script xóa sạch dữ liệu RabbitMQ của demo (docker compose down -v) và thư mục logs/.
set -uo pipefail
cd "$(dirname "$0")/.."

EVID=docs/evidence
RUN=.run
RABBIT=nmtht-rabbitmq
mkdir -p "$EVID" "$RUN" bin

# Không chạy chồng lên demo đang chạy bằng scripts/start-all.sh (script này xóa dữ liệu RabbitMQ).
for f in "$RUN"/*.pid; do
	if [ -e "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then
		echo "Đang có tiến trình demo chạy ($(basename "$f" .pid)). Chạy scripts/stop.sh trước rồi thử lại."
		exit 1
	fi
done
rm -f "$EVID"/*.txt

declare -a PIDS=() RESULTS=()

title() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# start <tên> <lệnh...>: chạy nền, output vào .run/<tên>.out
start() {
	local name=$1
	shift
	"$@" >"$RUN/$name.out" 2>&1 &
	echo $! >"$RUN/$name.pid"
	PIDS+=($!)
}

# stop <tên...>: dừng nhẹ nhàng bằng SIGTERM (giống nhấn Ctrl+C)
stop() {
	local name pid
	for name in "$@"; do
		pid=$(cat "$RUN/$name.pid" 2>/dev/null) || continue
		kill "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	done
}

cleanup() {
	for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
	wait 2>/dev/null
}
trap cleanup EXIT

# send <tham số academic...>: gửi, in ra mã lần gửi
send() {
	bin/academic "$@" 2>&1 | tee -a "$RUN/academic.out" | sed -n 's/.*lần gửi \([0-9a-f]\{4\}\):.*/\1/p'
}

# queues: bảng số thông điệp từng queue, lấy trực tiếp từ broker
queues() {
	docker exec "$RABBIT" rabbitmqctl list_queues -q name messages_ready messages_unacknowledged consumers | column -t
}

# queue_field <queue> <cột>: một giá trị của một queue (cột: messages_ready, consumers, ...)
queue_field() {
	docker exec "$RABBIT" rabbitmqctl list_queues -q name "$2" | awk -v q="$1" '$1 == q { print $2 }'
}

# wait_consumers <queue> <n>: chờ tối đa 20 giây tới khi queue có đủ n consumer
wait_consumers() {
	local i
	for ((i = 0; i < 40; i++)); do
		[ "$(queue_field "$1" consumers)" = "$2" ] && return 0
		sleep 0.5
	done
	echo "  (hết thời gian chờ $2 consumer trên $1)"
	return 1
}

# wait_verify <scenario> <run> <số giây> <file>: chạy verify tới khi ĐẠT hoặc hết giờ; ghi kết quả vào file
wait_verify() {
	local scenario=$1 run=$2 timeout=$3 out=$4 rc=1 i
	for ((i = 0; i < timeout * 4; i++)); do
		bin/verify -scenario "$scenario" -run "$run" >"$out.tmp" 2>&1
		rc=$?
		[ $rc -eq 0 ] && break
		sleep 0.25
	done
	{
		echo "\$ go run ./cmd/verify -scenario $scenario -run $run"
		cat "$out.tmp"
	} >>"$out"
	rm -f "$out.tmp"
	return $rc
}

# result <mã> <0|khác 0> <mô tả>
result() {
	local verdict="ĐẠT"
	[ "$2" -eq 0 ] || verdict="KHÔNG ĐẠT"
	RESULTS+=("$(printf '%-4s %-10s %s' "$1" "$verdict" "$3")")
	printf '  → %s: %s\n' "$1" "$verdict"
}

header() { # header <file> <mô tả>
	{
		echo "# $2"
		echo "# Chạy lúc: $(date '+%Y-%m-%d %H:%M:%S %Z')"
		echo
	} >"$1"
}

title "Chuẩn bị: RabbitMQ sạch, build các chương trình"
docker compose down -v >/dev/null 2>&1
docker compose up -d --wait
rm -rf logs "$RUN"/*.out "$RUN"/*.pid
go build -o bin/ ./cmd/... || exit 1

# ---------------------------------------------------------------------------
title "T1 – Kịch bản (i): 3 worker chia tải 12 sự kiện"
F=$EVID/T1-chia-tai.txt
header "$F" "T1 – Một queue finance.work, ba Finance Worker, 12 sự kiện"
for w in W1 W2 W3; do start "$w" bin/finance-worker -id "$w" -delay 300ms; done
wait_consumers finance.work 3
run=$(send -mode work)
wait_verify work "$run" 30 "$F"
result T1 $? "Một queue, 3 worker: mỗi sự kiện đúng một worker xử lý (lần gửi $run)"
stop W1 W2 W3

# ---------------------------------------------------------------------------
title "T2 – Lỗi: W1 chết khi đang xử lý thông điệp thứ 2"
F=$EVID/T2-worker-chet.txt
header "$F" "T2 – W1 chạy với -crash-on 2: chết khi đang xử lý thông điệp thứ 2, chưa ack"
start W1 bin/finance-worker -id W1 -delay 1s -crash-on 2
start W2 bin/finance-worker -id W2 -delay 1s
start W3 bin/finance-worker -id W3 -delay 1s
wait_consumers finance.work 3
run=$(send -mode work)
wait_verify work "$run" 40 "$F"
rc=$?
grep -q "Được RabbitMQ giao lại: ev-" "$F" || rc=1
{
	echo
	echo "--- Log của W1 (tiến trình chết giữa chừng):"
	cat "$RUN/W1.out"
	echo
	echo "--- Dòng [GIAO LẠI] ở W2/W3:"
	grep -h "GIAO LẠI" "$RUN/W2.out" "$RUN/W3.out"
} >>"$F"
result T2 $rc "Worker chết khi chưa ack: thông điệp được giao lại, vẫn xử lý đúng một lần (lần gửi $run)"
stop W1 W2 W3

# ---------------------------------------------------------------------------
title "T3 – Lỗi: thông điệp sai hợp đồng vào dead-letter queue"
F=$EVID/T3-sai-hop-dong.txt
header "$F" "T3 – Gửi 3 sự kiện hợp lệ và 1 thông điệp sai hợp đồng (-bad)"
start W2 bin/finance-worker -id W2 -delay 100ms
wait_consumers finance.work 1
run=$(send -mode work -n 3 -bad)
wait_verify work "$run" 20 "$F"
rc=$?
for _ in $(seq 1 20); do grep -qs "ev-$run-bad.*rejected" logs/*.jsonl && break; sleep 0.25; done
sleep 0.5
dlq=$(queue_field academic.dead-letter messages_ready)
[ "$dlq" = "1" ] || rc=1
{
	echo
	echo "--- Số thông điệp trong các queue (academic.dead-letter phải có 1):"
	queues
	echo
	echo "--- Nội dung thông điệp trong academic.dead-letter (đọc qua Management API, trả lại queue sau khi đọc):"
	curl -s -u demo:demo -H 'content-type: application/json' -X POST \
		-d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}' \
		"http://127.0.0.1:15672/api/queues/%2F/academic.dead-letter/get" |
		python3 -c 'import json,sys
m = json.load(sys.stdin)[0]
p = m["properties"]
d = p.get("headers", {}).get("x-death", [{}])[0]
print("message_id :", p.get("message_id"))
print("payload    :", m["payload"])
print("x-death    : queue=%s reason=%s count=%s" % (d.get("queue"), d.get("reason"), d.get("count")))'
} >>"$F"
result T3 $rc "Thông điệp sai hợp đồng bị từ chối, nằm trong academic.dead-letter (lần gửi $run)"
stop W2

# ---------------------------------------------------------------------------
title "T7 + T8 – Gửi khi chưa có worker, rồi khởi động lại RabbitMQ"
F=$EVID/T7-T8-ben-vung.txt
header "$F" "T7 – Gửi 5 sự kiện khi chưa có worker; T8 – khởi động lại RabbitMQ trước khi bật worker"
run=$(send -mode work -n 5)
sleep 1
before=$(queue_field finance.work messages_ready)
{
	echo "--- Sau khi gửi, chưa có worker nào (finance.work phải có 5 thông điệp Ready):"
	queues
} >>"$F"
docker compose restart rabbitmq >/dev/null 2>&1
docker compose up -d --wait >/dev/null 2>&1
after=$(queue_field finance.work messages_ready)
{
	echo
	echo "--- Sau khi khởi động lại RabbitMQ (vẫn phải còn 5):"
	queues
	echo
} >>"$F"
start W1 bin/finance-worker -id W1 -delay 100ms
wait_verify work "$run" 30 "$F"
rc=$?
[ "$before" = "5" ] && [ "$after" = "5" ] || rc=1
result T7 "$([ "$before" = "5" ] && echo 0 || echo 1)" "Gửi khi chưa có worker: thông điệp chờ trong queue (lần gửi $run)"
result T8 $rc "RabbitMQ khởi động lại: 5 thông điệp còn nguyên, worker xử lý đủ (lần gửi $run)"
stop W1

# ---------------------------------------------------------------------------
title "T9 – Thời gian xử lý 12 sự kiện: 1 worker so với 3 worker"
F=$EVID/T9-mo-rong.txt
header "$F" "T9 – 12 sự kiện, mỗi sự kiện xử lý 500ms; đo từ lúc bắt đầu gửi tới khi verify ĐẠT"
measure() { # measure <số worker>
	local n=$1 i t0 t1 r
	for ((i = 1; i <= n; i++)); do start "W$i" bin/finance-worker -id "W$i" -delay 500ms; done
	wait_consumers finance.work "$n" >/dev/null
	t0=$(date +%s.%N)
	r=$(send -mode work)
	echo "=== $n worker" >>"$F"
	wait_verify work "$r" 60 "$F" >/dev/null
	t1=$(date +%s.%N)
	for ((i = 1; i <= n; i++)); do stop "W$i"; done
	awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }'
}
t_one=$(measure 1)
t_three=$(measure 3)
printf '\nKẾT QUẢ ĐO: 1 worker = %s giây, 3 worker = %s giây\n' "$t_one" "$t_three" >>"$F"
result T9 "$(awk -v a="$t_one" -v b="$t_three" 'BEGIN { print (b < a ? 0 : 1) }')" "Thêm worker giảm thời gian xử lý: 1 worker ${t_one}s, 3 worker ${t_three}s"

# ---------------------------------------------------------------------------
title "T4 – Kịch bản (ii): Finance, Notification, Analytics mỗi bên một bản sao"
F=$EVID/T4-pubsub.txt
header "$F" "T4 – Publish 5 sự kiện lên exchange academic.events, ba subscriber"
start finance bin/finance-sub
start notification bin/notification-sub -delay 100ms
start analytics bin/analytics-sub
for q in finance.events notification.events analytics.events; do wait_consumers "$q" 1; done
run=$(send -mode pubsub -n 5)
wait_verify pubsub "$run" 20 "$F"
rc=$?
{
	echo
	echo "--- Cùng một event ID ev-$run-001 trong log của cả ba subscriber:"
	grep -h "ev-$run-001" "$RUN/finance.out" "$RUN/notification.out" "$RUN/analytics.out"
	echo
	echo "--- Binding của exchange academic.events (mỗi subscriber tự đăng ký queue của mình):"
	docker exec "$RABBIT" rabbitmqctl list_bindings -q source_name destination_name routing_key |
		awk '$1 == "academic.events"' | column -t
} >>"$F"
result T4 $rc "Pub/Sub: cùng một sự kiện tới cả ba subscriber (lần gửi $run)"

# ---------------------------------------------------------------------------
title "T5 – Notification tạm tắt rồi bật lại"
F=$EVID/T5-subscriber-tam-tat.txt
header "$F" "T5 – Tắt Notification, gửi 5 sự kiện, rồi bật lại"
stop notification
run=$(send -mode pubsub -n 5)
for _ in $(seq 1 20); do [ "$(queue_field notification.events messages_ready)" = "5" ] && break; sleep 0.25; done
waiting=$(queue_field notification.events messages_ready)
{
	echo "--- Khi Notification đang tắt (notification.events phải có 5 thông điệp Ready):"
	queues
	echo
} >>"$F"
start notification bin/notification-sub -delay 100ms
wait_verify pubsub "$run" 20 "$F"
rc=$?
[ "$waiting" = "5" ] || rc=1
result T5 $rc "Subscriber tạm tắt: queue riêng giữ 5 thông điệp, bật lại nhận đủ (lần gửi $run)"
prev=$run

# ---------------------------------------------------------------------------
title "T6 – Thêm Game subscriber, không sửa Academic"
F=$EVID/T6-them-subscriber.txt
header "$F" "T6 – Chạy thêm cmd/game-sub (subscriber thứ tư), gửi 3 sự kiện"
start game bin/game-sub
wait_consumers game.events 1
run=$(send -mode pubsub -n 3)
wait_verify pubsub "$run" 20 "$F"
rc=$?
{
	echo
	echo "--- Lần gửi TRƯỚC khi có Game ($prev): cột game phải là ✗ vì lúc đó chưa có queue game.events"
	echo "\$ go run ./cmd/verify -scenario pubsub -run $prev"
	bin/verify -scenario pubsub -run "$prev"
	echo "(mã thoát $? – 1 là đúng mong đợi)"
} >>"$F" 2>&1
bin/verify -scenario pubsub -run "$prev" >/dev/null 2>&1
[ $? -eq 1 ] || rc=1
result T6 $rc "Thêm subscriber mới không sửa publisher; nhận sự kiện mới, không nhận sự kiện cũ (lần gửi $run)"

stop finance notification game analytics
cp "$RUN/analytics.out" "$EVID/analytics-thong-ke.txt"

# ---------------------------------------------------------------------------
title "Kiểm thử đơn vị"
F=$EVID/unit-tests.txt
header "$F" "Kiểm thử đơn vị: go test ./..."
go test ./... >>"$F" 2>&1
result U $? "go test ./... (hợp đồng thông điệp, verify, dashboard)"

{
	echo "TÓM TẮT KIỂM THỬ – $(date '+%Y-%m-%d %H:%M:%S %Z')"
	echo "$(go version), RabbitMQ $(docker exec "$RABBIT" rabbitmqctl version)"
	echo
	printf '%s\n' "${RESULTS[@]}"
} | tee "$EVID/summary.txt"
