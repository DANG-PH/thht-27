#!/usr/bin/env bash
# Khởi động toàn bộ demo (Linux/macOS/Git Bash): RabbitMQ, Academic (HTTP), Dashboard API,
# 3 Finance Worker và 4 subscriber. Chạy lại lệnh này để bật lại những tiến trình đã dừng.
# Web Next.js chạy riêng: cd web && npm install && npm run dev
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bin .run

docker compose up -d --wait
go build -o bin/ ./cmd/...

start() {
	local name=$1
	shift
	if [ -f ".run/$name.pid" ] && kill -0 "$(cat ".run/$name.pid")" 2>/dev/null; then
		echo "  $name đang chạy"
		return
	fi
	nohup "$@" >>".run/$name.out" 2>&1 &
	echo $! >".run/$name.pid"
	echo "  $name (pid $!) → log ở .run/$name.out"
}

start academic bin/academic -serve 127.0.0.1:8081
start dashboard bin/dashboard
for w in W1 W2 W3; do start "$w" bin/finance-worker -id "$w"; done
start finance bin/finance-sub
start notification bin/notification-sub
start analytics bin/analytics-sub
start game bin/game-sub

cat <<'EOF'

Web:                         cd web && npm install && npm run dev   →  http://localhost:3000
Trang quản trị RabbitMQ:     http://localhost:15672  (demo / demo)
Tắt tạm một subscriber:      scripts/stop.sh notification
Giả lập worker chết:         gửi ở kịch bản (i), rồi ngay lập tức  scripts/stop.sh -9 W1
Bật lại phần đã tắt:         scripts/start-all.sh
Dừng tất cả:                 scripts/stop.sh
EOF
