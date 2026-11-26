#!/usr/bin/env bash
# Dừng các tiến trình demo đã khởi động bằng scripts/start-all.sh.
#
#   scripts/stop.sh                 dừng tất cả (RabbitMQ vẫn chạy)
#   scripts/stop.sh notification    dừng một tiến trình, nhẹ nhàng như nhấn Ctrl+C
#   scripts/stop.sh -9 W1           giết đột ngột: giả lập tiến trình chết, không kịp ack
set -uo pipefail
cd "$(dirname "$0")/.."

sig=TERM
if [ "${1:-}" = "-9" ]; then
	sig=KILL
	shift
fi

names=("$@")
if [ ${#names[@]} -eq 0 ]; then
	for f in .run/*.pid; do
		[ -e "$f" ] && names+=("$(basename "$f" .pid)")
	done
fi

for n in "${names[@]}"; do
	f=".run/$n.pid"
	if [ ! -f "$f" ]; then
		echo "  không thấy tiến trình $n"
		continue
	fi
	pid=$(cat "$f")
	if kill -s "$sig" "$pid" 2>/dev/null; then
		echo "  đã dừng $n (pid $pid, SIG$sig)"
	else
		echo "  $n vốn không chạy"
	fi
	rm -f "$f"
done
