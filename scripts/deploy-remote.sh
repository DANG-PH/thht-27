#!/usr/bin/env bash
# Chạy TRÊN VPS: áp bản release đã build sẵn (bởi GitHub Actions hoặc `make deploy`) rồi khởi động lại bằng pm2.
#
#   bash scripts/deploy-remote.sh .deploy-tmp/release.tar.gz
#
# Không build gì trên VPS: VPS dùng chung RAM với nhiều dịch vụ khác, build ở đây dễ chết giữa chừng.
set -euo pipefail
cd "$(dirname "$0")/.."
TARBALL=${1:?Thiếu đường dẫn tới release.tar.gz}

# Phiên SSH không tương tác có thể không nạp nvm – nạp tay nếu node/pm2 cài qua nvm.
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"

# 1. .env: tạo MỘT lần với mật khẩu RabbitMQ ngẫu nhiên. File nằm trong .gitignore
#    nên `git reset --hard` khi deploy không đụng tới. Tuyệt đối không dùng `git clean`.
if [ ! -f .env ]; then
	pass=$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)
	cat >.env <<EOF
RABBITMQ_USER=thht27
RABBITMQ_PASS=$pass
AMQP_PORT=5672
MGMT_PORT=15672
ACADEMIC_ADDR=127.0.0.1:18027
DASHBOARD_ADDR=127.0.0.1:18028
WEB_PORT=3027
WEB_HOST=0.0.0.0
EOF
	chmod 600 .env
	echo "Đã tạo .env với mật khẩu RabbitMQ ngẫu nhiên"
fi
set -a
. ./.env
set +a

# 2. Giải nén vào thư mục tạm trước, xong mới thay bin/ và web-dist/: tarball hỏng thì bản cũ vẫn còn.
rm -rf .release-new
mkdir .release-new
tar -xzf "$TARBALL" -C .release-new
test -x .release-new/bin/academic
test -f .release-new/web-dist/server.js
rm -rf bin web-dist
mv .release-new/bin bin
mv .release-new/web-dist web-dist
rm -rf .release-new
rm -f "$TARBALL"
rmdir .deploy-tmp 2>/dev/null || true

# 3. RabbitMQ (chỉ container của dự án này).
docker compose up -d --wait

# 4. pm2: chỉ khởi động / cập nhật app của dự án này (namespace thht27), KHÔNG `pm2 restart all`.
pm2 startOrReload deploy/ecosystem.config.js --update-env
pm2 save >/dev/null

# 5. Kiểm tra sống: thử tới 60 giây.
for i in $(seq 1 30); do
	if curl -fsS "http://127.0.0.1:${WEB_PORT}/" -o /dev/null &&
		curl -fsS "http://${DASHBOARD_ADDR}/api/health" -o /dev/null; then
		echo "Deploy xong: web ở cổng ${WEB_PORT}, commit $(git rev-parse --short HEAD)"
		pm2 ls --namespace thht27 2>/dev/null || pm2 ls
		exit 0
	fi
	[ "$i" = 30 ] && break
	sleep 2
done
echo "Web hoặc Dashboard không phản hồi sau 60 giây"
pm2 ls
exit 1
