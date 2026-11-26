# Lệnh tắt cho dự án. Gõ `make` để xem danh sách.
SHELL := /bin/bash

# Deploy thủ công từ máy cá nhân: make deploy VPS=root@<ip>
VPS ?=
SSH_KEY ?= ~/.ssh/id_ed25519_vps
APP_PATH ?= /root/thht-27
SSH := ssh -i $(SSH_KEY) -o BatchMode=yes
SCP := scp -i $(SSH_KEY) -o BatchMode=yes

.PHONY: help test build web release run stop e2e deploy

help: ## Liệt kê các lệnh
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-9s %s\n", $$1, $$2}'

test: ## go vet + go test + kiểm tra kiểu TypeScript của web
	go vet ./...
	go test ./...
	cd web && npm ci --no-audit --no-fund && npm run typecheck

build: ## Build các chương trình Go vào bin/ (cho máy đang dùng)
	go build -o bin/ ./cmd/...

web: ## Build web Next.js (bản tự chứa, ở web/.next/standalone)
	cd web && npm ci --no-audit --no-fund && npm run build

release: web ## Đóng gói release.tar.gz chạy trên VPS Linux amd64
	rm -rf .release && mkdir -p .release/bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o .release/bin/ ./cmd/...
	cp -r web/.next/standalone .release/web-dist
	cp -r web/.next/static .release/web-dist/.next/static
	tar -czf release.tar.gz -C .release .
	@echo "Đã tạo release.tar.gz ($$(du -h release.tar.gz | cut -f1))"

run: build ## Chạy toàn bộ demo trên máy (Linux/macOS); web: cd web && npm run dev
	scripts/start-all.sh

stop: ## Dừng demo trên máy
	scripts/stop.sh

e2e: ## Kiểm thử tích hợp T1–T9 với RabbitMQ thật (xóa dữ liệu demo)
	scripts/e2e.sh

deploy: release ## Deploy thủ công lên VPS: make deploy VPS=root@<ip>
	@test -n "$(VPS)" || { echo "Thiếu VPS, ví dụ: make deploy VPS=root@1.2.3.4"; exit 1; }
	$(SSH) $(VPS) 'test -d $(APP_PATH)/.git || git clone https://github.com/DANG-PH/thht-27.git $(APP_PATH); mkdir -p $(APP_PATH)/.deploy-tmp'
	$(SCP) release.tar.gz $(VPS):$(APP_PATH)/.deploy-tmp/release.tar.gz
	$(SSH) $(VPS) 'set -e; cd $(APP_PATH); git fetch -q origin main; git reset -q --hard origin/main; bash scripts/deploy-remote.sh .deploy-tmp/release.tar.gz'
