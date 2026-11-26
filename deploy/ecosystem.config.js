// Cấu hình pm2 trên VPS: 9 tiến trình Go + web Next.js, cùng namespace "thht27".
//
//   pm2 startOrReload deploy/ecosystem.config.js --update-env   khởi động / cập nhật
//   pm2 ls --namespace thht27                                     xem trạng thái
//   pm2 stop thht27-notification                                  tắt tạm một subscriber (để trình diễn)
//   pm2 logs thht27-W1                                            xem log một worker
//
// Mọi tên app có tiền tố "thht27-" để không đụng tới app của dự án khác trên cùng VPS.
// Mật khẩu và cổng đọc từ file .env ở thư mục gốc (do scripts/deploy-remote.sh tạo, không có trên git).

const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");

function loadEnv(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/.exec(line);
    if (m) out[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
  return out;
}

const env = loadEnv(path.join(root, ".env"));
const user = env.RABBITMQ_USER || "demo";
const pass = env.RABBITMQ_PASS || "demo";
const academicAddr = env.ACADEMIC_ADDR || "127.0.0.1:18027";
const dashboardAddr = env.DASHBOARD_ADDR || "127.0.0.1:18028";

const common = {
  AMQP_URL: `amqp://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@127.0.0.1:${env.AMQP_PORT || 5672}/`,
  MGMT_URL: `http://127.0.0.1:${env.MGMT_PORT || 15672}`,
  MGMT_USER: user,
  MGMT_PASS: pass,
  LOG_DIR: path.join(root, "logs"),
};

// Một chương trình Go đã build sẵn trong bin/. pm2 tự chạy lại khi tiến trình chết
// (kể cả khi consumer thoát vì mất kết nối RabbitMQ) sau 3 giây.
const goApp = (name, bin, args = []) => ({
  name: `thht27-${name}`,
  namespace: "thht27",
  cwd: root,
  script: path.join(root, "bin", bin),
  args,
  interpreter: "none",
  env: common,
  autorestart: true,
  restart_delay: 3000,
  time: true,
});

module.exports = {
  apps: [
    goApp("academic", "academic", ["-serve", academicAddr]),
    goApp("dashboard", "dashboard", [
      "-addr", dashboardAddr,
      "-academic", `http://${academicAddr}`,
      // Trang công khai trên Internet: giới hạn để không bị spam.
      "-max-n", "50",
      "-send-per-minute", "30",
    ]),
    goApp("W1", "finance-worker", ["-id", "W1"]),
    goApp("W2", "finance-worker", ["-id", "W2"]),
    goApp("W3", "finance-worker", ["-id", "W3"]),
    goApp("finance", "finance-sub"),
    goApp("notification", "notification-sub"),
    goApp("analytics", "analytics-sub"),
    goApp("game", "game-sub"),
    {
      name: "thht27-web",
      namespace: "thht27",
      cwd: path.join(root, "web-dist"),
      script: "server.js",
      env: {
        NODE_ENV: "production",
        PORT: env.WEB_PORT || "3027",
        HOSTNAME: env.WEB_HOST || "0.0.0.0",
        DASHBOARD_URL: `http://${dashboardAddr}`,
      },
      autorestart: true,
      restart_delay: 3000,
      max_memory_restart: "300M",
      time: true,
    },
  ],
};
