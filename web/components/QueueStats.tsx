import type { QueueInfo } from "@/lib/types";

/** Số liệu một queue, lấy từ RabbitMQ Management API qua Dashboard API. */
export function QueueStats({ name, q }: { name: string; q?: QueueInfo }) {
  return (
    <div className="qstats">
      <code>{name}</code>
      {q ? (
        <>
          <span className="stat" title="Thông điệp đang chờ trong queue">
            Ready <b>{q.messages_ready}</b>
          </span>
          <span className="stat" title="Đã giao cho consumer nhưng chưa được ack">
            Unacked <b>{q.messages_unacknowledged}</b>
          </span>
          <span className={q.consumers === 0 ? "stat warn" : "stat"} title="Số consumer đang kết nối">
            Consumer <b>{q.consumers}</b>
          </span>
        </>
      ) : (
        <span className="muted">chưa có queue</span>
      )}
    </div>
  );
}
