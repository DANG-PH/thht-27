// Kiểu dữ liệu nhận từ Dashboard API. EvRecord khớp với evidence.Record bên Go.

export type Status =
  | "published"
  | "received"
  | "processed"
  | "rejected"
  | "requeued"
  | "dead-lettered";

export interface EvRecord {
  ts: string;
  service: string;
  instance: string;
  scenario: string; // "work" | "pubsub"
  queue?: string;
  event_id: string;
  status: Status;
  redelivered?: boolean;
  note?: string;
  student_id?: string;
  course_id?: string;
}

export interface QueueInfo {
  name: string;
  messages_ready: number;
  messages_unacknowledged: number;
  consumers: number;
}

export interface QueuesPayload {
  queues: QueueInfo[];
  error?: string;
}

export type ConnState = "connecting" | "open" | "error";
