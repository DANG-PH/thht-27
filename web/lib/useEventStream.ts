"use client";

// Kết nối Server-Sent Events tới Dashboard API và giữ trạng thái trên trình duyệt.
// Trình duyệt KHÔNG kết nối RabbitMQ: mọi dữ liệu đi qua Dashboard API (Go).

import { useEffect, useReducer } from "react";
import type { ConnState, EvRecord, QueueInfo, QueuesPayload } from "./types";

const MAX_RECORDS = 3000;

interface State {
  conn: ConnState;
  records: EvRecord[];
  queues: QueueInfo[];
  queuesError?: string;
  // Tăng mỗi khi nhận lại toàn bộ lịch sử (snapshot/reset): sơ đồ luồng dựa vào đây để chỉ
  // vẽ gói tin chạy cho bản ghi MỚI, không vẽ lại cả lịch sử.
  version: number;
}

type Action =
  | { type: "conn"; conn: ConnState }
  | { type: "snapshot"; records: EvRecord[] }
  | { type: "records"; records: EvRecord[] }
  | { type: "reset" }
  | { type: "queues"; payload: QueuesPayload };

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "conn":
      return { ...state, conn: action.conn };
    case "snapshot":
      // Mỗi lần (kết nối lại) server gửi toàn bộ lịch sử → thay hẳn, không bị trùng.
      return { ...state, records: action.records.slice(-MAX_RECORDS), version: state.version + 1 };
    case "records":
      return { ...state, records: [...state.records, ...action.records].slice(-MAX_RECORDS) };
    case "reset":
      return { ...state, records: [], version: state.version + 1 };
    case "queues":
      return { ...state, queues: action.payload.queues ?? [], queuesError: action.payload.error };
  }
}

export function useEventStream(apiBase: string) {
  const [state, dispatch] = useReducer(reducer, { conn: "connecting", records: [], queues: [], version: 0 });

  useEffect(() => {
    let es: EventSource | undefined;
    let closed = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    // Gom các bản ghi tới dồn dập rồi cập nhật giao diện một lần, tránh vẽ lại liên tục.
    let pending: EvRecord[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;
    const flush = () => {
      timer = undefined;
      const batch = pending;
      pending = [];
      if (batch.length > 0) dispatch({ type: "records", records: batch });
    };
    const data = (e: Event) => JSON.parse((e as MessageEvent<string>).data);

    const connect = () => {
      const src = new EventSource(`${apiBase}/api/stream`);
      es = src;
      src.onopen = () => dispatch({ type: "conn", conn: "open" });
      src.onerror = () => {
        dispatch({ type: "conn", conn: "error" });
        // Lỗi mạng: EventSource tự thử lại. Nhưng nếu server trả lỗi HTTP (vd đang deploy lại),
        // EventSource đóng hẳn → tự mở kết nối mới sau 3 giây.
        if (src.readyState === EventSource.CLOSED && !closed) {
          retry = setTimeout(connect, 3000);
        }
      };
      src.addEventListener("snapshot", (e) => {
        pending = [];
        dispatch({ type: "snapshot", records: data(e) as EvRecord[] });
      });
      src.addEventListener("record", (e) => {
        pending.push(data(e) as EvRecord);
        if (!timer) timer = setTimeout(flush, 80);
      });
      src.addEventListener("reset", () => {
        pending = [];
        dispatch({ type: "reset" });
      });
      src.addEventListener("queues", (e) => dispatch({ type: "queues", payload: data(e) as QueuesPayload }));
    };
    connect();

    return () => {
      closed = true;
      es?.close();
      if (timer) clearTimeout(timer);
      if (retry) clearTimeout(retry);
    };
  }, [apiBase]);

  return state;
}
