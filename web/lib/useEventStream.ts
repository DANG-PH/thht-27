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
      return { ...state, records: action.records.slice(-MAX_RECORDS) };
    case "records":
      return { ...state, records: [...state.records, ...action.records].slice(-MAX_RECORDS) };
    case "reset":
      return { ...state, records: [] };
    case "queues":
      return { ...state, queues: action.payload.queues ?? [], queuesError: action.payload.error };
  }
}

export function useEventStream(apiBase: string) {
  const [state, dispatch] = useReducer(reducer, { conn: "connecting", records: [], queues: [] });

  useEffect(() => {
    const es = new EventSource(`${apiBase}/api/stream`);
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

    es.onopen = () => dispatch({ type: "conn", conn: "open" });
    es.onerror = () => dispatch({ type: "conn", conn: "error" }); // EventSource tự kết nối lại
    es.addEventListener("snapshot", (e) => {
      pending = [];
      dispatch({ type: "snapshot", records: data(e) as EvRecord[] });
    });
    es.addEventListener("record", (e) => {
      pending.push(data(e) as EvRecord);
      if (!timer) timer = setTimeout(flush, 80);
    });
    es.addEventListener("reset", () => {
      pending = [];
      dispatch({ type: "reset" });
    });
    es.addEventListener("queues", (e) => dispatch({ type: "queues", payload: data(e) as QueuesPayload }));

    return () => {
      es.close();
      if (timer) clearTimeout(timer);
    };
  }, [apiBase]);

  return state;
}
