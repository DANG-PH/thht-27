import { latestRun, trailsOfRun } from "@/lib/derive";
import type { EvRecord, QueueInfo } from "@/lib/types";
import { QueueStats } from "./QueueStats";

type ChipState = "done" | "working" | "moved" | "rejected" | "retry" | "dlq";

interface Chip {
  id: string;
  state: ChipState;
  redelivered: boolean;
  label: string;
  title: string;
}

const STATE_TEXT: Record<ChipState, string> = {
  done: "xử lý xong, đã ack",
  working: "đang xử lý, chưa ack",
  moved: "nhận nhưng không ack được (tiến trình chết?) → giao cho worker khác",
  rejected: "sai hợp đồng → dead-letter queue",
  retry: "lỗi xử lý → trả lại queue",
  dlq: "lỗi lặp lại → dead-letter queue",
};

/** Kịch bản (i): một queue finance.work, nhiều Finance Worker. Mỗi worker là một làn. */
export function WorkBoard({ records, queues }: { records: EvRecord[]; queues: Map<string, QueueInfo> }) {
  const run = latestRun(records, "work");
  const trails = run ? trailsOfRun(records, "work", run) : [];

  const lanes = new Map<string, Chip[]>();
  let normal = 0;
  let once = 0;
  let duplicated = 0;
  const redelivered: string[] = [];

  for (const t of trails) {
    const processed = t.steps.filter((s) => s.status === "processed");
    if (!t.malformed) {
      normal++;
      if (processed.length === 1) once++;
      if (processed.length > 1) duplicated++;
    }
    if (t.steps.some((s) => s.redelivered)) redelivered.push(t.id);

    const doneBy = processed.map((s) => s.instance);
    for (const inst of [...new Set(t.steps.map((s) => s.instance))]) {
      const mine = t.steps.filter((s) => s.instance === inst);
      const last = mine[mine.length - 1];
      let state: ChipState;
      if (mine.some((s) => s.status === "processed")) state = "done";
      else if (last.status === "rejected") state = "rejected";
      else if (last.status === "dead-lettered") state = "dlq";
      else if (last.status === "requeued") state = "retry";
      else if (doneBy.length > 0) state = "moved";
      else state = "working";

      const seq = t.id.split("-")[2] ?? t.id;
      const chip: Chip = {
        id: t.id,
        state,
        redelivered: mine.some((s) => s.redelivered),
        label: seq,
        title: `${t.id}${t.studentId ? ` · ${t.studentId} · ${t.courseId}` : ""}\n${inst}: ${STATE_TEXT[state]}${
          state === "moved" ? ` (${doneBy.join(", ")})` : ""
        }`,
      };
      if (!lanes.has(inst)) lanes.set(inst, []);
      lanes.get(inst)!.push(chip);
    }
  }
  const laneNames = [...lanes.keys()].sort();
  const pass = normal > 0 && once === normal && duplicated === 0;

  return (
    <section className="card">
      <header className="card-head">
        <h2>(i) Message Queue</h2>
        <p className="hint">
          Một queue, nhiều Finance Worker chia tải. Mỗi thông điệp chỉ tới <b>một</b> worker. Worker chết trước khi ack
          thì RabbitMQ giao lại thông điệp cho worker khác (↻).
        </p>
      </header>
      <QueueStats name="finance.work" q={queues.get("finance.work")} />
      {run ? (
        <p className="runline">
          <span>
            Lần gửi <code>{run}</code>: <b>{once}</b>/{normal} sự kiện được xử lý đúng một lần
            {duplicated > 0 && <span className="bad"> · {duplicated} sự kiện bị xử lý trùng</span>}
            {redelivered.length > 0 && <> · giao lại: {redelivered.map((id) => id.split("-")[2]).join(", ")}</>}
          </span>
          <span className={pass ? "verdict ok" : "verdict pending"}>{pass ? "ĐẠT" : "đang xử lý"}</span>
        </p>
      ) : (
        <p className="empty">Chưa có lần gửi nào ở kịch bản (i). Chọn kịch bản (i) rồi bấm gửi.</p>
      )}
      <div className="lanes">
        {laneNames.map((name) => {
          const chips = lanes.get(name)!;
          return (
            <div className="lane" key={name}>
              <div className="lane-name">
                {name}
                <span className="lane-count">{chips.filter((c) => c.state === "done").length}</span>
              </div>
              <div className="chips">
                {chips.map((c) => (
                  <span key={c.id} className={`chip ${c.state}`} title={c.title}>
                    {c.redelivered && "↻ "}
                    {c.label}
                  </span>
                ))}
              </div>
            </div>
          );
        })}
      </div>
      <div className="legend">
        <span className="chip done">xong</span>
        <span className="chip working">đang xử lý</span>
        <span className="chip moved">chết trước khi ack</span>
        <span className="chip rejected">sai hợp đồng → DLQ</span>
      </div>
    </section>
  );
}
