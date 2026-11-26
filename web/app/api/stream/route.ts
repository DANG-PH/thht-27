// Chuyển tiếp luồng Server-Sent Events từ Dashboard API (chỉ lắng nghe trong máy) ra trình duyệt.
// Nhờ vậy trình duyệt chỉ cần biết một địa chỉ là trang web, Dashboard không phải mở cổng ra ngoài.

export const dynamic = "force-dynamic";

const DASHBOARD = process.env.DASHBOARD_URL ?? "http://127.0.0.1:8080";

export async function GET(req: Request) {
  let upstream: Response;
  try {
    upstream = await fetch(`${DASHBOARD}/api/stream`, {
      signal: req.signal, // trình duyệt đóng trang → ngắt luôn kết nối tới Dashboard
      cache: "no-store",
      headers: { Accept: "text/event-stream" },
    });
  } catch {
    return Response.json({ error: `Không kết nối được Dashboard API tại ${DASHBOARD}` }, { status: 502 });
  }
  if (!upstream.ok || !upstream.body) {
    return new Response(await upstream.text(), { status: upstream.status || 502 });
  }
  return new Response(upstream.body, {
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "X-Accel-Buffering": "no",
    },
  });
}
