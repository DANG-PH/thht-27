// Chuyển tiếp yêu cầu gửi từ trình duyệt sang Dashboard API → Academic.

export const dynamic = "force-dynamic";

const DASHBOARD = process.env.DASHBOARD_URL ?? "http://127.0.0.1:8080";

export async function POST(req: Request) {
  const body = await req.text();
  if (body.length > 4096) {
    return Response.json({ error: "body quá lớn" }, { status: 413 });
  }
  try {
    const upstream = await fetch(`${DASHBOARD}/api/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      cache: "no-store",
    });
    return new Response(await upstream.text(), {
      status: upstream.status,
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });
  } catch {
    return Response.json({ error: `Không kết nối được Dashboard API tại ${DASHBOARD}` }, { status: 502 });
  }
}
