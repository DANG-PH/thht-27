// Chuyển tiếp danh mục sinh viên / học phần từ Dashboard API (→ Academic).

export const dynamic = "force-dynamic";

const DASHBOARD = process.env.DASHBOARD_URL ?? "http://127.0.0.1:8080";

export async function GET() {
  try {
    const upstream = await fetch(`${DASHBOARD}/api/catalog`, { cache: "no-store" });
    return new Response(await upstream.text(), {
      status: upstream.status,
      headers: { "Content-Type": "application/json; charset=utf-8" },
    });
  } catch {
    return Response.json({ error: `Không kết nối được Dashboard API tại ${DASHBOARD}` }, { status: 502 });
  }
}
