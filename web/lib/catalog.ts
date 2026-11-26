"use client";

// Danh mục sinh viên / học phần lấy từ Academic (qua /api/catalog) và các hàm định dạng dùng chung.

import { useEffect, useState } from "react";

export interface Student {
  id: string;
  name: string;
}

export interface Course {
  id: string;
  name: string;
  credits: number;
  fee: number;
}

export interface Catalog {
  semester: string;
  price_per_credit: number;
  students: Student[];
  courses: Course[];
}

export function useCatalog(api: string) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  useEffect(() => {
    let stop = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = async () => {
      try {
        const res = await fetch(`${api}/api/catalog`, { cache: "no-store" });
        if (!res.ok) throw new Error(String(res.status));
        const data = (await res.json()) as Catalog;
        if (!stop) setCatalog(data);
      } catch {
        if (!stop) timer = setTimeout(load, 3000); // Academic chưa chạy → thử lại
      }
    };
    load();
    return () => {
      stop = true;
      if (timer) clearTimeout(timer);
    };
  }, [api]);
  return catalog;
}

const vnd = new Intl.NumberFormat("vi-VN");

/** 1350000 → "1.350.000 đ" */
export function money(n: number): string {
  return `${vnd.format(n)} đ`;
}

/** Học phí theo số tín chỉ – cùng quy tắc với internal/tuition bên Go. */
export function feeOf(catalog: Catalog | null, courseId?: string): number {
  const c = catalog?.courses.find((x) => x.id === courseId);
  return c ? c.fee : 0;
}

export function studentName(catalog: Catalog | null, id?: string): string {
  return catalog?.students.find((s) => s.id === id)?.name ?? id ?? "";
}

export function courseOf(catalog: Catalog | null, id?: string): Course | undefined {
  return catalog?.courses.find((c) => c.id === id);
}

/** "Nguyễn Văn An" → "AN" (2 chữ cái cuối tên, dùng làm ảnh đại diện). */
export function initials(name: string): string {
  const parts = name.trim().split(/\s+/);
  const last = parts[parts.length - 1] ?? "";
  const prev = parts.length > 1 ? parts[parts.length - 2] : "";
  return ((prev[0] ?? "") + (last[0] ?? "")).toUpperCase();
}
