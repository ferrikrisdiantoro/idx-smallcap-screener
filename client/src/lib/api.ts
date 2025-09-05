// src/lib/api.ts
import type { HealthResp, SnapshotResp, BrokerAggResp, SignalsResp, ExplainResp } from "@/types/api";

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

async function getJson<T>(path: string): Promise<T> {
  const url = `${BASE}${path}`;
  const res = await fetch(url, { cache: "no-store" });

  // Baca sebagai text dulu agar bisa diagnosa JSON invalid (HTML, NaN, dsb)
  const text = await res.text();

  if (!res.ok) {
    // error dari server, tampilkan potongan body untuk debugging
    const snippet = text.slice(0, 300);
    throw new Error(`${res.status} ${res.statusText} – ${snippet}`);
  }

  try {
    return JSON.parse(text) as T;
  } catch (e: any) {
    const snippet = text.slice(0, 300);
    throw new Error(`Invalid JSON from ${url}. First 300 chars:\n${snippet}`);
  }
}

export const apiHealth    = () => getJson<HealthResp>("/health");
export const apiSnapshot  = (date?: string) =>
  getJson<SnapshotResp>(date ? `/snapshot?date=${date}` : `/snapshot`);
export const apiBrokerAgg = (date?: string) =>
  getJson<BrokerAggResp>(date ? `/broker-agg?date=${date}` : `/broker-agg`);
export const apiSignals   = (from: string, to: string, threshold: number) =>
  getJson<SignalsResp>(`/signals?from=${from}&to=${to}&threshold=${threshold}`);

export function saveAs(csvContent: string, filename: string) {
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.setAttribute("download", filename);
  link.click();
  URL.revokeObjectURL(url);
}

export const apiExplain = (symbol: string, date?: string, threshold?: number) => {
  const p = new URLSearchParams({ symbol });
  if (date) p.set("date", date);
  if (threshold !== undefined) p.set("threshold", String(threshold));
  return getJson<ExplainResp>(`/explain?${p.toString()}`);
};
