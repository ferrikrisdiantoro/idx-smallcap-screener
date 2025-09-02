import type { HealthResp, SnapshotResp, BrokerAggResp } from "@/types/api";

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText} – ${text}`);
  }
  return (await res.json()) as T;
}

export const apiHealth   = () => getJson<HealthResp>("/health");
export const apiSnapshot = (date?: string) =>
  getJson<SnapshotResp>(date ? `/snapshot?date=${date}` : `/snapshot`);
export const apiBrokerAgg = (date?: string) =>
  getJson<BrokerAggResp>(date ? `/broker-agg?date=${date}` : `/broker-agg`);

export type SignalsResp = {
  rows: {
    tanggal: string;
    saham: string;
    sinyal: "BELI" | "JUAL KUAT";
    harga: number;
    akumulasi_pct: number;
    distribusi_pct: number;
    alasan: string;
  }[];
  from: string;
  to: string;
  threshold: number;
};

export const apiSignals = (from: string, to: string, threshold: number) =>
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
