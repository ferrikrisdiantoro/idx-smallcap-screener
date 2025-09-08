"use client";
import { useMemo, useState } from "react";
import Link from "next/link";

export type SignalRow = {
  tanggal: string;
  saham: string;
  sinyal: "BELI" | "JUAL KUAT" | "—";
  harga: number;
  harga_now: number;
  kenaikan_pct: number;
  akumulasi_pct: number;
  distribusi_pct: number;
  alasan: string;
  top_buyer?: string | null;
};

function Badge({ type }: { type: SignalRow["sinyal"] }) {
  if (type === "BELI") return <span className="badge badge-green">BELI</span>;
  if (type === "JUAL KUAT") return <span className="badge badge-red">JUAL KUAT</span>;
  return <span className="badge badge-gray">-</span>;
}

export default function SignalsTable({ rows, loading }: { rows: SignalRow[]; loading: boolean }) {
  const [q, setQ] = useState("");

  const filtered = useMemo(() => {
    const key = q.trim().toLowerCase();
    if (!key) return rows;
    return rows.filter((r) =>
      r.saham.toLowerCase().includes(key) ||
      r.alasan.toLowerCase().includes(key) ||
      String(r.top_buyer ?? "").toLowerCase().includes(key)
    );
  }, [rows, q]);

  const headers: Array<{ label: string; className?: string }> = [
    { label: "Tanggal", className: "w-[110px]" },
    { label: "Saham", className: "w-[90px]" },
    { label: "Sinyal", className: "w-[110px]" },
    { label: "Harga", className: "w-[110px]" },
    { label: "Harga Saat Ini", className: "w-[120px]" },
    { label: "Kenaikan (%)", className: "w-[120px]" },
    { label: "Akumulasi (%)", className: "w-[120px]" },
    { label: "Distribusi (%)", className: "w-[120px]" },
    { label: "Alasan Pemicu Sinyal" },
  ];

  return (
    <section className="card card-pad">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="text-sm font-semibold">Daftar Sinyal</div>
        <div className="flex items-center gap-2">
          <input
            placeholder="Cari saham / alasan / broker…"
            className="input w-64"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="table">
          <thead>
            <tr>
              {headers.map((h, i) => (
                <th key={i} className={`th ${h.className ?? ""}`.trim()}>{h.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td className="td" colSpan={headers.length}>Sedang memproses…</td>
              </tr>
            ) : filtered.length === 0 ? (
              <tr>
                <td className="td" colSpan={headers.length}>Tidak ada data.</td>
              </tr>
            ) : (
              filtered.map((r, i) => (
                <tr key={`${r.tanggal}-${r.saham}-${i}`} className="hover:bg-slate-50">
                  <td className="td">{r.tanggal}</td>
                  <td className="td font-medium">
                    <Link
                      href={`/detail/${r.saham}?date=${encodeURIComponent(r.tanggal)}`}
                      className="text-emerald-700 hover:underline"
                    >
                      {r.saham}
                    </Link>
                  </td>
                  <td className="td"><Badge type={r.sinyal} /></td>
                  <td className="td td-num">{Number(r.harga ?? 0).toLocaleString("id-ID")}</td>
                  <td className="td td-num">{Number(r.harga_now ?? 0).toLocaleString("id-ID")}</td>
                  <td
                    className={`td td-num ${
                      (r.kenaikan_pct ?? 0) > 0 ? "text-emerald-700" :
                      (r.kenaikan_pct ?? 0) < 0 ? "text-rose-700" : ""
                    }`}
                  >
                    {Number(r.kenaikan_pct ?? 0).toFixed(1)}%
                  </td>
                  <td className="td td-num">{Number(r.akumulasi_pct ?? 0).toFixed(1)}%</td>
                  <td className="td td-num">{Number(r.distribusi_pct ?? 0).toFixed(1)}%</td>
                  <td className="td text-slate-700">{r.alasan}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="mt-3 text-xs text-slate-500">
        Total baris: {filtered.length.toLocaleString("id-ID")}
      </div>
    </section>
  );
}
