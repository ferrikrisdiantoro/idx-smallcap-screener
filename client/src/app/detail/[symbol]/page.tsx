"use client";

import { useEffect, useState } from "react";
import { useSearchParams, useParams } from "next/navigation";
import { apiExplain } from "@/lib/api";
import type { ExplainResp } from "@/types/api";
import Link from "next/link";

export default function DetailPage() {
  const params = useParams<{ symbol: string }>();
  const search = useSearchParams();
  const date = search.get("date") || undefined;

  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<ExplainResp | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    async function run() {
      try {
        setLoading(true);
        const res = await apiExplain(params.symbol, date);
        setData(res);
        setErr(null);
      } catch (e: any) {
        setErr(e?.message ?? "Gagal memuat detail.");
      } finally {
        setLoading(false);
      }
    }
    void run();
  }, [params.symbol, date]);

  return (
    <main className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="h1">Detail Sinyal: {params.symbol}</h1>
        <Link href="/" className="btn-secondary">← Kembali</Link>
      </div>

      {loading ? (
        <div className="card card-pad">Memuat…</div>
      ) : err ? (
        <div className="card card-pad text-rose-700">{err}</div>
      ) : data ? (
        <>
          <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="card card-pad">
              <div className="text-xs font-medium text-slate-600">Tanggal</div>
              <div className="mt-1 text-2xl font-semibold">{data.date}</div>
              <div className="mt-1 text-xs text-slate-500">as-of snapshot</div>
            </div>
            <div className="card card-pad">
              <div className="text-xs font-medium text-slate-600">Keputusan</div>
              <div className="mt-1 text-2xl font-semibold">{data.reason_simple}</div>
            </div>
            <div className="card card-pad">
              <div className="text-xs font-medium text-slate-600">Prob Up / Threshold</div>
              <div className="mt-1 text-2xl font-semibold">
                {data.prob_up.toFixed(2)} / {data.threshold_used.toFixed(2)}
              </div>
              <div className="mt-1 text-xs text-slate-500">target: naik ≥5%</div>
            </div>
            <div className="card card-pad">
              <div className="text-xs font-medium text-slate-600">Harga & Return 1D</div>
              <div className="mt-1 text-2xl font-semibold">
                {Number(data.close).toLocaleString("id-ID")} {" "}
                <span className={data.ret_1 <= -0.05 ? "text-rose-700" : data.ret_1 >= 0.05 ? "text-emerald-700" : "text-slate-600"}>
                  ({(data.ret_1*100).toFixed(1)}%)
                </span>
              </div>
            </div>
          </section>

          <section className="card card-pad">
            <div className="text-sm font-semibold mb-2">Penjelasan Ringkas</div>
            <ul className="list-disc pl-5 space-y-1">
              {data.bullets.map((b, i) => <li key={i}>{b}</li>)}
            </ul>
            <div className="mt-4 grid gap-4 sm:grid-cols-3">
              <div>
                <div className="text-xs font-medium text-slate-600">Vol Ratio</div>
                <div className="text-lg">{data.vol_ratio.toFixed(2)}×</div>
              </div>
              <div>
                <div className="text-xs font-medium text-slate-600">Top Buyer</div>
                <div className="text-lg">{data.top_buyer ?? "—"}</div>
              </div>
              <div>
                <div className="text-xs font-medium text-slate-600">Buyer Conc.</div>
                <div className="text-lg">{(data.top_buyer_concentration*100).toFixed(1)}%</div>
              </div>
            </div>
          </section>
        </>
      ) : null}
    </main>
  );
}
