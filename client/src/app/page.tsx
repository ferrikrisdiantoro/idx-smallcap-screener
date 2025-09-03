// src/app/page.tsx
"use client";

import { useEffect, useMemo, useState } from "react";
import { apiHealth, apiSnapshot, apiBrokerAgg, apiSignals } from "@/lib/api";
import Clock from "@/components/Clock";
import Filters from "@/components/Filters";
import SignalsTable, { SignalRow } from "@/components/SignalsTable";
import MetricCard from "@/components/MetricCard";
import type { BrokerAggResp, SnapshotResp } from "@/types/api";

// alias tipe supaya tidak pakai `any`
type BrokerRow = BrokerAggResp["rows"][number];

export default function Page() {
  const [loading, setLoading] = useState(true);
  const [hasModel, setHasModel] = useState(false);
  const [target, setTarget] = useState<string | null>(null);
  const [snap, setSnap] = useState<SnapshotResp | null>(null);
  const [agg, setAgg] = useState<BrokerAggResp | null>(null);

  // filters
  const [dateFrom, setDateFrom] = useState<string>("");
  const [dateTo, setDateTo] = useState<string>("");
  const [broker, setBroker] = useState<string>("Semua Broker");
  const [priceCond, setPriceCond] = useState<"diatas" | "dibawah">("diatas");
  const [priceValue, setPriceValue] = useState<number | "">("");
  const [threshold, setThreshold] = useState<number>(0.35);

  const [activeTab, setActiveTab] = useState<"backtest" | "broker">("backtest");

  async function boot() {
    try {
      setLoading(true);
      const [h, s, a] = await Promise.all([apiHealth(), apiSnapshot(), apiBrokerAgg()]);
      setHasModel(h.has_model);
      setTarget(h.target);
      setThreshold(h.threshold_default ?? 0.35);
      setSnap(s);
      setAgg(a);
      if (s?.date) {
        setDateFrom(s.date);
        setDateTo(s.date);
      }
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    void boot();
  }, []);

  const brokerOpts = useMemo(() => {
    const buyers = new Set<string>();
    (agg?.rows ?? []).forEach((r: BrokerRow) => {
      if (r.top_buyer) buyers.add(String(r.top_buyer));
    });
    return ["Semua Broker", ...Array.from(buyers)];
  }, [agg]);

  const [rows, setRows] = useState<SignalRow[]>([]);
  const [busyApply, setBusyApply] = useState(false);

  async function applyFilters() {
    setBusyApply(true);
    try {
      const from = (dateFrom || dateTo || "").trim();
      const to = (dateTo || dateFrom || "").trim();
      if (!from || !to) {
        setRows([]);
        return;
      }

      const [sig, s, a] = await Promise.all([
        apiSignals(from, to, threshold),
        apiSnapshot(to),
        apiBrokerAgg(to),
      ]);
      setSnap(s);
      setAgg(a);

      let out = sig.rows as unknown as SignalRow[];
      if (broker !== "Semua Broker") {
        out = out.filter((r) => (r.top_buyer ?? "") === broker);
      }
      if (priceValue !== "") {
        out = out.filter((r) =>
          priceCond === "diatas"
            ? r.harga >= Number(priceValue)
            : r.harga <= Number(priceValue)
        );
      }
      setRows(out);
    } finally {
      setBusyApply(false);
    }
  }

  return (
    <main className="space-y-6">
      <section className="card card-pad">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <h1 className="h1">Hasil Backtesting Sinyal Bandar</h1>
            <p className="mt-1 text-sm muted">
              Menampilkan sinyal Beli &amp; Jual yang terdeteksi oleh skrip Python.
            </p>
            <p className="mt-1 text-sm">
              Data Terakhir Diperbarui Pada:{" "}
              <span className="font-medium">{snap?.date ?? "—"}</span>
            </p>
          </div>
          <div className="text-right">
            <div className="text-xs uppercase text-slate-500">Waktu Lokal</div>
            <div className="text-2xl font-semibold tabular-nums">
              <Clock />
            </div>
          </div>
        </div>

        <div className="mt-6 flex flex-wrap items-center gap-3 text-sm">
          <span className="muted">Backend:</span>
          <span className="badge badge-gray">FastAPI</span>
          <span className="muted">• Model target:</span>
          <span className="badge badge-gray">{target ?? "-"}</span>
          <span className="muted">• Status model:</span>
          <span className={`badge ${hasModel ? "badge-green" : "badge-red"}`}>
            {hasModel ? "READY" : "NOT LOADED"}
          </span>
        </div>
      </section>

      <div className="flex items-end gap-6 border-b border-slate-200">
        <button
          onClick={() => setActiveTab("backtest")}
          className={`pb-3 -mb-px border-b-2 font-medium ${
            activeTab === "backtest"
              ? "border-emerald-500 text-emerald-700"
              : "border-transparent text-slate-500 hover:text-slate-700"
          }`}
        >
          Hasil Backtesting
        </button>
        <button
          onClick={() => setActiveTab("broker")}
          className={`pb-3 -mb-px border-b-2 font-medium ${
            activeTab === "broker"
              ? "border-emerald-500 text-emerald-700"
              : "border-transparent text-slate-500 hover:text-slate-700"
          }`}
        >
          Prestasi Broker
        </button>
      </div>

      {activeTab === "backtest" ? (
        <>
          <Filters
            dateFrom={dateFrom}
            dateTo={dateTo}
            broker={broker}
            brokerOptions={brokerOpts}
            priceCond={priceCond}
            priceValue={priceValue}
            threshold={threshold}
            setDateFrom={setDateFrom}
            setDateTo={setDateTo}
            setBroker={setBroker}
            setPriceCond={setPriceCond}
            setPriceValue={setPriceValue}
            setThreshold={setThreshold}
            onApply={applyFilters}
            onReset={() => {
              setBroker("Semua Broker");
              setPriceCond("diatas");
              setPriceValue("");
              setThreshold(0.35);
              setRows([]);
            }}
            busy={busyApply}
          />

          <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <MetricCard
              title="Jumlah Sinyal"
              value={rows.length.toLocaleString("id-ID")}
              subtitle="setelah filter diterapkan"
            />
            <MetricCard
              title="Threshold Model"
              value={threshold.toFixed(2)}
              subtitle="probabilitas naik"
            />
            <MetricCard
              title="Latest Snapshot"
              value={snap?.date ?? "—"}
              subtitle="Tanggal data harga"
            />
            <MetricCard
              title="Latest Broker Agg"
              value={agg?.date ?? "—"}
              subtitle="Ringkasan broker"
            />
          </section>

          <SignalsTable rows={rows} loading={busyApply || loading} />
        </>
      ) : (
        <section className="card card-pad">
          <div className="mb-3 text-sm font-semibold">Prestasi Broker</div>
          <div className="overflow-x-auto">
            <table className="table">
              <thead>
                <tr>
                  <th className="th">Symbol</th>
                  <th className="th">Top Buyer</th>
                  <th className="th">Buyer Conc. (%)</th>
                  <th className="th">Top Buyer Net</th>
                  <th className="th">Total Net</th>
                </tr>
              </thead>
              <tbody>
                {(agg?.rows ?? []).map((r: BrokerRow, i) => (
                  <tr key={i} className="hover:bg-slate-50">
                    <td className="td">{r.symbol ?? ""}</td>
                    <td className="td">{r.top_buyer ?? "—"}</td>
                    <td className="td td-num">
                      {((r.top_buyer_concentration ?? 0) * 100).toFixed(1)}%
                    </td>
                    <td className="td td-num">
                      {Number(r.top_buyer_net_value ?? 0).toLocaleString("id-ID")}
                    </td>
                    <td className="td td-num">
                      {Number(r.total_net_value ?? 0).toLocaleString("id-ID")}
                    </td>
                  </tr>
                ))}
                {(agg?.rows ?? []).length === 0 && (
                  <tr>
                    <td className="td" colSpan={5}>
                      Tidak ada data.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </main>
  );
}
