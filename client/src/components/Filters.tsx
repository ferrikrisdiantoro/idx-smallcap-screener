"use client";
import React from "react";

type Props = {
  dateFrom: string;
  dateTo: string;
  broker: string;
  brokerOptions: string[];
  priceCond: "diatas" | "dibawah";
  priceValue: number | "";
  threshold: number;

  setDateFrom: (v: string) => void;
  setDateTo: (v: string) => void;
  setBroker: (v: string) => void;
  setPriceCond: (v: "diatas" | "dibawah") => void;
  setPriceValue: (v: number | "") => void;
  setThreshold: (v: number) => void;

  onApply: () => void;
  onReset: () => void;
  busy?: boolean;
};

const Filters: React.FC<Props> = ({
  dateFrom, dateTo, broker, brokerOptions, priceCond, priceValue, threshold,
  setDateFrom, setDateTo, setBroker, setPriceCond, setPriceValue, setThreshold,
  onApply, onReset, busy
}) => {
  return (
    <section className="card card-pad">
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-5">
        <div>
          <label className="block text-xs font-medium text-slate-600">Tanggal Mulai</label>
          <input type="date" className="input mt-1 w-full" value={dateFrom ?? ""} onChange={e => setDateFrom(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Tanggal Selesai</label>
          <input type="date" className="input mt-1 w-full" value={dateTo ?? ""} onChange={e => setDateTo(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Broker</label>
          <select className="select mt-1 w-full" value={broker} onChange={e => setBroker(e.target.value)}>
            {brokerOptions.map(b => <option key={b} value={b}>{b}</option>)}
          </select>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="block text-xs font-medium text-slate-600">Harga</label>
            <select className="select mt-1 w-full" value={priceCond} onChange={e => setPriceCond(e.target.value as any)}>
              <option value="diatas">Diatas</option>
              <option value="dibawah">Dibawah</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600">&nbsp;</label>
            <input
              type="number"
              placeholder="Contoh: 500"
              className="input mt-1 w-full"
              value={priceValue}
              onChange={e => setPriceValue(e.target.value === "" ? "" : Number(e.target.value))}
            />
          </div>
        </div>
        <div className="flex flex-col">
          <label className="block text-xs font-medium text-slate-600">Threshold Model</label>
          <div className="mt-2 flex items-center gap-3">
            <input
              type="range"
              min={0} max={1} step={0.01}
              className="w-full accent-emerald-500"
              value={threshold}
              onChange={e => setThreshold(parseFloat(e.target.value))}
            />
            <div className="w-12 text-right tabular-nums text-sm">{threshold.toFixed(2)}</div>
          </div>
        </div>
      </div>

      <div className="mt-4 flex gap-3">
        <button onClick={onApply} className="btn" disabled={busy}>
          {busy ? "Memproses…" : "Terapkan"}
        </button>
        <button onClick={onReset} className="btn-secondary">Reset</button>
      </div>
    </section>
  );
};

export default Filters;
