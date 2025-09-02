"use client";
import React from "react";

type Props = {
  symbols: string[];
  symbol: string | null;
  onSymbolChange: (s: string) => void;
  threshold: number;
  onThresholdChange: (t: number) => void;
  onRefresh: () => void;
};

const Toolbar: React.FC<Props> = ({ symbols, symbol, onSymbolChange, threshold, onThresholdChange, onRefresh }) => (
  <div className="card card-pad">
    <div className="flex flex-col gap-5 md:flex-row md:items-end md:justify-between">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {/* Symbol */}
        <div>
          <label className="block text-xs font-medium muted">Symbol</label>
          <select
            className="mt-1 w-56 rounded-xl border border-slate-700 bg-slate-900 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500"
            value={symbol ?? ""}
            onChange={(e) => onSymbolChange(e.target.value)}
          >
            {!symbol && <option value="">Pilih…</option>}
            {symbols.map((s) => (<option key={s} value={s}>{s}</option>))}
          </select>
        </div>

        {/* Threshold slider */}
        <div>
          <label className="block text-xs font-medium muted">Threshold</label>
          <div className="mt-2 flex items-center gap-3">
            <input
              type="range"
              min={0} max={1} step={0.01}
              value={threshold}
              onChange={(e) => onThresholdChange(parseFloat(e.target.value))}
              className="w-64 accent-emerald-400"
              title="Ubah ambang keputusan (0..1)"
            />
            <div className="w-12 text-right text-sm tabular-nums">{threshold.toFixed(2)}</div>
          </div>
        </div>
      </div>

      <button onClick={onRefresh} className="btn btn-primary" title="Ambil ulang prediksi untuk symbol aktif">
        Refresh
      </button>
    </div>
  </div>
);

export default Toolbar;
