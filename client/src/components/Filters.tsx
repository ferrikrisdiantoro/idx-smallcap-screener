"use client";
import React from "react";

type PriceCond = "diatas" | "dibawah";

type Props = {
  dateFrom: string;
  dateTo: string;
  broker: string;
  brokerOptions: string[];
  priceCond: PriceCond;
  priceValue: number | "";
  threshold: number; // 0..1 (tetap seperti sebelumnya)

  // NEW
  signal: "Semua" | "BELI" | "JUAL";
  symbolExact: string;
  sortDate: "asc" | "desc";
  volRatioMin: number | "";

  setDateFrom: (v: string) => void;
  setDateTo: (v: string) => void;
  setBroker: (v: string) => void;
  setPriceCond: (v: PriceCond) => void;
  setPriceValue: (v: number | "") => void;
  setThreshold: (v: number) => void; // menerima 0..1

  // NEW setters
  setSignal: (v: "Semua" | "BELI" | "JUAL") => void;
  setSymbolExact: (v: string) => void;
  setSortDate: (v: "asc" | "desc") => void;
  setVolRatioMin: (v: number | "") => void;

  onApply: () => void;
  onReset: () => void;
  busy?: boolean;
};

const Filters: React.FC<Props> = ({
  dateFrom, dateTo, broker, brokerOptions, priceCond, priceValue, threshold,
  signal, symbolExact, sortDate, volRatioMin,
  setDateFrom, setDateTo, setBroker, setPriceCond, setPriceValue, setThreshold,
  setSignal, setSymbolExact, setSortDate, setVolRatioMin,
  onApply, onReset, busy,
}) => {
  const handlePriceChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const raw = e.target.value.trim();
    if (raw === "") return setPriceValue("");
    const n = Number(raw);
    if (Number.isFinite(n)) setPriceValue(n);
  };

  const handleVolRatioMin = (e: React.ChangeEvent<HTMLInputElement>) => {
    const raw = e.target.value.trim();
    if (raw === "") return setVolRatioMin("");
    const n = Number(raw);
    if (Number.isFinite(n)) setVolRatioMin(n);
  };

  // === THRESHOLD (input angka persen) ===
  // Kita tampilkan dalam persen 0..100, tapi state internal tetap 0..1
  const thresholdPct = Math.round((Number.isFinite(threshold) ? threshold : 0) * 100);
  const handleThresholdPctChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const raw = e.target.value;
    if (raw === "") {
      // kalau dikosongkan, jangan ubah state supaya tidak bikin 0 mendadak
      return;
    }
    let pct = Number(raw);
    if (!Number.isFinite(pct)) return;
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    setThreshold(pct / 100); // convert ke 0..1
  };

  return (
    <section className="card card-pad">
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-6">
        <div>
          <label className="block text-xs font-medium text-slate-600">Tanggal Mulai</label>
          <input
            type="date"
            className="input mt-1 w-full"
            value={dateFrom ?? ""}
            onChange={(e) => setDateFrom(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Tanggal Selesai</label>
          <input
            type="date"
            className="input mt-1 w-full"
            value={dateTo ?? ""}
            onChange={(e) => setDateTo(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Sinyal</label>
          <select
            className="select mt-1 w-full"
            value={signal}
            onChange={(e) => setSignal(e.target.value as any)}
          >
            <option>Semua</option>
            <option value="BELI">BELI</option>
            <option value="JUAL">JUAL</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Emiten</label>
          <input
            placeholder="Contoh: BBCA"
            className="input mt-1 w-full"
            value={symbolExact}
            onChange={(e) => setSymbolExact(e.target.value.toUpperCase())}
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Urut Tanggal</label>
          <select
            className="select mt-1 w-full"
            value={sortDate}
            onChange={(e) => setSortDate(e.target.value as any)}
          >
            <option value="desc">DESC (terbaru dulu)</option>
            <option value="asc">ASC (terlama dulu)</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-slate-600">Broker</label>
          <select
            className="select mt-1 w-full"
            value={broker}
            onChange={(e) => setBroker(e.target.value)}
          >
            {brokerOptions.map((b) => (
              <option key={b} value={b}>{b}</option>
            ))}
          </select>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="block text-xs font-medium text-slate-600">Harga</label>
            <select
              className="select mt-1 w-full"
              value={priceCond}
              onChange={(e) => setPriceCond(e.target.value as PriceCond)}
            >
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
              value={priceValue === "" ? "" : String(priceValue)}
              onChange={handlePriceChange}
              min={0}
            />
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-600">Vol Ratio Min</label>
          <input
            type="number"
            placeholder="Contoh: 10"
            className="input mt-1 w-full"
            value={volRatioMin === "" ? "" : String(volRatioMin)}
            onChange={handleVolRatioMin}
            min={0}
          />
        </div>

        {/* === Threshold: input angka persen (0..100) === */}
        <div>
          <label className="block text-xs font-medium text-slate-600">Threshold Model (%)</label>
          <input
            type="number"
            min={0}
            max={100}
            step={1}
            className="input mt-1 w-full"
            value={thresholdPct}
            onChange={handleThresholdPctChange}
            placeholder="Contoh: 70"
          />
          <div className="mt-1 text-xs text-slate-500">
            Saat ini: {thresholdPct}% (probabilitas naik). Backend akan memakai { (threshold).toFixed(2) }.
          </div>
        </div>
      </div>

      <div className="mt-4 flex gap-3">
        <button onClick={onApply} className="btn" disabled={busy}>
          {busy ? "Memproses…" : "Terapkan"}
        </button>
        <button onClick={onReset} className="btn-secondary" disabled={busy}>
          Reset
        </button>
      </div>
    </section>
  );
};

export default Filters;
