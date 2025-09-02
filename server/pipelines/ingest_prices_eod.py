"""
Usage:
  DATA_DIR=./data python3 pipelines/ingest_prices_eod.py 2025-09-02 /path/to/vendor.csv|/path/dir|glob|-

- Baca daftar simbol dari DATA_DIR/nama_saham.csv (fleksibel: symbol/symbols/kode/kodesaham/ticker/code/…)
- Baca CSV vendor (fleksibel: symbol/kode/ticker/code, date/tanggal/tgl, close/harga/price/last, volume/vol)
- Vendor path bisa:
    * path ke file CSV,
    * path ke direktori (ambil CSV TERMUTAKHIR),
    * pola glob (mis. /tmp/vendor_2025-09-02*.csv → ambil yang terbaru),
    * "-" atau "NONE" → skip vendor (isi harga/volume NaN).
- Tulis DATA_DIR/prices_YYYY-MM-DD.csv berisi SEMUA simbol, kolom: symbol,date,close,volume
"""
import os
import sys
import re
import glob
import pandas as pd
import numpy as np

DATA_DIR = os.environ.get("DATA_DIR", "data")

# ---------- Utils ----------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).strip().lower())

def _first_match(cols_norm, candidates):
    for cand in candidates:
        if cand in cols_norm:
            return cols_norm[cand]
    return None

def _pick_latest_csv(path: str) -> str | None:
    """Jika path adalah direktori → pilih CSV terbaru di dalamnya.
       Jika path adalah glob → pilih file terbaru yang cocok.
       Jika path adalah file → kembalikan path bila ada."""
    if path in ("-", "NONE", None):
        return None

    # file langsung?
    if os.path.isfile(path):
        return path

    candidates = []
    # direktori?
    if os.path.isdir(path):
        candidates = glob.glob(os.path.join(path, "*.csv"))
    else:
        # asumsikan glob pattern
        g = glob.glob(path)
        # kalau pattern mengarah ke file yang ada, pakai list itu
        candidates = g

    if not candidates:
        return None

    # pilih berdasarkan mtime terbaru
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

# ---------- Symbols ----------
def _read_symbols() -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "nama_saham.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"File simbol tidak ditemukan: {path}")

    df = pd.read_csv(path)
    cols_norm = {_norm(c): c for c in df.columns}

    candidates = [
        "symbol", "symbols", "ticker", "code", "tickercode",
        "kode", "kodesaham", "stock", "stocksymbol", "emiten", "kodeemiten"
    ]

    key = _first_match(cols_norm, candidates)

    # Heuristik auto-deteksi: kolom dengan rasio pola ticker tertinggi
    if key is None:
        def ticker_like_ratio(series: pd.Series) -> float:
            s = series.astype(str).str.strip()
            s = s.str.replace(r"\s+", "", regex=True).str.upper()
            pat = re.compile(r"^[A-Z]{3,5}(?:[.\-]?[A-Z0-9]{0,2})?$")
            valid = s.str.fullmatch(pat).fillna(False)
            return valid.mean()

        ratios = {c: ticker_like_ratio(df[c]) for c in df.columns}
        candidates_auto = [c for c, r in ratios.items() if r >= 0.6]
        if candidates_auto:
            key = max(candidates_auto, key=lambda c: ratios[c])

    if key is None and df.shape[1] == 1:
        key = df.columns[0]

    if key is None:
        available = ", ".join([str(c) for c in df.columns])
        raise RuntimeError(
            "nama_saham.csv harus punya kolom 'symbol' / 'kode' / 'ticker' / 'code'. "
            f"Kolom tersedia: [{available}]"
        )

    out = df[[key]].rename(columns={key: "symbol"}).copy()
    out["symbol"] = (
        out["symbol"].astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.upper()
    )
    out = out[out["symbol"].str.len() > 0]
    out = out.drop_duplicates("symbol").sort_values("symbol", kind="stable").reset_index(drop=True)

    if out.empty:
        raise RuntimeError("Daftar simbol kosong setelah normalisasi. Cek isi nama_saham.csv Anda.")

    print(f"[ingest] symbols: {len(out)} ditemukan. contoh: {out['symbol'].head(5).tolist()}")
    return out

# ---------- Vendor ----------
def _read_vendor_maybe(vendor_hint: str | None, asof: str) -> pd.DataFrame:
    """Coba baca vendor CSV. Jika tidak ada, kembalikan DF kosong dengan kolom target."""
    path = _pick_latest_csv(vendor_hint) if vendor_hint else None

    if path is None:
        print(f"[ingest] vendor CSV tidak ditemukan (hint: {vendor_hint}). Melanjutkan tanpa vendor (close/volume NaN).")
        # DF kosong namun schema sudah sesuai agar downstream aman
        return pd.DataFrame(columns=["symbol", "close", "volume", "date"])

    df = pd.read_csv(path)
    lowmap = {_norm(c): c for c in df.columns}

    def pick(*names):
        for n in names:
            nn = _norm(n)
            if nn in lowmap:
                return lowmap[nn]
        return None

    c_sym = pick("symbol", "symbols", "kode", "kodesaham", "ticker", "code", "tickercode", "emiten")
    c_dat = pick("date", "tanggal", "tgl")
    c_clo = pick("close", "harga", "price", "last", "closeprice")
    c_vol = pick("volume", "vol")

    if c_sym is None or c_clo is None:
        avail = ", ".join([str(c) for c in df.columns])
        print(
            "[ingest] WARNING: CSV vendor tidak memiliki kolom minimum (symbol & close). "
            f"Kolom tersedia: [{avail}]. Melanjutkan tanpa vendor."
        )
        return pd.DataFrame(columns=["symbol", "close", "volume", "date"])

    out = pd.DataFrame({
        "symbol": df[c_sym].astype(str).str.replace(r"\s+", "", regex=True).str.upper(),
        "close": pd.to_numeric(df[c_clo], errors="coerce"),
        "volume": pd.to_numeric(df[c_vol], errors="coerce") if c_vol else np.nan,
    })

    if c_dat:
        out["date"] = pd.to_datetime(df[c_dat], errors="coerce").dt.strftime("%Y-%m-%d")

    out = out[out["symbol"].str.len() > 0].copy()
    print(f"[ingest] vendor rows: {len(out)} | source: {path}")
    return out

# ---------- Main ----------
def main(asof: str, vendor_hint: str | None):
    print(f"[ingest] target date: {asof}")
    os.makedirs(DATA_DIR, exist_ok=True)

    syms = _read_symbols()
    ven  = _read_vendor_maybe(vendor_hint, asof)

    # tetapkan tanggal
    if "date" in ven.columns and ven["date"].notna().any():
        ven = ven[ven["date"] == asof].copy()
    else:
        ven = ven.copy()
        ven["date"] = asof

    # ringkas vendor
    if not ven.empty:
        ven = ven.groupby("symbol", as_index=False).agg({"date":"first", "close":"last", "volume":"sum"})

    # join agar semua simbol keluar
    merged = syms.merge(ven, on="symbol", how="left")
    merged["date"] = merged["date"].fillna(asof)

    out = merged[["symbol", "date", "close", "volume"]].copy()
    out_path = os.path.join(DATA_DIR, f"prices_{asof}.csv")
    out.to_csv(out_path, index=False)
    print(f"[ingest] wrote {len(out):,} rows -> {out_path}")

    missing = out["close"].isna().sum()
    if missing:
        print(f"[ingest] WARNING: {missing} simbol tanpa harga pada {asof} (tetap disertakan).")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: DATA_DIR=./data python3 pipelines/ingest_prices_eod.py YYYY-MM-DD /path/to/vendor.csv|/path/dir|glob|-")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
