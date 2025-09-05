"""
Usage:
  DATA_DIR=./data python3 pipelines/ingest_prices_eod.py 2025-09-02 /path/to/vendor.csv|/path/dir|glob|-|goapi

Perbaikan penting:
- Normalisasi & pembersihan: treat close<=0 sebagai NaN supaya tidak ikut prediksi.
"""

import os
import sys
import re
import glob
import time
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np

# ==== Load .env ====
try:
    from dotenv import load_dotenv, find_dotenv
    _env_file = os.environ.get("ENV_FILE")
    if _env_file and os.path.exists(_env_file):
        load_dotenv(_env_file)
    else:
        load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

# ==== Config umum ====
DATA_DIR = os.environ.get("DATA_DIR", "data")

# ==== GoAPI config ====
GOAPI_BASE_URL = os.environ.get("GOAPI_BASE_URL", "https://api.goapi.io").rstrip("/")
GOAPI_API_KEY  = os.environ.get("GOAPI_API_KEY", "").strip()
GOAPI_LOOKBACK_DAYS = int(os.environ.get("GOAPI_LOOKBACK_DAYS", "7"))

# Parallel & HTTP
MAX_WORKERS        = int(os.environ.get("MAX_WORKERS", "12"))
REQ_TIMEOUT_SEC    = float(os.environ.get("REQ_TIMEOUT_SEC", "20"))
MAX_RETRY          = int(os.environ.get("MAX_RETRY", "3"))
RETRY_BACKOFF_MIN  = float(os.environ.get("RETRY_BACKOFF_MIN", "0.5"))
RETRY_BACKOFF_MAX  = float(os.environ.get("RETRY_BACKOFF_MAX", "1.5"))
RATE_LIMIT_SLEEP   = float(os.environ.get("RATE_LIMIT_SLEEP", "0.03"))

# ---------- Utils ----------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).strip().lower())

def _first_match(cols_norm, candidates):
    for cand in candidates:
        if cand in cols_norm:
            return cols_norm[cand]
    return None

def _pick_latest_csv(path: str) -> Optional[str]:
    if path in ("-", "NONE", None):
        return None
    if os.path.isfile(path):
        return path
    candidates: List[str] = []
    if os.path.isdir(path):
        candidates = glob.glob(os.path.join(path, "*.csv"))
    else:
        candidates = glob.glob(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

def _http_get_json(url: str, params: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    import requests
    last_err = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "idx-ingest/1.1"})
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    raise RuntimeError(f"Non-JSON response: {r.text[:200]}")
            else:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRY:
                time.sleep(random.uniform(RETRY_BACKOFF_MIN, RETRY_BACKOFF_MAX))
        finally:
            if RATE_LIMIT_SLEEP > 0:
                time.sleep(RATE_LIMIT_SLEEP)
    raise last_err if last_err else RuntimeError("Unknown HTTP error")

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

    if key is None:
        def ticker_like_ratio(series: pd.Series) -> float:
            s = series.astype(str).str.strip()
            s = s.str.replace(r"\s+", "", regex=True).str.upper()
            pat = re.compile(r"^[A-Z]{2,6}(?:[.\-]?[A-Z0-9]{0,2})?$")
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

# ---------- Vendor CSV ----------
def _read_vendor_maybe(vendor_hint: Optional[str], asof: str) -> pd.DataFrame:
    path = _pick_latest_csv(vendor_hint) if vendor_hint else None
    if path is None:
        print(f"[ingest] vendor CSV tidak ditemukan (hint: {vendor_hint}).")
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
        print("[ingest] WARNING: CSV vendor tidak memiliki kolom minimum (symbol & close). "
              f"Kolom tersedia: [{avail}]. Melewati vendor.")
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

# ---------- GoAPI fetch ----------
def _http_first_list(obj: Any):
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            res = _http_first_list(v)
            if res:
                return res
    return None

def _goapi_fetch_one(symbol: str, asof: str, lookback_days: int) -> Dict[str, Any]:
    try:
        end = datetime.strptime(asof, "%Y-%m-%d").date()
    except ValueError:
        end = datetime.utcnow().date()
    start = end - timedelta(days=max(1, lookback_days))
    url = f"{GOAPI_BASE_URL}/stock/idx/{symbol}/historical"
    params = {"from": start.isoformat(), "to": end.isoformat(), "api_key": GOAPI_API_KEY}
    js = _http_get_json(url, params, timeout=REQ_TIMEOUT_SEC)
    rows = _http_first_list(js) or []
    if not rows:
        return {"symbol": symbol, "date": asof, "close": np.nan, "volume": np.nan}
    df = pd.DataFrame(rows).copy()
    lower = {c.lower(): c for c in df.columns}

    def norm(cands, dst):
        for cc in cands:
            if cc in lower:
                df.rename(columns={lower[cc]: dst}, inplace=True)
                return
        if dst not in df.columns:
            df[dst] = np.nan

    norm(["date", "tanggal", "time", "timestamp"], "date")
    norm(["close", "c", "last", "close_price", "price"], "close")
    norm(["volume", "v", "vol"], "volume")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # 1) Cari bar tepat asof, 2) fallback ke terakhir ≤ asof
    sub = df[df["date"] == asof]
    if not sub.empty:
        r = sub.iloc[-1]
    else:
        df_sorted = df.sort_values("date")
        df_sorted = df_sorted[df_sorted["date"] <= asof]
        if df_sorted.empty:
            return {"symbol": symbol, "date": asof, "close": np.nan, "volume": np.nan}
        r = df_sorted.iloc[-1]

    return {
        "symbol": symbol,
        "date": str(r.get("date") or asof),
        "close": float(r.get("close")) if pd.notna(r.get("close")) else np.nan,
        "volume": float(r.get("volume")) if pd.notna(r.get("volume")) else np.nan,
    }

def _goapi_fetch_all(symbols: List[str], asof: str, lookback_days: int) -> pd.DataFrame:
    if not GOAPI_API_KEY:
        print("[ingest] GOAPI_API_KEY belum di-set. Tidak bisa fetch dari GoAPI.")
        return pd.DataFrame(columns=["symbol", "date", "close", "volume"])
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out_rows: List[Dict[str, Any]] = []
    errs = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_goapi_fetch_one, sym, asof, lookback_days): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                row = fut.result()
                out_rows.append(row)
            except Exception:
                errs += 1
                out_rows.append({"symbol": sym, "date": asof, "close": np.nan, "volume": np.nan})
    if errs:
        print(f"[ingest] WARNING: {errs} simbol gagal diambil dari GoAPI (diisi NaN).")
    df = pd.DataFrame(out_rows)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["date"] = df["date"].astype(str)
    return df[["symbol", "date", "close", "volume"]]

# ---------- Main ----------
def main(asof: str, vendor_hint: Optional[str]):
    print(f"[ingest] target date: {asof}")
    os.makedirs(DATA_DIR, exist_ok=True)

    syms_df = _read_symbols()
    symbols = syms_df["symbol"].tolist()

    vendor_df = pd.DataFrame(columns=["symbol", "close", "volume", "date"])
    use_goapi = False
    if vendor_hint:
        hint = vendor_hint.strip().lower()
        if hint in ("goapi",):
            use_goapi = True
        else:
            vendor_df = _read_vendor_maybe(vendor_hint, asof)
            if vendor_df.empty and hint in ("-", "none"):
                use_goapi = True
    else:
        vendor_df = _read_vendor_maybe(None, asof)
        if vendor_df.empty and GOAPI_API_KEY:
            use_goapi = True

    if use_goapi:
        print(f"[ingest] menggunakan GoAPI (window {GOAPI_LOOKBACK_DAYS} hari) untuk mengambil harga…")
        vendor_df = _goapi_fetch_all(symbols, asof, GOAPI_LOOKBACK_DAYS)

    # --- NORMALISASI TANGGAL ---
    # Simpan tanggal bar asli di 'source_date', dan paksa kolom 'date' menjadi asof agar konsisten.
    if "date" in vendor_df.columns:
        vendor_df = vendor_df.rename(columns={"date": "source_date"})
    else:
        vendor_df["source_date"] = asof
    vendor_df["date"] = asof  # effective date untuk snapshot

    # --- PERBAIKAN: pembersihan nilai ---
    if not vendor_df.empty:
        vendor_df["close"]  = pd.to_numeric(vendor_df["close"], errors="coerce")
        vendor_df["volume"] = pd.to_numeric(vendor_df["volume"], errors="coerce")
        # treat harga tidak valid sebagai NaN
        vendor_df.loc[(vendor_df["close"] <= 0) | (vendor_df["close"].isna()), "close"] = np.nan
        vendor_df.loc[vendor_df["volume"] < 0, "volume"] = 0

        def _sum_min_count(s: pd.Series):
            return s.sum(min_count=1)
        vendor_df = vendor_df.groupby("symbol", as_index=False).agg({
            "date": "last",
            "close": "last",
            "volume": _sum_min_count,
            "source_date": "last"
        })

    # Join ke daftar simbol agar semua simbol keluar
    merged = syms_df.merge(vendor_df, on="symbol", how="left")
    merged["date"] = merged["date"].fillna(asof)
    if "source_date" not in merged.columns:
        merged["source_date"] = asof

    out = merged[["symbol", "date", "close", "volume", "source_date"]].copy()
    out_path = os.path.join(DATA_DIR, f"prices_{asof}.csv")
    out.to_csv(out_path, index=False)
    print(f"[ingest] wrote {len(out):,} rows -> {out_path}")

    # Ringkasan kualitas data
    missing_close = int(out["close"].isna().sum())
    n_asof = int((out["source_date"] == asof).sum())
    n_fallback = len(out) - n_asof
    print(f"[ingest] INFO: {n_asof} bar tepat {asof}, {n_fallback} bar fallback ≤ asof. "
          f"Tanpa harga (NaN): {missing_close} simbol.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: DATA_DIR=./data python3 pipelines/ingest_prices_eod.py YYYY-MM-DD /path/to/vendor.csv|/path/dir|glob|-|goapi")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
