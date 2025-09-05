import os, sys, glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = os.environ.get("DATA_DIR", "data")

def load_prices_for(date_str: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, f"prices_{date_str}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path)

def find_agg_on_or_before(date_str: str):
    files = sorted(glob.glob(os.path.join(DATA_DIR, "broker_agg_*.csv")))
    if not files:
        return None, None
    tgt = pd.to_datetime(date_str).date()
    cand = []
    for p in files:
        d = pd.to_datetime(os.path.basename(p)[11:-4]).date()
        if d <= tgt:
            cand.append((d, p))
    if not cand:
        return None, None
    cand.sort()
    p = cand[-1][1]
    eff = os.path.basename(p)[11:-4]
    return p, eff

def add_lags(df: pd.DataFrame, cols, lags=(1,2,3)):
    df = df.sort_values(["symbol","date"]).copy()
    for c in cols:
        if c in df.columns:
            for k in lags:
                df[f"{c}_lag{k}"] = df.groupby("symbol", sort=False)[c].shift(k)
    return df

def compute_features(prices: pd.DataFrame) -> pd.DataFrame:
    prices["symbol"] = prices["symbol"].astype(str).str.upper()
    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()
    for c in ["close","volume"]:
        if c not in prices.columns:
            prices[c] = np.nan

    prices = prices.sort_values(["symbol","date"])
    prices["close_lag1"] = prices.groupby("symbol", sort=False)["close"].shift(1)
    prices["ret_1"] = (prices["close"] / prices["close_lag1"] - 1.0).replace([np.inf,-np.inf], np.nan)

    prices["vol_lag20"] = prices.groupby("symbol", sort=False)["volume"].rolling(20, min_periods=1).mean().reset_index(level=0, drop=True)
    prices["vol_ratio"] = (prices["volume"] / prices["vol_lag20"]).replace([np.inf,-np.inf], np.nan)

    prices = add_lags(prices, ["ret_1","vol_ratio"], lags=(1,2,3))
    prices["is_price_lt_500"] = (prices["close"] < 500).astype(int)
    return prices

def find_latest_snapshot_on_or_before(target_date: str) -> str | None:
    snaps = sorted(glob.glob(os.path.join(DATA_DIR, "daily_snapshot_*.csv")))
    if not snaps:
        return None
    tgt = pd.to_datetime(target_date).date()
    cand = []
    for p in snaps:
        d = pd.to_datetime(os.path.basename(p)[15:-4]).date()
        if d <= tgt:
            cand.append((d, p))
    if not cand:
        return None
    cand.sort()
    return cand[-1][1]

def fallback_clone_from_latest_snapshot(target_date: str) -> int:
    src = find_latest_snapshot_on_or_before(target_date)
    if not src:
        raise RuntimeError("Fallback gagal: tidak ada snapshot yang bisa diclone.")
    print(f"[fallback] clone dari: {os.path.basename(src)} -> daily_snapshot_{target_date}.csv")

    df = pd.read_csv(src)
    df["date"] = target_date
    df["symbol"] = df["symbol"].astype(str).str.upper()

    agg_path, _eff = find_agg_on_or_before(target_date)
    if agg_path:
        agg = pd.read_csv(agg_path)
        if "symbol" in agg.columns:
            agg["symbol"] = agg["symbol"].astype(str).str.upper()
            df = df.merge(agg, on="symbol", how="left", suffixes=("","_agg"))

    keep = [
        "symbol","date","close","volume",
        "ret_1","vol_ratio",
        "ret_1_lag1","ret_1_lag2","ret_1_lag3",
        "vol_ratio_lag1","vol_ratio_lag2","vol_ratio_lag3",
        "is_price_lt_500",
        "top_buyer","top_buyer_concentration","top_buyer_net_value","total_net_value",
        "num_buyers","num_sellers","num_brokers","retail_broker_ratio",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = np.nan
    df = df[keep].copy()
    out_path = os.path.join(DATA_DIR, f"daily_snapshot_{target_date}.csv")
    df.to_csv(out_path, index=False)
    print(f"[fallback] wrote {len(df):,} rows -> {out_path}")
    return len(df)

def build_snapshot_for(date_str: str):
    out_path = os.path.join(DATA_DIR, f"daily_snapshot_{date_str}.csv")
    print(f"[make_snapshot] target: {out_path}")

    end = datetime.strptime(date_str, "%Y-%m-%d")
    start = end - timedelta(days=90)

    frames = []
    for d in pd.date_range(start, end, freq="D"):
        dstr = d.strftime("%Y-%m-%d")
        try:
            frames.append(load_prices_for(dstr))
        except FileNotFoundError:
            continue

    if frames:
        prices_all = pd.concat(frames, ignore_index=True)
        # pastikan kolom date bisa di-parse meski dari vendor/ingest baru
        if "date" not in prices_all.columns:
            raise RuntimeError("Kolom 'date' tidak ditemukan di prices_.csv")
        feats = compute_features(prices_all)
        asof = pd.to_datetime(date_str).normalize()

        # PILIH BAR TERAKHIR ≤ ASOF PER SIMBOL (bukan strict equal)
        sub = (feats[feats["date"] <= asof]
               .sort_values(["symbol","date"])
               .groupby("symbol", as_index=False)
               .tail(1)
              ).copy()

        # set effective date = asof untuk konsistensi nama file
        sub["date"] = asof

        # join broker agg (≤ tanggal)
        agg_path, _eff = find_agg_on_or_before(date_str)
        if agg_path:
            agg = pd.read_csv(agg_path)
            if "symbol" in agg.columns:
                agg["symbol"] = agg["symbol"].astype(str).str.upper()
                sub = sub.merge(agg, on="symbol", how="left", suffixes=("","_agg"))

        keep = [
            "symbol","date","close","volume",
            "ret_1","vol_ratio",
            "ret_1_lag1","ret_1_lag2","ret_1_lag3",
            "vol_ratio_lag1","vol_ratio_lag2","vol_ratio_lag3",
            "is_price_lt_500",
            "top_buyer","top_buyer_concentration","top_buyer_net_value","total_net_value",
            "num_buyers","num_sellers","num_brokers","retail_broker_ratio",
        ]
        for c in keep:
            if c not in sub.columns:
                sub[c] = np.nan
        sub = sub[keep].copy()
        sub["date"] = sub["date"].dt.strftime("%Y-%m-%d")
        sub.to_csv(out_path, index=False)
        print(f"[make_snapshot] wrote {len(sub):,} rows (jalur harga)")
        return

    print("[make_snapshot] Tidak ada file harga untuk range itu. Fallback clone…")
    n = fallback_clone_from_latest_snapshot(date_str)
    if n == 0:
        raise RuntimeError("Fallback clone menghasilkan 0 baris.")
    print(f"[make_snapshot] selesai fallback clone ({n} baris).")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pipelines/make_daily_snapshot.py YYYY-MM-DD")
        sys.exit(1)
    build_snapshot_for(sys.argv[1])
