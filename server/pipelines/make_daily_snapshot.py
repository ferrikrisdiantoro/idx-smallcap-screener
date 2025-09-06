import os, sys, glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = os.environ.get("DATA_DIR", "data").rstrip("/")

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
        d = pd.to_datetime(os.path.basename(p)[11:-4]).date()  # tanggal di NAMA FILE (requested)
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
    prices = prices.copy()
    prices["symbol"] = prices["symbol"].astype(str).str.upper()

    # date = "asof" dari file; source_date = tanggal bar asli (kalau ada)
    if "date" not in prices.columns:
        raise RuntimeError("Kolom 'date' tidak ditemukan di prices_.csv")
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.normalize()
    if "source_date" in prices.columns:
        prices["source_date"] = pd.to_datetime(prices["source_date"], errors="coerce").dt.normalize()

    for c in ["close","volume"]:
        if c not in prices.columns:
            prices[c] = np.nan

    prices = prices.sort_values(["symbol","date"])
    prices["close_lag1"] = prices.groupby("symbol", sort=False)["close"].shift(1)
    prices["ret_1"] = (prices["close"] / prices["close_lag1"] - 1.0).replace([np.inf,-np.inf], np.nan)

    prices["vol_lag20"] = (
        prices.groupby("symbol", sort=False)["volume"]
              .rolling(20, min_periods=1)
              .mean().reset_index(level=0, drop=True)
    )
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
        # di file broker_agg baru, tanggal asli ada di 'broker_source_date'
        if "broker_source_date" not in agg.columns and "date" in agg.columns:
            agg.rename(columns={"date":"broker_source_date"}, inplace=True)
        if "symbol" in agg.columns:
            agg["symbol"] = agg["symbol"].astype(str).str.upper()
            df = df.merge(agg, on="symbol", how="left", suffixes=("","_agg"))

    df = _finalize_snapshot_columns(df, target_date)
    out_path = os.path.join(DATA_DIR, f"daily_snapshot_{target_date}.csv")
    df.to_csv(out_path, index=False)
    print(f"[fallback] wrote {len(df):,} rows -> {out_path}")
    return len(df)

def _finalize_snapshot_columns(df: pd.DataFrame, asof_str: str) -> pd.DataFrame:
    keep = [
        "symbol","date","close","volume",
        "ret_1","vol_ratio",
        "ret_1_lag1","ret_1_lag2","ret_1_lag3",
        "vol_ratio_lag1","vol_ratio_lag2","vol_ratio_lag3",
        "is_price_lt_500",
        "top_buyer","top_buyer_concentration","top_buyer_net_value","total_net_value",
        "num_buyers","num_sellers","num_brokers","retail_broker_ratio",
        # penanda staleness:
        "price_source_date","broker_source_date","age_price_days","age_broker_days","is_market_closed",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = np.nan
    # jaga format tanggal sebagai YYYY-MM-DD string
    for c in ("date","price_source_date","broker_source_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # pastikan date final = asof_str
    df["date"] = asof_str
    return df[keep].copy()

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
        feats = compute_features(prices_all)
        asof = pd.to_datetime(date_str).normalize()

        # PILIH bar terakhir <= ASOF per simbol berdasarkan "date" (file harian)
        sub = (feats[feats["date"] <= asof]
               .sort_values(["symbol","date"])
               .groupby("symbol", as_index=False)
               .tail(1)
               ).copy()

        # Bawa source_date untuk staleness harga
        if "source_date" in sub.columns:
            sub.rename(columns={"source_date":"price_source_date"}, inplace=True)
        else:
            sub["price_source_date"] = sub["date"]

        # Join broker agg dari file broker_agg_(<=asof).csv
        agg_path, _eff = find_agg_on_or_before(date_str)
        if agg_path:
            agg = pd.read_csv(agg_path)
            # file broker_agg baru sudah pakai 'broker_source_date'
            if "broker_source_date" not in agg.columns and "date" in agg.columns:
                agg.rename(columns={"date":"broker_source_date"}, inplace=True)
            if "symbol" in agg.columns:
                agg["symbol"] = agg["symbol"].astype(str).str.upper()
                sub = sub.merge(agg, on="symbol", how="left", suffixes=("","_agg"))

        # Hitung usia data & flag market closed
        sub["asof"] = asof
        for c in ("price_source_date","broker_source_date","asof"):
            sub[c] = pd.to_datetime(sub[c], errors="coerce")
        sub["age_price_days"]  = (sub["asof"] - sub["price_source_date"]).dt.days
        sub["age_broker_days"] = (sub["asof"] - sub["broker_source_date"]).dt.days
        sub["is_market_closed"] = (sub["age_price_days"] > 0).astype(int)

        # Set tanggal final ke string
        sub["date"] = asof

        df_out = _finalize_snapshot_columns(sub, asof.strftime("%Y-%m-%d"))
        df_out.to_csv(out_path, index=False)
        print(f"[make_snapshot] wrote {len(df_out):,} rows (jalur harga)")
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
