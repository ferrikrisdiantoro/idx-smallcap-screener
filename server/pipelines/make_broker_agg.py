import os
import sys
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

# ==== Load .env (opsional) ====
try:
    from dotenv import load_dotenv, find_dotenv
    _env_file = os.environ.get("ENV_FILE")
    if _env_file and os.path.exists(_env_file):
        load_dotenv(_env_file)
    else:
        load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

# =========================
# Config
# =========================
DATA_DIR = os.environ.get("DATA_DIR", "data").rstrip("/")
GOAPI_BASE_URL = os.environ.get("GOAPI_BASE_URL", "https://api.goapi.io").rstrip("/")
GOAPI_API_KEY = os.environ.get("GOAPI_API_KEY", "").strip()

# Parallel & HTTP
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "16"))
REQ_TIMEOUT_SEC = float(os.environ.get("REQ_TIMEOUT_SEC", "20"))
MAX_RETRY = int(os.environ.get("MAX_RETRY", "3"))
RETRY_BACKOFF_MIN = float(os.environ.get("RETRY_BACKOFF_MIN", "0.5"))
RETRY_BACKOFF_MAX = float(os.environ.get("RETRY_BACKOFF_MAX", "1.5"))
RATE_LIMIT_SLEEP = float(os.environ.get("RATE_LIMIT_SLEEP", "0.03"))

# =========================
# Utils
# =========================
def log(msg: str) -> None:
    print(msg, flush=True)

def _http_get_json(url: str, params: dict) -> dict:
    import requests
    last_err = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = requests.get(url, params=params, timeout=REQ_TIMEOUT_SEC, headers={"User-Agent": "idx-broker-agg/1.3"})
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

def http_get(path: str, params: dict | None = None) -> dict:
    if params is None:
        params = {}
    if GOAPI_API_KEY:
        params = dict(params)
        params["api_key"] = GOAPI_API_KEY
    url = f"{GOAPI_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    return _http_get_json(url, params)

def _first_list(obj):
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            res = _first_list(v)
            if res:
                return res
    return None

def _load_symbols() -> list[str]:
    for p in ["nama_saham.csv", os.path.join(DATA_DIR, "nama_saham.csv")]:
        if os.path.exists(p):
            df = pd.read_csv(p)
            low = {str(c).strip().lower(): c for c in df.columns}
            pick = None
            for k in ("symbol", "ticker", "code", "kode", "emiten", "kodesaham", "kode emiten"):
                if k in low:
                    pick = low[k]; break
            if pick is None:
                best, best_ratio = None, -1.0
                for c in df.columns:
                    vals = df[c].astype(str).str.strip().str.upper().str.replace(r"\.JK$", "", regex=True)
                    ratio = vals.str.fullmatch(r"[A-Z]{2,5}").mean()
                    if ratio > best_ratio: best_ratio, best = ratio, c
                pick = best
            syms = (df[pick].astype(str).str.strip().str.upper().str.replace(r"\.JK$", "", regex=True))
            syms = syms[syms.str.fullmatch(r"[A-Z]{2,5}")].dropna().unique().tolist()
            return sorted(syms)
    raise FileNotFoundError("nama_saham.csv tidak ditemukan (root atau ./data)")

def _guess_latest_trading_date(asof: str) -> str:
    """
    Cari tanggal trading terakhir <= asof dari file prices_*.csv.
    Gunakan mode(source_date) jika ada, supaya nyambung dengan kalender trading sebenarnya.
    """
    asof_dt = pd.to_datetime(asof, errors="coerce")
    if pd.isna(asof_dt):
        return asof
    asof_dt = asof_dt.date()
    for k in range(0, 7):
        d = (asof_dt - timedelta(days=k)).strftime("%Y-%m-%d")
        p = os.path.join(DATA_DIR, f"prices_{d}.csv")
        if os.path.exists(p):
            try:
                dfp = pd.read_csv(p)
                if "source_date" in dfp.columns and dfp["source_date"].notna().any():
                    sd = pd.to_datetime(dfp["source_date"], errors="coerce").dt.strftime("%Y-%m-%d").mode()
                    if not sd.empty and isinstance(sd.iloc[0], str) and len(sd.iloc[0]) == 10:
                        return sd.iloc[0]
            except Exception:
                pass
            return d
    return asof

# =========================
# Fetch broker summary (robust)
# =========================
def _fetch_broker_summary_robust(symbol: str, date_iso: str) -> pd.DataFrame:
    """
    Return: rows (date, symbol, broker, net_value)
    Tahan format:
      A) flat [{broker_code, side(BUY/SELL), value}, ...]
      B) split { buy: [...], sell: [...] }
    """
    try:
        js = http_get(f"/stock/idx/{symbol}/broker_summary", {"date": date_iso})
    except Exception as e:
        log(f"[warn] {symbol}: {e}")
        return pd.DataFrame(columns=["date", "symbol", "broker", "net_value"])

    rows = _first_list(js) or []
    # Case A
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        df = pd.json_normalize(rows, sep="_")
        cols = {str(c).lower(): c for c in df.columns}
        c_broker = next((cols[k] for k in ("broker_code","code","broker","brokercode") if k in cols), None)
        c_side   = next((cols[k] for k in ("side","action","type") if k in cols), None)
        c_val    = next((cols[k] for k in ("value","val","amount","qty_value","net","net_value") if k in cols), None)
        if c_broker and c_side and c_val:
            v = pd.to_numeric(df[c_val], errors="coerce")
            side = df[c_side].astype(str).str.upper()
            net = np.where(side.eq("BUY"), v, -v)
            out = pd.DataFrame({"date": date_iso, "symbol": symbol, "broker": df[c_broker].astype(str), "net_value": net})
            return out.dropna(subset=["net_value"], how="all")

    # Case B
    buy_list, sell_list = None, None
    if isinstance(js, dict):
        for k, v in js.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                kl = str(k).lower()
                if "buy" in kl:  buy_list  = v
                if "sell" in kl: sell_list = v

    def _lst_to_df(lst, sign):
        if not lst: return pd.DataFrame(columns=["date","symbol","broker","net_value"])
        d = pd.json_normalize(lst, sep="_")
        cols = {str(c).lower(): c for c in d.columns}
        c_b = next((cols[k] for k in ("broker_code","code","broker","brokercode") if k in cols), None)
        c_v = next((cols[k] for k in ("value","val","amount","qty_value","net","net_value") if k in cols), None)
        if c_b and c_v:
            return pd.DataFrame({
                "date": date_iso, "symbol": symbol,
                "broker": d[c_b].astype(str),
                "net_value": sign * pd.to_numeric(d[c_v], errors="coerce")
            })
        return pd.DataFrame(columns=["date","symbol","broker","net_value"])

    df_buy  = _lst_to_df(buy_list,  +1)
    df_sell = _lst_to_df(sell_list, -1)
    out = pd.concat([df_buy, df_sell], ignore_index=True)
    return out.dropna(subset=["net_value"], how="all")

# =========================
# Aggregator
# =========================
def _retail_flag(_broker_code: str) -> int:
    # Placeholder: jika punya daftar broker ritel, ubah di sini
    return 0

def aggregate_broker(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=[
            "date","symbol","total_net_value","top_buyer",
            "top_buyer_concentration","top_buyer_net_value",
            "num_buyers","num_sellers","num_brokers","retail_broker_ratio"
        ])

    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["broker"] = df["broker"].astype(str)
    df["net_value"] = pd.to_numeric(df["net_value"], errors="coerce")

    # satukan duplikat broker per (date,symbol,broker)
    df = (df.groupby(["date","symbol","broker"], as_index=False, sort=False)["net_value"]
            .sum(min_count=1).fillna({"net_value":0}))

    total_net = df.groupby(["date","symbol"], as_index=False, sort=False)["net_value"].sum(min_count=1)
    total_net.rename(columns={"net_value":"total_net_value"}, inplace=True)

    num_buyers = (df.assign(is_buy=df["net_value"]>0)
                    .groupby(["date","symbol"])["is_buy"].sum().reset_index()
                    .rename(columns={"is_buy":"num_buyers"}))
    num_sellers = (df.assign(is_sell=df["net_value"]<0)
                    .groupby(["date","symbol"])["is_sell"].sum().reset_index()
                    .rename(columns={"is_sell":"num_sellers"}))
    num_brokers = df.groupby(["date","symbol"], as_index=False)["broker"].nunique()
    num_brokers.rename(columns={"broker":"num_brokers"}, inplace=True)

    buyers_only = df[df["net_value"]>0].copy()
    if buyers_only.empty:
        top = pd.DataFrame(columns=["date","symbol","top_buyer","top_buyer_net_value","top_buyer_concentration"])
    else:
        buyers_sum = (buyers_only.groupby(["date","symbol","broker"], as_index=False, sort=False)["net_value"]
                        .sum(min_count=1))
        idx = buyers_sum.groupby(["date","symbol"])["net_value"].idxmax()
        top_rows = buyers_sum.loc[idx].copy()
        top_rows.rename(columns={"broker":"top_buyer","net_value":"top_buyer_net_value"}, inplace=True)
        pos_sum = buyers_sum.groupby(["date","symbol"], as_index=False, sort=False)["net_value"].sum(min_count=1)
        pos_sum.rename(columns={"net_value":"sum_positive"}, inplace=True)
        top = top_rows.merge(pos_sum, on=["date","symbol"], how="left")
        top["top_buyer_concentration"] = np.where(
            top["sum_positive"]>0, top["top_buyer_net_value"]/top["sum_positive"], np.nan
        )
        top.drop(columns=["sum_positive"], inplace=True)

    df["is_retail_broker"] = df["broker"].map(_retail_flag).fillna(0).astype(int)
    retail_ratio = df.groupby(["date","symbol"], as_index=False, sort=False)["is_retail_broker"].mean()
    retail_ratio.rename(columns={"is_retail_broker":"retail_broker_ratio"}, inplace=True)

    out = (total_net
            .merge(num_buyers,  on=["date","symbol"], how="left")
            .merge(num_sellers, on=["date","symbol"], how="left")
            .merge(num_brokers, on=["date","symbol"], how="left")
            .merge(top,         on=["date","symbol"], how="left")
            .merge(retail_ratio,on=["date","symbol"], how="left"))

    cols = ["date","symbol","total_net_value","top_buyer","top_buyer_concentration",
            "top_buyer_net_value","num_buyers","num_sellers","num_brokers","retail_broker_ratio"]
    for c in cols:
        if c not in out.columns: out[c] = np.nan
    out = out[cols].copy()
    out["num_buyers"]  = out["num_buyers"].fillna(0).astype(int)
    out["num_sellers"] = out["num_sellers"].fillna(0).astype(int)
    out["num_brokers"] = out["num_brokers"].fillna(0).astype(int)
    return out.sort_values(["date","symbol"], kind="stable").reset_index(drop=True)

# =========================
# Main
# =========================
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 pipelines/make_broker_agg.py YYYY-MM-DD")
        sys.exit(1)

    date_req = sys.argv[1]  # tanggal yang kamu minta (untuk nama file)
    symbols = _load_symbols()

    # tanggal efektif broker (latest trading date <= requested)
    date_eff = _guess_latest_trading_date(date_req)
    if date_eff != date_req:
        log(f"[broker_agg] note: fallback tanggal {date_req} -> {date_eff}")

    log(f"[broker_agg] date={date_eff} | symbols={len(symbols)}")

    parts: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_broker_summary_robust, sym, date_eff): sym for sym in symbols}
        done = 0
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                df = fut.result()
                if df is not None and not df.empty:
                    parts.append(df)
            except Exception as e:
                log(f"[warn] {sym}: {e}")
            done += 1
            if done % 100 == 0:
                print(f"  .. {done} symbols", flush=True)

    if parts:
        raw_all = pd.concat(parts, ignore_index=True)
        agg = aggregate_broker(raw_all)
    else:
        log("[broker_agg] tidak ada data broker untuk tanggal efektif ini.")
        agg = pd.DataFrame(columns=[
            "date","symbol","total_net_value","top_buyer","top_buyer_concentration",
            "top_buyer_net_value","num_buyers","num_sellers","num_brokers","retail_broker_ratio"
        ])

    # Tambah kolom penanda tanggal asli broker
    agg.rename(columns={"date": "broker_source_date"}, inplace=True)
    agg["broker_source_date"] = pd.to_datetime(agg["broker_source_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Output: filename pakai TANGGAL YANG DIMINTA; isi punya broker_source_date
    out_path = os.path.join(DATA_DIR, f"broker_agg_{date_req}.csv")
    agg.to_csv(out_path, index=False)
    log(f"[broker_agg] wrote {len(agg)} rows -> {out_path}")

if __name__ == "__main__":
    main()
