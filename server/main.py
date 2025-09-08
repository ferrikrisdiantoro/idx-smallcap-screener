# main.py
# ============================================
# IDX Up-Move Predictor API (stable JSON + signals + explain)
# - Safe JSONResponse (sanitize deep: NaN/Inf → null, numpy/pandas -> python)
# - /signals menambahkan harga_now & kenaikan_pct
# - /explain memberi alasan sederhana + parameter (volume & broker)
# ============================================

from __future__ import annotations
import os, glob, json
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import joblib

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# ---------- Deep sanitizer ----------
def _sanitize_json(obj):
    """Recursively sanitize any object into JSON-safe types:
       - numpy scalars -> python
       - pandas Timestamps -> isoformat string
       - float NaN/±Inf -> None
       - sets/tuples -> lists
       - dict/list -> deep-sanitized
    """
    # numpy scalars
    if isinstance(obj, (np.integer, np.bool_)):
        return obj.item()
    if isinstance(obj, np.floating):
        x = float(obj)
        if not np.isfinite(x):
            return None
        return x

    # plain floats
    if isinstance(obj, float):
        if not np.isfinite(obj):
            return None
        return obj

    # pandas timey / datetime
    if isinstance(obj, (pd.Timestamp, pd.Timedelta, datetime)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    # pandas NA-like
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    # containers
    if isinstance(obj, dict):
        return {str(k): _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_sanitize_json(v) for v in obj]

    return obj

class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        clean = _sanitize_json(content)
        # allow_nan=False memastikan tidak ada token NaN/Infinity muncul
        return json.dumps(clean, ensure_ascii=False, allow_nan=False).encode("utf-8")

# ---------- Config ----------
load_dotenv()
DATA_DIR   = os.environ.get("DATA_DIR", "data")
MODEL_DIR  = os.environ.get("MODEL_DIR", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "up_model.joblib")

ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000").split(",") if o.strip()
]
THRESHOLD_DEFAULT   = float(os.environ.get("THRESHOLD_DEFAULT", "0.35"))
PREDICT_BATCH_LIMIT = int(os.environ.get("PREDICT_BATCH_LIMIT", "5000"))

app = FastAPI(
    title="IDX Up-Move Predictor API",
    version="0.4.7",
    default_response_class=SafeJSONResponse,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------
def safe_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

def load_latest_file(pattern: str) -> Optional[str]:
    files = sorted(glob.glob(os.path.join(DATA_DIR, pattern)))
    return files[-1] if files else None

def find_agg_on_or_before(date_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    files = sorted(glob.glob(os.path.join(DATA_DIR, "broker_agg_*.csv")))
    if not files:
        return None, None
    if not date_str:
        path = files[-1]
        eff = os.path.basename(path)[11:-4]
        return path, eff
    tgt = pd.to_datetime(date_str).date()
    candidates: List[Tuple[pd.Timestamp, str]] = []
    for p in files:
        d = pd.to_datetime(os.path.basename(p)[11:-4]).date()
        if d <= tgt:
            candidates.append((pd.Timestamp(d), p))
    if not candidates:
        return None, None
    candidates.sort()
    path = candidates[-1][1]
    eff  = os.path.basename(path)[11:-4]
    return path, eff

def load_artifact():
    if not os.path.exists(MODEL_PATH):
        return None
    return joblib.load(MODEL_PATH)

ART = load_artifact()  # {"model","features","target","threshold_default",...}

# === NEW: cari snapshot tepat tanggal atau terakhir ≤ tanggal ===
def find_snapshot_on_or_before(date_str: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Cari file daily_snapshot_YYYY-MM-DD.csv tepat pada 'date_str'.
    Kalau tidak ada, pilih snapshot terakhir yang <= date_str.
    Return: (path, effective_date) atau (None, None) bila tidak ada.
    """
    files = sorted(glob.glob(os.path.join(DATA_DIR, "daily_snapshot_*.csv")))
    if not files:
        return None, None
    if not date_str:
        path = files[-1]
        eff = os.path.basename(path)[15:-4]
        return path, eff
    tgt = pd.to_datetime(date_str).date()
    cand: list[tuple[pd.Timestamp, str]] = []
    for p in files:
        d = pd.to_datetime(os.path.basename(p)[15:-4]).date()
        if d <= tgt:
            cand.append((pd.Timestamp(d), p))
    if not cand:
        return None, None
    cand.sort()
    path = cand[-1][1]
    eff  = os.path.basename(path)[15:-4]
    return path, eff

# ---------- Schemas ----------
class PredictIn(BaseModel):
    features: Dict[str, float]
    threshold: Optional[float] = None

class PredictOut(BaseModel):
    prob_up: float
    label: int
    threshold_used: float
    target: Optional[str]
    features_used: List[str]

class PredictGetOut(PredictOut):
    symbol: str
    asof: str

class PredictBatchIn(BaseModel):
    symbols: Optional[List[str]] = None
    asof: Optional[str] = None
    threshold: Optional[float] = None

# ---------- Predict Utils ----------
def _clf_proba(clf, X: np.ndarray) -> np.ndarray:
    if hasattr(clf, "predict_proba"):
        return clf.predict_proba(X)[:, 1].astype(float)
    s = clf.decision_function(X).astype(float)
    return 1.0 / (1.0 + np.exp(-s))

def build_feature_row_from_snapshot_row(snap_row: Dict[str, Any]) -> Dict[str, float]:
    if ART is None:
        return {}
    out: Dict[str, float] = {}
    for f in ART["features"]:
        v = snap_row.get(f, 0.0)
        try:
            v = 0.0 if pd.isna(v) else float(v)
        except Exception:
            v = 0.0
        out[f] = float(v)
    return out

def predict_batch_from_snapshot(
    snap_df: pd.DataFrame,
    threshold: float,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    if ART is None:
        raise RuntimeError("Model belum dimuat.")
    if snap_df is None or snap_df.empty:
        return pd.DataFrame(columns=["symbol", "asof", "prob_up", "label"])

    df = snap_df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()

    # --- filter bar tidak valid (close<=0 / NaN) ---
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["close"].notna() & (df["close"] > 0)]

    if symbols:
        want = [s.upper() for s in symbols]
        df = df[df["symbol"].isin(want)]
    if len(df) > PREDICT_BATCH_LIMIT:
        df = df.iloc[:PREDICT_BATCH_LIMIT].copy()
    for f in ART["features"]:
        if f not in df.columns:
            df[f] = 0.0
    X = df[ART["features"]].astype(float).fillna(0.0).to_numpy()
    proba = _clf_proba(ART["model"], X)
    label = (proba >= float(threshold)).astype(int)
    out = pd.DataFrame({
        "symbol": df["symbol"].values,
        "asof": df["date"].astype(str).values,
        "prob_up": proba,
        "label": label,
    })
    return out

# ---------- Routes ----------
@app.get("/health")
def health():
    # clamp threshold_default supaya tidak 0
    if ART is not None:
        td = float(ART.get("threshold_default", THRESHOLD_DEFAULT))
    else:
        td = THRESHOLD_DEFAULT
    if not (0.0 < td <= 1.0):
        td = THRESHOLD_DEFAULT if (0.0 < THRESHOLD_DEFAULT <= 1.0) else 0.35

    return {
        "status": "ok",
        "has_model": ART is not None,
        "model_features": (ART.get("features") if ART else None),
        "target": (ART.get("target") if ART else None),
        "threshold_default": td,
        "predict_batch_limit": PREDICT_BATCH_LIMIT,
    }

@app.get("/tickers")
def tickers():
    path = load_latest_file("daily_snapshot_*.csv")
    syms: List[str] = []
    if path:
        df = pd.read_csv(path)
        if "symbol" in df.columns:
            syms = sorted(df["symbol"].astype(str).str.upper().unique().tolist())
    return {"tickers": syms}

@app.get("/snapshot")
def snapshot(date: Optional[str] = Query(default=None, description="YYYY-MM-DD")):
    if date:
        path = os.path.join(DATA_DIR, f"daily_snapshot_{date}.csv")
        if not os.path.exists(path):
            raise HTTPException(404, f"snapshot file not found for {date}")
    else:
        path = load_latest_file("daily_snapshot_*.csv")
        if not path:
            return {"date": None, "rows": []}
        date = os.path.basename(path)[15:-4]
    df = pd.read_csv(path)
    return {"date": date, "rows": safe_rows(df)}

@app.get("/broker-agg")
def broker_agg(date: Optional[str] = Query(default=None, description="YYYY-MM-DD")):
    path, eff = find_agg_on_or_before(date)
    if not path:
        return {"date": None, "rows": []}
    df = pd.read_csv(path)
    return {"date": eff, "rows": safe_rows(df)}

class _PredictGetResponse(PredictGetOut):
    pass

@app.get("/predict", response_model=_PredictGetResponse)
def predict_get(
    symbol: str = Query(..., description="Ticker, mis. BBCA"),
    asof: Optional[str] = Query(None, description="YYYY-MM-DD (opsional)"),
    threshold: Optional[float] = Query(None, ge=0, le=1),
):
    if ART is None:
        raise HTTPException(503, "Model belum tersedia.")
    path = None
    if asof:
        cand = os.path.join(DATA_DIR, f"daily_snapshot_{asof}.csv")
        if os.path.exists(cand):
            path = cand
    if not path:
        path = load_latest_file("daily_snapshot_*.csv")
    if not path:
        raise HTTPException(404, "Snapshot tidak ditemukan.")
    df = pd.read_csv(path)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    sym = symbol.upper().strip()
    sub = df[df["symbol"] == sym]
    if sub.empty:
        raise HTTPException(400, f"Symbol {sym} tidak ada di snapshot.")
    row = sub.iloc[-1].to_dict()
    thr_raw = threshold if threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT)
    thr = float(max(0.01, min(1.0, thr_raw)))
    feats = build_feature_row_from_snapshot_row(row)
    X = np.array([[feats[f] for f in ART["features"]]], dtype=float)
    proba = float(_clf_proba(ART["model"], X)[0])
    label = int(proba >= thr)
    return _PredictGetResponse(
        symbol=sym,
        asof=str(row.get("date")),
        prob_up=proba,
        label=label,
        threshold_used=thr,
        target=ART.get("target"),
        features_used=ART["features"],
    )

@app.post("/predict", response_model=PredictOut)
def predict_post(payload: PredictIn):
    if ART is None:
        raise HTTPException(503, "Model belum tersedia.")
    thr_raw = payload.threshold if payload.threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT)
    thr = float(max(0.01, min(1.0, thr_raw)))
    row = [float(payload.features.get(f, 0.0) or 0.0) for f in ART["features"]]
    X = np.array([row], dtype=float)
    proba = float(_clf_proba(ART["model"], X)[0])
    return PredictOut(
        prob_up=proba,
        label=int(proba >= thr),
        threshold_used=thr,
        target=ART.get("target"),
        features_used=ART["features"],
    )

@app.post("/predict-batch")
def predict_batch(payload: PredictBatchIn):
    if ART is None:
        raise HTTPException(503, "Model belum tersedia.")
    path = None
    if payload.asof:
        cand = os.path.join(DATA_DIR, f"daily_snapshot_{payload.asof}.csv")
        if os.path.exists(cand):
            path = cand
    if not path:
        path = load_latest_file("daily_snapshot_*.csv")
    if not path:
        raise HTTPException(404, "Snapshot tidak ditemukan.")
    snap = pd.read_csv(path)
    thr_raw = payload.threshold if payload.threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT)
    thr = float(max(0.01, min(1.0, thr_raw)))
    pred = predict_batch_from_snapshot(snap, threshold=thr, symbols=payload.symbols)
    return {"rows": safe_rows(pred), "asof": os.path.basename(path)[15:-4], "threshold": thr}

@app.get("/signals")
def signals(
    date_from: str = Query(..., alias="from", description="YYYY-MM-DD"),
    date_to:   str = Query(..., alias="to",   description="YYYY-MM-DD"),
    threshold: float = Query(THRESHOLD_DEFAULT, ge=0, le=1),
    limit_per_day: int = Query(2000, ge=1, le=10000),
):
    """
    Sapu snapshot per-hari di rentang [from..to], hitung sinyal dengan batch predict,
    dan join broker_agg (hanya jika tanggalnya SAMA) bila ada.
    Nilai akumulasi/distribusi DIJAMIN numeric (fallback 0.0).
    Tambahan: harga_now (dari snapshot terbaru) & kenaikan_pct sejak sinyal.
    """
    if ART is None:
        return {"rows": [], "from": date_from, "to": date_to, "threshold": threshold}

    thr = float(max(0.01, min(1.0, threshold)))  # clamp supaya tidak 0

    # --- ambil harga terbaru dari snapshot paling akhir ---
    latest_path = load_latest_file("daily_snapshot_*.csv")
    latest_map: dict[str, float] = {}
    if latest_path:
        _ldf = pd.read_csv(latest_path)
        if not _ldf.empty and "symbol" in _ldf.columns:
            _ldf["symbol"] = _ldf["symbol"].astype(str).str.upper()
            _ldf["close"] = pd.to_numeric(_ldf.get("close"), errors="coerce")
            latest_map = (
                _ldf.dropna(subset=["close"])
                    .set_index("symbol")["close"]
                    .astype(float)
                    .to_dict()
            )

    dates = pd.date_range(pd.to_datetime(date_from), pd.to_datetime(date_to), freq="D")
    all_rows: List[Dict[str, Any]] = []

    for d in dates:
        dstr = d.strftime("%Y-%m-%d")
        path = os.path.join(DATA_DIR, f"daily_snapshot_{dstr}.csv")
        if not os.path.exists(path):
            continue

        snap = pd.read_csv(path)
        if snap.empty or "symbol" not in snap.columns:
            continue
        snap["symbol"] = snap["symbol"].astype(str).str.upper()

        # filter bar tidak valid untuk prediksi
        snap["close"] = pd.to_numeric(snap["close"], errors="coerce")
        snap = snap[snap["close"].notna() & (snap["close"] > 0)]

        # join broker_agg hanya jika tanggalnya SAMA (hindari fitur stale)
        agg_path, eff = find_agg_on_or_before(dstr)
        if agg_path and eff == dstr:
            agg = pd.read_csv(agg_path)
            if not agg.empty and "symbol" in agg.columns:
                agg["symbol"] = agg["symbol"].astype(str).str.upper()
                snap = snap.merge(agg, on="symbol", how="left", suffixes=("", "_agg"))

        uniq = snap["symbol"].unique().tolist()[:limit_per_day]
        sub = snap[snap["symbol"].isin(uniq)].copy()

        pred = predict_batch_from_snapshot(sub, threshold=thr, symbols=None)
        pred = pred.merge(sub, on=["symbol"], how="left")

        for _, r in pred.iterrows():
            sig = "BELI"
            try:
                if float(r.get("ret_1", 0) or 0) <= -0.05:
                    sig = "JUAL KUAT"
                elif float(r["prob_up"]) < thr:
                    continue
            except Exception:
                if float(r["prob_up"]) < thr:
                    continue

            tb_conc_raw = r.get("top_buyer_concentration", 0)
            try:
                tb_conc = float(tb_conc_raw) if not pd.isna(tb_conc_raw) else 0.0
            except Exception:
                tb_conc = 0.0

            akum = max(0.0, tb_conc * 100.0)      # 0..100
            dist = max(0.0, 100.0 - akum)         # 0..100

            # harga saat sinyal (hari dstr)
            harga_raw = r.get("close", 0)
            try:
                harga = float(harga_raw) if not pd.isna(harga_raw) else 0.0
            except Exception:
                harga = 0.0

            # harga terbaru dari snapshot paling akhir (fallback: harga sinyal)
            sym = str(r["symbol"])
            _hnow = latest_map.get(sym)
            try:
                harga_now = float(_hnow) if (_hnow is not None and np.isfinite(_hnow)) else harga
            except Exception:
                harga_now = harga

            # % kenaikan sejak sinyal
            kenaikan_pct = 0.0
            if harga > 0 and harga_now > 0:
                kenaikan_pct = (harga_now / harga - 1.0) * 100.0

            alasan = (
                "Stop loss: harga turun ≥5% dari penutupan"
                if sig == "JUAL KUAT"
                else f"Sinyal model • prob_up={float(r['prob_up']):.2f}"
            )

            all_rows.append({
                "tanggal": dstr,
                "saham": sym,
                "sinyal": sig,
                "harga": harga,
                "harga_now": harga_now,           # NEW
                "kenaikan_pct": kenaikan_pct,     # NEW
                "akumulasi_pct": akum,
                "distribusi_pct": dist,
                "alasan": alasan,
                "top_buyer": (None if pd.isna(r.get("top_buyer")) else str(r.get("top_buyer"))),
            })

    out = pd.DataFrame(all_rows)
    return {
        "rows": safe_rows(out) if not out.empty else [],
        "from": date_from,
        "to": date_to,
        "threshold": thr,
    }

# === NEW: EXPLAIN endpoint ===
@app.get("/explain")
def explain(
    symbol: str = Query(..., description="Ticker, mis. BBCA"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD (opsional)"),
    threshold: Optional[float] = Query(None, ge=0, le=1),
):
    """
    Penjelasan ringkas: keputusan (BELI/JUAL/TIDAK ADA SINYAL) + parameter kunci
    seperti vol_ratio dan konsentrasi top buyer pada tanggal yang diminta.
    """
    sym = symbol.strip().upper()

    # pilih snapshot (tepat date; jika tak ada → terakhir ≤ date; jika date None → latest)
    path, eff_date = (None, None)
    if date:
        cand = os.path.join(DATA_DIR, f"daily_snapshot_{date}.csv")
        if os.path.exists(cand):
            path, eff_date = cand, date
        else:
            path, eff_date = find_snapshot_on_or_before(date)
    else:
        path, eff_date = find_snapshot_on_or_before(None)

    if not path:
        raise HTTPException(404, f"Snapshot tidak ditemukan (date={date or 'latest'}).")

    df = pd.read_csv(path)
    if df.empty or "symbol" not in df.columns:
        raise HTTPException(404, "Snapshot kosong atau tidak valid.")
    df["symbol"] = df["symbol"].astype(str).str.upper()

    row_df = df[df["symbol"] == sym]
    if row_df.empty:
        raise HTTPException(400, f"Symbol {sym} tidak ada di snapshot {eff_date}.")

    row = row_df.iloc[-1].to_dict()

    # broker agg HANYA jika tanggalnya sama persis
    top_buyer = row.get("top_buyer")
    top_buyer_conc = row.get("top_buyer_concentration")
    total_net_value = row.get("total_net_value")

    if (pd.isna(top_buyer) or "top_buyer" not in row_df.columns) and eff_date:
        agg_path, agg_eff = find_agg_on_or_before(eff_date)
        if agg_path and agg_eff == eff_date:
            agg = pd.read_csv(agg_path)
            if "symbol" in agg.columns:
                agg["symbol"] = agg["symbol"].astype(str).str.upper()
                a = agg[agg["symbol"] == sym]
                if not a.empty:
                    arow = a.iloc[-1].to_dict()
                    top_buyer = arow.get("top_buyer", top_buyer)
                    top_buyer_conc = arow.get("top_buyer_concentration", top_buyer_conc)
                    total_net_value = arow.get("total_net_value", total_net_value)

    # normalisasi angka
    def num(v, default=0.0):
        try:
            x = float(v)
            if not np.isfinite(x):
                return default
            return x
        except Exception:
            return default

    close = num(row.get("close"))
    ret_1 = num(row.get("ret_1"))
    vol_ratio = num(row.get("vol_ratio"), default=0.0)
    top_buyer_conc = num(top_buyer_conc, default=0.0)  # 0..1
    total_net_value = num(total_net_value, default=0.0)

    # prediksi model (jika tersedia)
    if ART is not None:
        thr_raw = threshold if threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT)
        thr = float(max(0.01, min(1.0, thr_raw)))
        feats = build_feature_row_from_snapshot_row(row)
        X = np.array([[feats.get(f, 0.0) for f in ART["features"]]], dtype=float)
        prob_up = float(_clf_proba(ART["model"], X)[0])
        label = int(prob_up >= thr)
    else:
        thr = float(max(0.01, min(1.0, threshold if threshold is not None else THRESHOLD_DEFAULT)))
        prob_up = 0.0
        label = 0

    # keputusan sederhana & bullets (bahasa ringan)
    if ret_1 <= -0.05:
        reason_simple = "JUAL KUAT (harga turun ≥5% → disiplin stop-loss)"
    elif prob_up >= thr:
        reason_simple = "BELI (probabilitas naik melewati ambang)"
    else:
        reason_simple = "TIDAK ADA SINYAL BELI (probabilitas masih di bawah ambang)"

    bullets: list[str] = []
    if ART is None:
        bullets.append("Model belum siap, penjelasan berbasis data volume & broker.")
    else:
        bullets.append(f"Model menilai peluang naik {prob_up:.2f} dengan ambang {thr:.2f}.")

    # volume & likuiditas
    if vol_ratio >= 20:
        bullets.append(f"Volume sangat padat (vol ratio {vol_ratio:.2f}× rata-rata 20 hari).")
    elif vol_ratio >= 10:
        bullets.append(f"Volume padat (vol ratio {vol_ratio:.2f}× MA20).")
    elif vol_ratio >= 3:
        bullets.append(f"Volume mulai aktif (vol ratio {vol_ratio:.2f}× MA20).")
    else:
        bullets.append(f"Volume relatif biasa (vol ratio {vol_ratio:.2f}× MA20).")

    # broker concentration
    if top_buyer:
        pct = top_buyer_conc * 100.0
        if pct >= 40:
            bullets.append(f"Ada dominasi {top_buyer} (konsentrasi {pct:.1f}%).")
        elif pct >= 20:
            bullets.append(f"{top_buyer} tampak aktif (konsentrasi {pct:.1f}%).")
        else:
            bullets.append(f"Aktivitas broker tersebar (top buyer {top_buyer}, {pct:.1f}%).")
    else:
        bullets.append("Data broker harian tidak tersedia untuk tanggal ini.")

    # harga & return 1D
    bullets.append(
        f"Harga penutupan {int(close):,} dengan return 1D {ret_1*100:.1f}%."
        .replace(",", ".")  # format lokal ID sederhana
    )

    return {
        "symbol": sym,
        "date": str(eff_date or row.get("date") or ""),
        "close": close,
        "ret_1": ret_1,
        "vol_ratio": vol_ratio,
        "top_buyer": (None if pd.isna(top_buyer) else (str(top_buyer) if top_buyer is not None else None)),
        "top_buyer_concentration": top_buyer_conc,  # 0..1
        "total_net_value": total_net_value,
        "prob_up": prob_up,
        "label": label,
        "threshold_used": thr,
        "reason_simple": reason_simple,
        "bullets": bullets,
    }
