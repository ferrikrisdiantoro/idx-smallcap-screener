# main.py
# ============================================
# IDX Up-Move Predictor API (stable JSON + signals)
# - Safe JSONResponse (NaN/NumPy friendly)
# - /signals menjamin angka selalu numeric (fallback 0)
# - kirim top_buyer untuk filter broker di UI
# ============================================

from __future__ import annotations
import os, glob, json
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import joblib

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# ---------- Safe JSONResponse (kebal NaN/NumPy) ----------
class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        def default(o):
            # NumPy scalars → Python
            if isinstance(o, (np.floating, np.integer, np.bool_)):
                return o.item()
            # Pandas timey
            if isinstance(o, (pd.Timestamp, pd.Timedelta)):
                return o.isoformat()
            # NaN/NaT → None
            try:
                if pd.isna(o):
                    return None
            except Exception:
                pass
            return str(o)
        # allow_nan=True mencegah crash kalau ada sisa NaN
        return json.dumps(content, ensure_ascii=False, allow_nan=True, default=default).encode("utf-8")

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
    version="0.4.3",
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
    return {
        "status": "ok",
        "has_model": ART is not None,
        "model_features": (ART.get("features") if ART else None),
        "target": (ART.get("target") if ART else None),
        "threshold_default": float(ART.get("threshold_default", THRESHOLD_DEFAULT)) if ART else THRESHOLD_DEFAULT,
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

@app.get("/predict", response_model=PredictGetOut)
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
    thr = float(threshold if threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT))
    feats = build_feature_row_from_snapshot_row(row)
    X = np.array([[feats[f] for f in ART["features"]]], dtype=float)
    proba = float(_clf_proba(ART["model"], X)[0])
    label = int(proba >= thr)
    return PredictGetOut(
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
    thr = float(payload.threshold if payload.threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT))
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
    thr = float(payload.threshold if payload.threshold is not None else ART.get("threshold_default", THRESHOLD_DEFAULT))
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
    dan join broker_agg (≤ tanggal tsb) bila ada.
    Nilai akumulasi/distribusi DIJAMIN numeric (fallback 0.0).
    """
    if ART is None:
        return {"rows": [], "from": date_from, "to": date_to, "threshold": threshold}

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

        # join broker_agg (fallback ≤ tanggal)
        agg_path, _eff = find_agg_on_or_before(dstr)
        agg = pd.read_csv(agg_path) if agg_path else pd.DataFrame()
        if not agg.empty and "symbol" in agg.columns:
            agg["symbol"] = agg["symbol"].astype(str).str.upper()
            snap = snap.merge(agg, on="symbol", how="left", suffixes=("", "_agg"))

        # batasi jumlah simbol per-hari
        uniq = snap["symbol"].unique().tolist()[:limit_per_day]
        sub = snap[snap["symbol"].isin(uniq)].copy()

        # batch predict
        pred = predict_batch_from_snapshot(sub, threshold=threshold, symbols=None)
        pred = pred.merge(sub, on=["symbol"], how="left")

        # compose output rows (sinyal BUY/JUAL KUAT saja)
        for _, r in pred.iterrows():
            sig = "BELI"
            try:
                if float(r.get("ret_1", 0) or 0) <= -0.05:
                    sig = "JUAL KUAT"
                elif float(r["prob_up"]) < float(threshold):
                    continue
            except Exception:
                if float(r["prob_up"]) < float(threshold):
                    continue

            # --- aman-kan angka agar tidak NaN/null di JSON ---
            tb_conc_raw = r.get("top_buyer_concentration", 0)
            try:
                tb_conc = float(tb_conc_raw) if not pd.isna(tb_conc_raw) else 0.0
            except Exception:
                tb_conc = 0.0

            akum = max(0.0, tb_conc * 100.0)      # 0..100
            dist = max(0.0, 100.0 - akum)         # 0..100

            harga_raw = r.get("close", 0)
            try:
                harga = float(harga_raw) if not pd.isna(harga_raw) else 0.0
            except Exception:
                harga = 0.0

            alasan = (
                "Stop loss: harga turun ≥5% dari penutupan"
                if sig == "JUAL KUAT"
                else f"Sinyal model • prob_up={float(r['prob_up']):.2f}"
            )

            all_rows.append({
                "tanggal": dstr,
                "saham": str(r["symbol"]),
                "sinyal": sig,
                "harga": harga,
                "akumulasi_pct": akum,          # <- DIJAMIN NUMBER
                "distribusi_pct": dist,         # <- DIJAMIN NUMBER
                "alasan": alasan,
                "top_buyer": (None if pd.isna(r.get("top_buyer")) else str(r.get("top_buyer"))),
            })

    out = pd.DataFrame(all_rows)
    return {
        "rows": safe_rows(out) if not out.empty else [],
        "from": date_from,
        "to": date_to,
        "threshold": threshold,
    }
