// ==== Health ====
export type HealthResp = {
  status?: string;
  has_model: boolean;
  model_features?: string[] | null;
  target: string | null;
  threshold_default?: number;
};

// ==== Tickers ====
export type TickersResp = { tickers: string[] };

// ==== Snapshot ====
export type SnapshotResp = {
  date: string | null;
  // hindari any
  rows: Array<Record<string, unknown>>;
};

// ==== Broker Aggregation ====
export interface BrokerAggRow {
  symbol: string;
  top_buyer: string | null;
  top_buyer_concentration: number; // 0..1
  top_buyer_net_value: number;
  total_net_value: number;
}

export interface BrokerAggResp {
  date: string | null;
  rows: BrokerAggRow[];
}

// ==== Predict (kalau dipakai) ====
export type PredictResp = {
  symbol: string;
  asof: string;
  prob_up: number;
  label: number;
  threshold_used: number;
  target: string;
  features_used: string[];
};

// ==== Signals ====
export type SignalsResp = {
  rows: {
    tanggal: string;
    saham: string;
    sinyal: "BELI" | "JUAL KUAT";
    harga: number;
    akumulasi_pct: number;
    distribusi_pct: number;
    alasan: string;
    top_buyer?: string | null;
  }[];
  from: string;
  to: string;
  threshold: number;
};
