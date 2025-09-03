export type HealthResp = {
  status: string;
  has_model: boolean;
  model_features: string[] | null;
  target: string | null;
  threshold_default?: number;
};

export type TickersResp = { tickers: string[] };

export type SnapshotResp = {
  date: string | null;
  rows: Array<Record<string, any>>;
};

export type BrokerAggResp = {
  date: string | null;
  rows: Array<Record<string, any>>;
};

export type PredictResp = {
  symbol: string;
  asof: string;
  prob_up: number;
  label: number;
  threshold_used: number;
  target: string;
  features_used: string[];
};

export type SignalsResp = {
  rows: {
    tanggal: string;
    saham: string;
    sinyal: "BELI" | "JUAL KUAT";
    harga: number;
    akumulasi_pct: number;
    distribusi_pct: number;
    alasan: string;
    top_buyer?: string | null; // <— penting utk filter
  }[];
  from: string;
  to: string;
  threshold: number;
};
