export type HealthResp = {
  status: string;
  has_model: boolean;
  model_features: string[] | null;
  target: string | null;
  threshold_default?: number; // <— baru
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
