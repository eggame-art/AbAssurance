export type Feature =
  | "contract_tenure_days"
  | "claim_count"
  | "payment_count"
  | "payment_amount_known_total"
  | "payment_amount_missing_count"
  | "days_since_last_payment";

export type PredictionRequest = Partial<Record<Feature, number | null>>;

export type ModelInfo = {
  model_id: string;
  algorithm: "logistic-regression" | "random-forest";
  status: "experimental" | "data_validated";
  business_validated: boolean;
  validation_scope: string;
  experimental_warning: string | null;
  feature_columns: Feature[];
  metrics: Record<string, unknown>;
};

export type PredictionResponse = {
  cancellation_probability: number;
  predicted_class: 0 | 1;
  model_id: string;
  status: "experimental" | "data_validated";
  business_validated: boolean;
};

export type ContractPredictionRequest = {
  customer_id: string;
  contract_id: string;
  integration_id: string;
  observation_date: string;
};

export type ContractPredictionResponse = ContractPredictionRequest & {
  snapshot: string;
  features: PredictionRequest;
  prediction: PredictionResponse;
};
