import type { ContractPredictionRequest, ContractPredictionResponse } from "../models/Prediction";
import { httpClient } from "../../../shared/http/httpClient";
import type {
  ModelInfo,
  PredictionRequest,
  PredictionResponse,
} from "../models/Prediction";

const MODEL_ID_PATTERN = /^[0-9a-f]{32}$/;

const features = [
  "contract_tenure_days",
  "claim_count",
  "payment_count",
  "payment_amount_known_total",
  "payment_amount_missing_count",
  "days_since_last_payment",
];

const algorithms = ["logistic-regression", "random-forest"];
const statuses = ["experimental", "data_validated"];

function isModelInfo(data: ModelInfo): boolean {
  if (
    !data ||
    typeof data.model_id !== "string" ||
    !MODEL_ID_PATTERN.test(data.model_id)
  )
    return false;
  if (!algorithms.includes(data.algorithm) || !statuses.includes(data.status))
    return false;
  if (
    typeof data.validation_scope !== "string" ||
    typeof data.business_validated !== "boolean"
  )
    return false;
  if (
    data.experimental_warning !== null &&
    typeof data.experimental_warning !== "string"
  )
    return false;
  if (!Array.isArray(data.feature_columns) || data.feature_columns.length === 0)
    return false;
  if (new Set(data.feature_columns).size !== data.feature_columns.length)
    return false;

  return data.feature_columns.every((feature) => features.includes(feature));
}

function isPredictionResponse(data: PredictionResponse): boolean {
  if (!data || typeof data.model_id !== "string") return false;
  if (!Number.isFinite(data.cancellation_probability)) return false;
  if (data.cancellation_probability < 0 || data.cancellation_probability > 1)
    return false;
  if (![0, 1].includes(data.predicted_class)) return false;
  if (
    typeof data.business_validated !== "boolean" ||
    !statuses.includes(data.status)
  )
    return false;

  return true;
}

export const predictionService = {
  async predictContract(body: ContractPredictionRequest): Promise<ContractPredictionResponse> {
    const data = await httpClient.post<ContractPredictionResponse>("/predict/contract", undefined, body, 150_000);
    if (!data || data.customer_id !== body.customer_id || data.contract_id !== body.contract_id
      || data.integration_id !== body.integration_id || data.observation_date !== body.observation_date
      || !data.features || typeof data.features !== "object" || Array.isArray(data.features)
      || !features.every((field) => field in data.features)
      || !Object.values(data.features).every((value) => value === null
        || (typeof value === "number" && Number.isFinite(value) && value >= 0))
      || typeof data.snapshot !== "string" || !MODEL_ID_PATTERN.test(data.snapshot)
      || !isPredictionResponse(data.prediction)) throw new Error("Invalid contract prediction response.");
    return data;
  },
  async getModel(signal?: AbortSignal): Promise<ModelInfo> {
    const data = await httpClient.get<ModelInfo>("/model", signal, 55_000);

    if (!isModelInfo(data)) throw new Error("Invalid model response.");

    return data;
  },

  async predict(features: PredictionRequest): Promise<PredictionResponse> {
    const data = await httpClient.post<PredictionResponse>(
      "/predict",
      undefined,
      features,
      55_000,
    );

    if (!isPredictionResponse(data))
      throw new Error("Invalid prediction response.");

    return data;
  },
};
