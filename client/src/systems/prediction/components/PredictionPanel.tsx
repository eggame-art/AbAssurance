import { ContractPrediction } from "./ContractPrediction";
import { PredictionSummary } from "./PredictionSummary";
import { useEffect, useState } from "react";
import { predictionController } from "../controllers/predictionController";
import type {
  Feature,
  ModelInfo,
  PredictionRequest,
  PredictionResponse,
} from "../models/Prediction";
import styles from "./PredictionPanel.module.css";

const labels: Record<Feature, string> = {
  contract_tenure_days: "Ancienneté du contrat (jours)",
  claim_count: "Nombre de sinistres",
  payment_count: "Nombre de paiements",
  payment_amount_known_total: "Total des paiements connus",
  payment_amount_missing_count: "Paiements sans montant",
  days_since_last_payment: "Jours depuis le dernier paiement",
};

export function PredictionPanel() {
  const [mode, setMode] = useState<"contract" | "manual">("contract");
  const [model, setModel] = useState<ModelInfo | null>(null);
  const [values, setValues] = useState<Partial<Record<Feature, string>>>({});
  const [result, setResult] = useState<PredictionResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    setModel(null);
    setResult(null);
    setError("");

    void predictionController
      .getModel(abort.signal)
      .then((data) => {
        if (!abort.signal.aborted) setModel(data);
      })
      .catch((error: unknown) => {
        if (!abort.signal.aborted) {
          setError(
            error instanceof Error ? error.message : "Modèle indisponible.",
          );
        }
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });

    return () => abort.abort();
  }, [refresh]);

  async function predict() {
    if (!model) return;
    setError("");
    setResult(null);
    setSubmitting(true);

    try {
      const features: PredictionRequest = {};
      for (const field of model.feature_columns) {
        const input = values[field]?.trim() ?? "";
        const value = input === "" ? null : Number(input);
        if (
          value !== null &&
          (!Number.isFinite(value) ||
            value < 0 ||
            (field !== "payment_amount_known_total" &&
              !Number.isSafeInteger(value)))
        ) {
          throw new Error(`${labels[field]} : valeur invalide.`);
        }
        features[field] = value;
      }
      setResult(await predictionController.predict(features));
    } catch (error) {
      setError(
        error instanceof Error ? error.message : "La prédiction a échoué.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section>
      <h2>Prédiction de résiliation</h2>
      <button
        type="button"
        disabled={loading || submitting}
        onClick={() => setRefresh((value) => value + 1)}
      >
        Actualiser le modèle
      </button>
      {loading && <p role="status">Chargement du modèle...</p>}
      {error && <p role="alert">{error}</p>}
      <label>
        Mode de test{" "}
        <select
          disabled={submitting}
          value={mode}
          onChange={(event) => {
            setMode(event.target.value as "contract" | "manual");
            setResult(null);
          }}
        >
          <option value="contract">Choisir un client et son contrat</option>
          <option value="manual">Saisie manuelle</option>
        </select>
      </label>
      {model && (!model.business_validated || model.status === "experimental")}
      {model && mode === "contract" && (
        <ContractPrediction
          key={`${model.model_id}:${refresh}`}
          onResult={setResult}
        />
      )}
      {model && mode === "manual" && (
        <>
          <p>
            Modèle : {model.model_id} ({model.algorithm})
          </p>
          {!model.business_validated || model.status === "experimental"}
          <form
            className={styles.form}
            onSubmit={(event) => {
              event.preventDefault();
              void predict();
            }}
          >
            {model.feature_columns.map((field) => (
              <label key={field}>
                {labels[field]}
                <input
                  type="number"
                  min="0"
                  step={field === "payment_amount_known_total" ? "any" : "1"}
                  value={values[field] ?? ""}
                  disabled={submitting}
                  onChange={(event) => {
                    setValues((current) => ({
                      ...current,
                      [field]: event.target.value,
                    }));
                    setResult(null);
                  }}
                />
              </label>
            ))}
            <button type="submit" disabled={submitting}>
              {submitting ? "Calcul..." : "Tester la prédiction"}
            </button>
          </form>
        </>
      )}
      {result && (
        <section aria-label="Résultat de prédiction">
          <PredictionSummary result={result} />
          <details>
            <summary>Réponse API</summary>
            <pre>{JSON.stringify(result, null, 2)}</pre>
          </details>
        </section>
      )}
    </section>
  );
}
