import { useEffect, useRef, useState } from "react";
import type { ConsolidatedResponse } from "../../data/models/Consolidated";
import type {
  ContractPredictionResponse,
  Feature,
} from "../../prediction/models/Prediction";
import { predictionController } from "../../prediction/controllers/predictionController";
import { PredictionSummary } from "../../prediction/components/PredictionSummary";
import { authController } from "../controllers/authController";

const features: Record<Feature, string> = {
  contract_tenure_days: "Ancienneté du contrat (jours)",
  claim_count: "Nombre de sinistres",
  payment_count: "Nombre de paiements",
  payment_amount_known_total: "Total des paiements connus",
  payment_amount_missing_count: "Paiements sans montant renseigné",
  days_since_last_payment: "Jours depuis le dernier paiement",
};

export function PersonalPrediction() {
  const [contracts, setContracts] = useState<ConsolidatedResponse | null>(null);
  const [offset, setOffset] = useState(0);
  const [refresh, setRefresh] = useState(0);
  const [loading, setLoading] = useState(true);
  const [contract, setContract] = useState("");
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [result, setResult] = useState<ContractPredictionResponse | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);

  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    setContracts(null);
    setContract("");
    setResult(null);
    setError("");
    void authController
      .getData("contracts", offset, abort.signal)
      .then((data) => {
        if (abort.signal.aborted) return;
        setContracts(data);
        if (data.records.length === 1)
          setContract(String(data.records[0].data.contract_id));
      })
      .catch((error: unknown) => {
        if (!abort.signal.aborted)
          setError(
            error instanceof Error
              ? error.message
              : "Lecture des contrats impossible.",
          );
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [offset, refresh]);

  async function predict() {
    if (!contracts || !contract || busy || loading) return;
    setBusy(true);
    setError("");
    setResult(null);
    try {
      const response = await predictionController.predictOwnContract({
        contract_id: contract,
        integration_id: contracts.integration_id,
        observation_date: date,
      });
      if (active.current) setResult(response);
    } catch (error) {
      if (active.current)
        setError(
          error instanceof Error ? error.message : "Prédiction indisponible.",
        );
    } finally {
      if (active.current) setBusy(false);
    }
  }

  return (
    <section aria-label="Prédiction sur mes contrats">
      <h2>Prédiction de résiliation</h2>
      {loading && <p role="status">Chargement de mes contrats…</p>}
      {error && <p role="alert">{error}</p>}
      <button
        type="button"
        disabled={busy || loading}
        onClick={() => setRefresh((value) => value + 1)}
      >
        Actualiser mes contrats
      </button>
      {contracts && contracts.total === 0 && <p>Aucun contrat disponible.</p>}
      {contracts && contracts.records.length > 0 && (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void predict();
          }}
        >
          <label>
            Contrat{" "}
            <select
              required
              value={contract}
              disabled={busy}
              onChange={(event) => {
                setContract(event.target.value);
                setResult(null);
                setError("");
              }}
            >
              <option value="">Choisir un contrat</option>
              {contracts.records.map(({ data }) => (
                <option
                  key={String(data.contract_id)}
                  value={String(data.contract_id)}
                >
                  {String(data.source_reference ?? data.contract_id)}
                </option>
              ))}
            </select>
          </label>{" "}
          <label>
            Date d’observation{" "}
            <input
              type="date"
              required
              value={date}
              max={new Date().toISOString().slice(0, 10)}
              disabled={busy}
              onChange={(event) => {
                setDate(event.target.value);
                setResult(null);
                setError("");
              }}
            />
          </label>{" "}
          <button disabled={busy || !contract || !date}>
            {busy ? "Calcul en cours…" : "Estimer la résiliation"}
          </button>
        </form>
      )}
      {contracts && (offset > 0 || contracts.total > contracts.limit) && (
        <p>
          <button
            type="button"
            disabled={busy || loading || offset === 0}
            onClick={() =>
              setOffset((value) => Math.max(0, value - contracts.limit))
            }
          >
            Contrats précédents
          </button>{" "}
          <button
            type="button"
            disabled={
              busy || loading || offset + contracts.limit >= contracts.total
            }
            onClick={() => setOffset((value) => value + contracts.limit)}
          >
            Contrats suivants
          </button>
        </p>
      )}
      {result && (
        <>
          <PredictionSummary result={result.prediction} />
          <details>
            <summary>Données utilisées</summary>
            <p>Date d’observation : {result.observation_date}</p>
            <dl>
              {(
                Object.entries(result.features) as [Feature, number | null][]
              ).map(([name, value]) => (
                <div key={name}>
                  <dt>{features[name]}</dt>
                  <dd>
                    {value === null
                      ? "Valeur absente"
                      : value.toLocaleString("fr-FR")}
                  </dd>
                </div>
              ))}
            </dl>
          </details>
        </>
      )}
    </section>
  );
}
