import { useEffect, useRef, useState } from "react";
import type { ConsolidatedResponse } from "../../data/models/Consolidated";
import type { PredictionResponse } from "../../prediction/models/Prediction";
import { predictionController } from "../../prediction/controllers/predictionController";
import { PredictionSummary } from "../../prediction/components/PredictionSummary";

export function PersonalPrediction({ contracts }: { contracts: ConsolidatedResponse }) {
  const [contract, setContract] = useState("");
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [result, setResult] = useState<PredictionResponse | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => { active.current = false; };
  }, []);

  async function predict() {
    setBusy(true); setError(""); setResult(null);
    try {
      const response = await predictionController.predictOwnContract({
        contract_id: contract, integration_id: contracts.integration_id, observation_date: date,
      });
      if (active.current) setResult(response.prediction);
    } catch (error) {
      if (active.current) setError(error instanceof Error ? error.message : "Prédiction indisponible.");
    } finally {
      if (active.current) setBusy(false);
    }
  }

  return (
    <section aria-label="Prédiction sur mes contrats">
      <h2>Estimer le risque de résiliation</h2>
      <p>Choisissez un de vos contrats pour obtenir une estimation de sa probabilité de résiliation.</p>
      <form onSubmit={(event) => { event.preventDefault(); void predict(); }}>
        <label>Contrat{" "}
          <select required value={contract} disabled={busy} onChange={(event) => {
            setContract(event.target.value); setResult(null); setError("");
          }}>
            <option value="">Choisir un contrat de cette page</option>
            {contracts.records.map(({ data }) => (
              <option key={String(data.contract_id)} value={String(data.contract_id)}>
                {String(data.source_reference ?? data.contract_id)}
              </option>
            ))}
          </select>
        </label>{" "}
        <label>Date d’observation{" "}
          <input type="date" required value={date} max={new Date().toISOString().slice(0, 10)} disabled={busy}
            onChange={(event) => { setDate(event.target.value); setResult(null); setError(""); }} />
        </label>{" "}
        <button disabled={busy || !contract || !date}>{busy ? "Calcul en cours…" : "Estimer la résiliation"}</button>
      </form>
      {error && <p role="alert">{error}</p>}
      {result && <PredictionSummary result={result} />}
    </section>
  );
}
