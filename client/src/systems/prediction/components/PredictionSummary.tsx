import type { PredictionResponse } from "../models/Prediction";

export function PredictionSummary({ result }: { result: PredictionResponse }) {
  const score = result.cancellation_probability;
  const percentage =
    score > 0.9999 && score < 1
      ? "Plus de 99,99 %"
      : score > 0 && score < 0.0001
        ? "Moins de 0,01 %"
        : new Intl.NumberFormat("fr-FR", {
            style: "percent",
            maximumFractionDigits: 2,
          }).format(score);

  return (
    <section role="status" aria-label="Résultat de la prédiction">
      <h3>Résultat</h3>
      <p>
        Score de résiliation : <strong>{percentage}</strong>
      </p>
      {result.status === "experimental" ? (
        <p>
          Modèle de démonstration, entraîné sur des données fictives. Risque
          réel non validé.
        </p>
      ) : !result.business_validated ? (
        <p>Estimation non validée pour un usage métier.</p>
      ) : null}
      {score === 1 && (
        <p>
          Le modèle a renvoyé son score maximal. Une résiliation n’est pas
          certaine.
        </p>
      )}
      <details>
        <summary>Détail du résultat</summary>
        <dl>
          <dt>Score brut, entre 0 et 1</dt>
          <dd>{String(score)}</dd>
          <dt>Modèle utilisé</dt>
          <dd>{result.model_id}</dd>
        </dl>
      </details>
    </section>
  );
}
