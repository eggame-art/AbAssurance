import type { PredictionResponse } from "../models/Prediction";
import styles from "./PredictionSummary.module.css";

export function PredictionSummary({ result }: { result: PredictionResponse }) {
  const percentage = new Intl.NumberFormat("fr-FR", {
    style: "percent",
    maximumFractionDigits: 2,
  }).format(result.cancellation_probability);

  return (
    <section className={styles.summary} role="status" aria-label="Résultat de la prédiction">
      <h3>Probabilité estimée de résiliation</h3>
      <p className={styles.percentage}>{percentage}</p>
      <p>
        Pour ce contrat, le modèle estime la probabilité de résiliation à {percentage}.
        Plus ce pourcentage est élevé, plus le modèle considère une résiliation probable.
      </p>
      <p>Ce résultat est une estimation, pas une certitude.</p>
      {result.status === "experimental" ? (
        <p className={styles.notice}>
          Mode démonstration : le modèle a été entraîné sur des données fictives.
          Ce résultat ne mesure pas le risque réel de votre contrat.
        </p>
      ) : !result.business_validated ? (
        <p className={styles.notice}>
          Cette estimation n’a pas encore été validée pour un usage métier.
        </p>
      ) : null}
    </section>
  );
}
