import { useEffect, useState } from "react";
import { authController } from "../controllers/authController";
import { PersonalPrediction } from "../components/PersonalPrediction";
import type {
  ConsolidatedResponse,
  DataEntity,
} from "../../data/models/Consolidated";
import styles from "../../data/components/SourceTables.module.css";

const labels: Record<string, string> = {
  customer_id: "Identifiant client",
  contract_id: "Identifiant contrat",
  claim_id: "Identifiant sinistre",
  payment_id: "Identifiant paiement",
  source_system: "Origine",
  source_id: "Identifiant source",
  source_reference: "Reference source",
  full_name: "Nom complet",
  birth_date: "Date de naissance",
  email: "E-mail",
  phone_number: "Telephone",
  address: "Adresse",
  postal_code: "Code postal",
  tax_number: "Numero fiscal",
  created_at: "Date de creation",
  customer_status: "Statut client",
  loyalty_score: "Score de fidelite",
  product_type: "Type d'assurance",
  start_date: "Debut",
  end_date: "Fin",
  annual_premium: "Prime annuelle",
  contract_status: "Statut contrat",
  agency_id: "Agence",
  broker_code: "Courtier",
  incident_date: "Date du sinistre",
  estimated_amount: "Montant estime",
  claim_status: "Statut sinistre",
  description: "Description",
  fraud_score: "Score de fraude",
  payment_date: "Date du paiement",
  amount: "Montant",
  payment_method: "Mode de paiement",
  transaction_status: "Statut paiement",
};

export function PersonalPage() {
  const [view, setView] = useState<"data" | "prediction">("data");
  const [entity, setEntity] = useState<DataEntity>("clients");
  const [offset, setOffset] = useState(0);
  const [refresh, setRefresh] = useState(0);
  const [data, setData] = useState<ConsolidatedResponse | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (view !== "data") return;
    const abort = new AbortController();
    setLoading(true);
    setData(null);
    setError("");
    void authController
      .getData(entity, offset, abort.signal)
      .then((result) => {
        if (!abort.signal.aborted) setData(result);
      })
      .catch((error: unknown) => {
        if (!abort.signal.aborted)
          setError(
            error instanceof Error ? error.message : "Lecture impossible.",
          );
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [view, entity, offset, refresh]);

  return (
    <section>
      <h1>Mon espace</h1>
      <nav aria-label="Mon espace">
        <button
          type="button"
          aria-pressed={view === "data"}
          onClick={() => setView("data")}
        >
          Mes données
        </button>{" "}
        <button
          type="button"
          aria-pressed={view === "prediction"}
          onClick={() => setView("prediction")}
        >
          Faire une prédiction
        </button>
      </nav>
      {view === "prediction" ? (
        <PersonalPrediction />
      ) : (
        <>
          <h2>Mes données</h2>
          <label>
            Type de données{" "}
            <select
              value={entity}
              onChange={(event) => {
                setOffset(0);
                setEntity(event.target.value as DataEntity);
              }}
            >
              <option value="clients">client</option>
              <option value="contracts">contracts</option>
              <option value="claims">claims</option>
              <option value="payments">payments</option>
            </select>
          </label>{" "}
          <button
            disabled={loading}
            onClick={() => setRefresh((value) => value + 1)}
          >
            Actualiser
          </button>
          {loading && <p role="status">Chargement...</p>}
          {error && <p role="alert">{error}</p>}
          {data && (
            <>
              {data.data_kind === "demo" && <p>Données de démonstration.</p>}
              {data.total === 0 ? (
                <p>Aucun enregistrement.</p>
              ) : (
                <div className={styles.container}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        {data.columns.map((column) => (
                          <th key={column} scope="col">
                            {labels[column] ?? column}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {data.records.map((record, index) => (
                        <tr key={index}>
                          {data.columns.map((column) => (
                            <td key={column}>
                              {String(record.data[column] ?? "")}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              <div className={styles.pagination}>
                <button
                  disabled={offset === 0}
                  onClick={() => setOffset((value) => Math.max(0, value - 25))}
                >
                  Précédent
                </button>
                <span>{data.total} enregistrements</span>
                <button
                  disabled={offset + 25 >= data.total}
                  onClick={() => setOffset((value) => value + 25)}
                >
                  Suivant
                </button>
              </div>
            </>
          )}
        </>
      )}
    </section>
  );
}
