import { useEffect, useRef, useState } from "react";
import { dataController } from "../controllers/dataController";
import type {
  ConsolidatedRecord,
  ConsolidatedResponse,
  DataEntity,
} from "../models/Consolidated";
import styles from "./SourceTables.module.css";

const LIMIT = 25;

const visibleColumns: Record<DataEntity, string[]> = {
  clients: [
    "customer_id",
    "source_system",
    "source_id",
    "full_name",
    "email",
    "customer_status",
  ],
  contracts: [
    "contract_id",
    "source_system",
    "source_reference",
    "customer_id",
    "product_type",
    "start_date",
    "end_date",
    "annual_premium",
    "contract_status",
  ],
  claims: [
    "claim_id",
    "source_system",
    "source_id",
    "contract_id",
    "incident_date",
    "estimated_amount",
    "claim_status",
  ],
  payments: [
    "payment_id",
    "source_system",
    "source_id",
    "contract_id",
    "payment_date",
    "amount",
    "payment_method",
    "transaction_status",
  ],
};

const labels: Record<string, string> = {
  customer_id: "Identifiant client commun",
  source_system: "Origine",
  source_id: "Identifiant source",
  full_name: "Nom complet",
  email: "E-mail",
  customer_status: "Statut client",
  contract_id: "Identifiant contrat commun",
  source_reference: "Référence source",
  product_type: "Type",
  start_date: "Début",
  end_date: "Fin",
  annual_premium: "Prime annuelle",
  contract_status: "Statut contrat",
  claim_id: "Identifiant sinistre commun",
  incident_date: "Date du sinistre",
  estimated_amount: "Montant estimé",
  claim_status: "Statut sinistre",
  payment_id: "Identifiant paiement commun",
  payment_date: "Date du paiement",
  amount: "Montant",
  payment_method: "Mode",
  transaction_status: "Statut paiement",
};

interface ConsolidatedPanelProps {
  entity: DataEntity;
  onLoadingChange: (loading: boolean) => void;
}

export function ConsolidatedPanel({
  entity,
  onLoadingChange,
}: ConsolidatedPanelProps) {
  const [data, setData] = useState<ConsolidatedResponse | null>(null);
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState("");
  const [selected, setSelected] = useState<ConsolidatedRecord | null>(null);

  const snapshot = useRef<string | undefined>(undefined);
  const previousEntity = useRef(entity);

  const active = data !== null && !error;
  const columns = visibleColumns[entity].filter((column) =>
    data?.columns.includes(column),
  );

  useEffect(() => {
    const controller = new AbortController();

    if (previousEntity.current !== entity) {
      snapshot.current = undefined;
      previousEntity.current = entity;
    }

    onLoadingChange(true);
    setData(null);
    setSelected(null);
    setError("");

    async function load() {
      try {
        const result = await dataController.getConsolidated(
          entity,
          offset,
          LIMIT,
          snapshot.current,
          controller.signal,
        );

        if (controller.signal.aborted) return;

        snapshot.current = result.integration_id;
        setData(result);
      } catch (error) {
        if (controller.signal.aborted) return;

        setError(
          error instanceof Error
            ? error.message
            : "Impossible de lire les données HDFS.",
        );
      } finally {
        if (!controller.signal.aborted) onLoadingChange(false);
      }
    }

    void load();

    return () => controller.abort();
  }, [entity, offset, onLoadingChange]);

  return (
    <>
      {!data && !error && (
        <p role="status">Chargement des données consolidées...</p>
      )}
      {error && <p role="alert">{error}</p>}

      {active && data && (
        <>
          {data.data_kind === "demo" && <p>Données de démonstration</p>}

          {data.records.length === 0 ? (
            <p>Aucun enregistrement</p>
          ) : (
            <div className={styles.container}>
              <table className={styles.table} aria-label="Données consolidées">
                <thead>
                  <tr>
                    {columns.map((column) => (
                      <th key={column} scope="col">
                        {labels[column] ?? column}
                      </th>
                    ))}
                    <th scope="col">Comparaison</th>
                  </tr>
                </thead>

                <tbody>
                  {data.records.map((record, index) => (
                    <tr key={`${String(record.data[columns[0]])}-${index}`}>
                      {columns.map((column) => (
                        <td key={column}>
                          {record.data[column] == null
                            ? ""
                            : String(record.data[column])}
                        </td>
                      ))}

                      <td>
                        <button
                          type="button"
                          onClick={() => setSelected(record)}
                        >
                          Comparer à la source
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div className={styles.pagination}>
            <button
              type="button"
              disabled={offset === 0}
              onClick={() => setOffset((value) => Math.max(0, value - LIMIT))}
            >
              Précédent
            </button>

            <span>
              Page {Math.floor(offset / LIMIT) + 1} ({data.total} lignes)
            </span>

            <button
              type="button"
              disabled={offset + LIMIT >= data.total}
              onClick={() => setOffset((value) => value + LIMIT)}
            >
              Suivant
            </button>
          </div>

          {selected && (
            <section>
              <h3>Comparaison de l’enregistrement</h3>

              <button type="button" onClick={() => setSelected(null)}>
                Fermer
              </button>

              <div className={styles.comparison}>
                <div>
                  <h4>Source lors de l’intégration</h4>
                  <pre>{JSON.stringify(selected.original, null, 2)}</pre>
                </div>

                <div>
                  <h4>Modèle commun</h4>
                  <pre>{JSON.stringify(selected.data, null, 2)}</pre>
                </div>
              </div>
            </section>
          )}
        </>
      )}
    </>
  );
}
