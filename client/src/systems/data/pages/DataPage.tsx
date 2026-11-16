import { useEffect, useState } from "react";
import { dataController } from "../controllers/dataController";
import {
  ClaimsTable,
  ClientsTable,
  ContractsTable,
  PaymentsTable,
} from "../components/SourceTables";
import { ConsolidatedPanel } from "../components/ConsolidatedPanel";
import { PredictionPanel } from "../../prediction/components/PredictionPanel";
import type { ClientsResponse, DataSource } from "../models/Client";
import type { ContractsResponse } from "../models/Contract";
import type { ClaimsResponse } from "../models/Claim";
import type { PaymentsResponse } from "../models/Payment";
import styles from "./DataPage.module.css";

const entityLabels = {
  clients: "Clients",
  contracts: "Contrats",
  claims: "Sinistres",
  payments: "Paiements",
};

const readers = {
  clients: dataController.getClients,
  contracts: dataController.getContracts,
  claims: dataController.getClaims,
  payments: dataController.getPayments,
};

type Entity = keyof typeof entityLabels;

export function DataPage() {
  const [tab, setTab] = useState<"data" | "prediction">("data");
  const [source, setSource] = useState<DataSource | "consolidated">("oracle");
  const [entity, setEntity] = useState<Entity>("clients");
  const [data, setData] = useState<
    | ClientsResponse
    | ContractsResponse
    | ClaimsResponse
    | PaymentsResponse
    | null
  >(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    if (tab !== "data" || source === "consolidated") return;
    const controller = new AbortController();
    setLoading(true);
    setError("");
    setData(null);

    const request = readers[entity](source, controller.signal);

    void request
      .then((result) => {
        if (!controller.signal.aborted) setData(result);
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            error instanceof Error
              ? error.message
              : "La lecture des données a échoué",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [source, entity, refresh, tab]);

  return (
    <>
      <h1>Données et prédictions</h1>
      <div className={styles.tabs} aria-label="Consultation et prédictions">
        <button
          type="button"
          aria-pressed={tab === "data"}
          onClick={() => setTab("data")}
        >
          Données
        </button>
        <button
          type="button"
          aria-pressed={tab === "prediction"}
          onClick={() => setTab("prediction")}
        >
          Prédictions
        </button>
      </div>

      {tab === "prediction" ? (
        <PredictionPanel />
      ) : (
        <section>
          <h2>{entityLabels[entity]}</h2>
          <div className={styles.controls}>
            <label htmlFor="client-source">Source</label>
            <select
              id="client-source"
              value={source}
              onChange={(event) => {
                setData(null);
                setError("");
                setLoading(true);
                setSource(event.target.value as DataSource | "consolidated");
              }}
            >
              <option value="oracle">Oracle (AbAssurance)</option>
              <option value="sqlserver">SQL Server (AssurePlus)</option>
              <option value="consolidated">Données unifiées (HDFS)</option>
            </select>
            <label htmlFor="data-entity">Type de données</label>
            <select
              id="data-entity"
              value={entity}
              onChange={(event) => {
                setData(null);
                setError("");
                setLoading(true);
                setEntity(event.target.value as Entity);
              }}
            >
              <option value="clients">Clients</option>
              <option value="contracts">Contrats</option>
              <option value="claims">Sinistres</option>
              <option value="payments">Paiements</option>
            </select>
            <button
              type="button"
              disabled={loading}
              onClick={() => setRefresh((value) => value + 1)}
            >
              Actualiser
            </button>
          </div>

          {source === "consolidated" && (
            <ConsolidatedPanel
              key={`${entity}:${refresh}`}
              entity={entity}
              onLoadingChange={setLoading}
            />
          )}
          {source !== "consolidated" && loading && (
            <p role="status">Chargement des données...</p>
          )}
          {error && <p role="alert">{error}</p>}
          {!loading && !error && data?.source === source && (
            <>
              {data.data_kind === "demo"}
              {entity === "clients" &&
                "clients" in data &&
                (data.clients.length === 0 ? (
                  <p>Aucun client.</p>
                ) : (
                  <ClientsTable data={data} />
                ))}
              {entity === "contracts" &&
                "contracts" in data &&
                (data.contracts.length === 0 ? (
                  <p>Aucun contrat.</p>
                ) : (
                  <ContractsTable data={data} />
                ))}
              {entity === "claims" &&
                "claims" in data &&
                (data.claims.length === 0 ? (
                  <p>Aucun sinistre.</p>
                ) : (
                  <ClaimsTable data={data} />
                ))}
              {entity === "payments" &&
                "payments" in data &&
                (data.payments.length === 0 ? (
                  <p>Aucun paiement.</p>
                ) : (
                  <PaymentsTable data={data} />
                ))}
            </>
          )}
        </section>
      )}
    </>
  );
}
