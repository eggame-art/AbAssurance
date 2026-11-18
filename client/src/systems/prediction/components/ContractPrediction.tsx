import { useEffect, useRef, useState, type FormEvent } from "react";
import { dataController } from "../../data/controllers/dataController";
import { predictionController } from "../controllers/predictionController";
import type { ConsolidatedResponse } from "../../data/models/Consolidated";
import type {
  ContractPredictionResponse,
  PredictionResponse,
} from "../models/Prediction";
import styles from "./PredictionPanel.module.css";

const PAGE_SIZE = 25;

interface ContractPredictionProps {
  onResult: (result: PredictionResponse | null) => void;
}

export function ContractPrediction({ onResult }: ContractPredictionProps) {
  const [clients, setClients] = useState<ConsolidatedResponse | null>(null);
  const [contracts, setContracts] = useState<ConsolidatedResponse | null>(null);
  const [clientOffset, setClientOffset] = useState(0);
  const [contractOffset, setContractOffset] = useState(0);
  const [customer, setCustomer] = useState("");
  const [contract, setContract] = useState("");
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));

  const [loadingClients, setLoadingClients] = useState(true);
  const [loadingContracts, setLoadingContracts] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<ContractPredictionResponse | null>(null);

  const run = useRef<string | undefined>(undefined);
  const active = useRef(true);
  const requestId = useRef(0);

  useEffect(() => {
    active.current = true;

    return () => {
      active.current = false;
      requestId.current++;
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    setLoadingClients(true);
    setClients(null);
    setError("");

    async function loadClients() {
      try {
        const data = await dataController.getConsolidated(
          "clients",
          clientOffset,
          PAGE_SIZE,
          run.current,
          controller.signal,
        );

        if (controller.signal.aborted) return;

        run.current = data.integration_id;
        setClients(data);
      } catch (error) {
        if (controller.signal.aborted) return;

        setError(
          error instanceof Error
            ? error.message
            : "Impossible de charger les clients.",
        );
      } finally {
        if (!controller.signal.aborted) setLoadingClients(false);
      }
    }

    void loadClients();

    return () => controller.abort();
  }, [clientOffset]);

  useEffect(() => {
    if (!customer) return;

    const controller = new AbortController();

    setLoadingContracts(true);
    setContracts(null);
    setError("");

    async function loadContracts() {
      try {
        const data = await dataController.getConsolidated(
          "contracts",
          contractOffset,
          PAGE_SIZE,
          run.current,
          controller.signal,
          customer,
        );

        if (controller.signal.aborted) return;

        setContracts(data);
      } catch (error) {
        if (controller.signal.aborted) return;

        setError(
          error instanceof Error
            ? error.message
            : "Impossible de charger les contrats.",
        );
      } finally {
        if (!controller.signal.aborted) setLoadingContracts(false);
      }
    }

    void loadContracts();

    return () => controller.abort();
  }, [customer, contractOffset]);

  function clearResult() {
    requestId.current++;
    setResult(null);
    onResult(null);
  }

  function selectCustomer(value: string) {
    setCustomer(value);
    setContract("");
    setContracts(null);
    setContractOffset(0);
    clearResult();
  }

  function selectContract(value: string) {
    setContract(value);
    clearResult();
  }

  function changeClientPage(direction: number) {
    selectCustomer("");
    setClientOffset((offset) => offset + direction * PAGE_SIZE);
  }

  function changeContractPage(direction: number) {
    selectContract("");
    setContractOffset((offset) => offset + direction * PAGE_SIZE);
  }

  async function predict(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!run.current || !customer || !contract || submitting) return;

    const currentRequest = ++requestId.current;

    setSubmitting(true);
    setError("");
    setResult(null);
    onResult(null);

    try {
      const data = await predictionController.predictContract({
        customer_id: customer,
        contract_id: contract,
        integration_id: run.current,
        observation_date: date,
      });

      if (!active.current || currentRequest !== requestId.current) return;

      setResult(data);
      onResult(data.prediction);
    } catch (error) {
      if (!active.current || currentRequest !== requestId.current) return;

      setError(
        error instanceof Error
          ? error.message
          : "La prédiction est indisponible.",
      );
    } finally {
      if (active.current) setSubmitting(false);
    }
  }

  return (
    <section>
      {loadingClients && <p role="status">Chargement des clients...</p>}
      {error && <p role="alert">{error}</p>}

      <form className={styles.form} onSubmit={predict}>
        <label>
          Client
          <select
            required
            value={customer}
            disabled={loadingClients || submitting}
            onChange={(event) => selectCustomer(event.target.value)}
          >
            <option value="">Choisir un client</option>

            {clients?.records.map(({ data }) => (
              <option
                key={String(data.customer_id)}
                value={String(data.customer_id)}
              >
                {data.full_name} ({data.source_system}, {data.source_id})
              </option>
            ))}
          </select>
        </label>

        <label>
          Contrat
          <select
            required
            value={contract}
            disabled={!customer || loadingContracts || submitting}
            onChange={(event) => selectContract(event.target.value)}
          >
            <option value="">Choisir un contrat</option>

            {contracts?.records.map(({ data }) => (
              <option
                key={String(data.contract_id)}
                value={String(data.contract_id)}
              >
                {data.source_reference} ({data.product_type},{" "}
                {data.contract_status})
              </option>
            ))}
          </select>
        </label>

        <label>
          Date d’observation
          <input
            type="date"
            required
            max={new Date().toISOString().slice(0, 10)}
            value={date}
            disabled={submitting}
            onChange={(event) => {
              setDate(event.target.value);
              clearResult();
            }}
          />
        </label>

        <button
          type="submit"
          disabled={submitting || !contract || loadingContracts}
        >
          {submitting ? "Calcul en cours..." : "Tester ce contrat"}
        </button>
      </form>

      {clients && (
        <div className={styles.pagination}>
          <button
            type="button"
            disabled={clientOffset === 0 || submitting || loadingClients}
            onClick={() => changeClientPage(-1)}
          >
            Clients précédents
          </button>

          <button
            type="button"
            disabled={
              clientOffset + PAGE_SIZE >= clients.total ||
              submitting ||
              loadingClients
            }
            onClick={() => changeClientPage(1)}
          >
            Clients suivants
          </button>
        </div>
      )}

      {loadingContracts && <p role="status">Chargement des contrats...</p>}
      {contracts && contracts.total === 0 && (
        <p>Aucun contrat pour ce client dans ce snapshot</p>
      )}

      {contracts && contracts.total > PAGE_SIZE && (
        <div className={styles.pagination}>
          <button
            type="button"
            disabled={contractOffset === 0 || submitting}
            onClick={() => changeContractPage(-1)}
          >
            Contrats précédents
          </button>

          <button
            type="button"
            disabled={
              contractOffset + PAGE_SIZE >= contracts.total || submitting
            }
            onClick={() => changeContractPage(1)}
          >
            Contrats suivants
          </button>
        </div>
      )}

      {result && (
        <details open>
          <summary>Caractéristiques calculées par l’API</summary>

          <p>
            Snapshot : {result.snapshot} · Date : {result.observation_date}
          </p>
          <pre>{JSON.stringify(result.features, null, 2)}</pre>
        </details>
      )}
    </section>
  );
}
