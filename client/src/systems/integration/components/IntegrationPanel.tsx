import { useEffect, useRef, useState, type FormEvent } from "react";
import { pipelineController } from "../controllers/pipelineController";
import type { IntegrationRun } from "../models/IntegrationRun";
import styles from "./IntegrationPanel.module.css";

const statuses = {
  queued: "En attente",
  running: "En cours",
  succeeded: "Terminée",
  failed: "Échec",
};

const stages = {
  pending: "En attente de démarrage",
  etl: "Extraction Apache Hop et publication Kafka",
  spark: "Traitement et stockage des données",
  verification: "Vérification des données consolidées",
  completed: "Intégration terminée",
};

interface IntegrationPanelProps {
  onFinished: () => void;
}

export function IntegrationPanel({ onFinished }: IntegrationPanelProps) {
  const [run, setRun] = useState<IntegrationRun | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  const previousStatus = useRef<IntegrationRun["status"] | null>(null);
  const onFinishedRef = useRef(onFinished);
  onFinishedRef.current = onFinished;

  const active = run?.status === "queued" || run?.status === "running";

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;

    async function refresh() {
      try {
        const latest = await pipelineController.getLatestIntegration(
          controller.signal,
        );

        if (controller.signal.aborted) return;

        const wasActive =
          previousStatus.current === "queued" ||
          previousStatus.current === "running";
        const isActive =
          latest?.status === "queued" || latest?.status === "running";

        previousStatus.current = latest?.status ?? null;

        setRun(latest);
        setError("");

        if (isActive) {
          timer = setTimeout(refresh, 2000);
        } else if (wasActive && latest) {
          onFinishedRef.current();
        }
      } catch (error) {
        if (controller.signal.aborted) return;

        setError(
          error instanceof Error
            ? error.message
            : "Le suivi de l'intégration est indisponible.",
        );
        timer = setTimeout(refresh, 5000);
      }
    }

    void refresh();

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [run?.id]);

  async function start(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (submitting || active) return;

    setSubmitting(true);
    setError("");

    try {
      const started = await pipelineController.startIntegration("");

      previousStatus.current = started.status;
      setRun(started);
    } catch (error) {
      setError(
        error instanceof Error
          ? error.message
          : "Impossible de lancer l'intégration.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className={styles.panel}>
      <h2>Intégration des données</h2>

      <form className={styles.form} onSubmit={start}>
        <button type="submit" disabled={active || submitting}>
          {submitting ? "Démarrage..." : "Lancer l'intégration"}
        </button>
      </form>

      {error && <p role="alert">{error}</p>}

      {run && (
        <>
          <p role="status">{statuses[run.status]}</p>
          <p>{stages[run.stage]}</p>

          <dl className={styles.details}>
            <dt>Début</dt>
            <dd>
              {run.started_at
                ? new Date(run.started_at).toLocaleString("fr-FR")
                : "En attente"}
            </dd>

            {run.finished_at && (
              <>
                <dt>Fin</dt>
                <dd>{new Date(run.finished_at).toLocaleString("fr-FR")}</dd>
              </>
            )}

            {run.snapshot && (
              <>
                <dt>Snapshot HDFS</dt>
                <dd>{run.snapshot}</dd>
              </>
            )}
          </dl>

          {run.error && <p role="alert">{run.error}</p>}

          {run.counts && (
            <ul>
              <li>{run.counts.input} enregistrements traités</li>
              <li>
                {run.counts.accepted.customers} clients,{" "}
                {run.counts.accepted.contracts} contrats
              </li>
              <li>
                {run.counts.accepted.claims} sinistres,{" "}
                {run.counts.accepted.payments} paiements
              </li>
              <li>
                {run.counts.rejected} rejets, dont {run.counts.duplicates}{" "}
                doublons
              </li>
            </ul>
          )}
        </>
      )}
    </section>
  );
}
