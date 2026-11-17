import { useCallback, useEffect, useState } from "react";
import { IntegrationPanel } from "../components/IntegrationPanel";
import { pipelineController } from "../controllers/pipelineController";
import type { ServiceStatus } from "../models/ServiceStatus";
import styles from "./PipelinePage.module.css";

const statusLabels: Record<ServiceStatus["status"], string> = {
  available: "Available",
  unavailable: "Unavailable",
  not_checked: "Not checked",
};

export function PipelinePage() {
  const [services, setServices] = useState<ServiceStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const loadServices = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError("");

    try {
      const result = await pipelineController.getServicesStatus(signal);

      if (signal?.aborted) return;

      setServices(result);
    } catch (error) {
      if (signal?.aborted) return;

      setError(
        error instanceof Error ? error.message : "Failed to check services.",
      );
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    void loadServices(controller.signal);

    return () => controller.abort();
  }, [loadServices]);

  return (
    <>
      <h1>Data Pipeline</h1>

      <button
        type="button"
        disabled={loading}
        onClick={() => void loadServices()}
      >
        {loading ? "Refreshing..." : "Refresh"}
      </button>

      {loading && <p role="status">Checking services...</p>}
      {error && <p role="alert">{error}</p>}

      {services.length > 0 && (
        <div className={styles.tableContainer}>
          <table className={styles.table}>
            <caption>Service Status</caption>

            <thead>
              <tr>
                <th scope="col">Service</th>
                <th scope="col">Status</th>
                <th scope="col">Message</th>
              </tr>
            </thead>

            <tbody>
              {services.map((service) => (
                <tr key={service.id}>
                  <th scope="row">{service.name}</th>
                  <td>{statusLabels[service.status]}</td>
                  <td>{service.message || ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && services.length === 0 && <p>No services found</p>}

      <IntegrationPanel onFinished={() => void loadServices()} />
    </>
  );
}
