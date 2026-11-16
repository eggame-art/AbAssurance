import { httpClient } from "../../../shared/http/httpClient";
import type { ServiceStatus } from "../models/ServiceStatus";
import type { IntegrationRun } from "../models/IntegrationRun";

const serviceIds = ["oracle", "sqlserver", "kafka", "spark", "hdfs", "etl"];
const serviceStatuses = ["available", "unavailable", "not_checked"];

function isServiceStatus(value: unknown): value is ServiceStatus {
  if (!value || typeof value !== "object") return false;

  const service = value as Record<string, unknown>;

  return (
    typeof service.id === "string" &&
    serviceIds.includes(service.id) &&
    typeof service.name === "string" &&
    typeof service.status === "string" &&
    serviceStatuses.includes(service.status) &&
    (service.message === null || typeof service.message === "string")
  );
}

export const pipelineService = {
  getLatestIntegration(signal?: AbortSignal): Promise<IntegrationRun | null> {
    return httpClient.get<IntegrationRun | null>(
      "/pipeline/integrations/latest",
      signal,
    );
  },

  startIntegration(token: string): Promise<IntegrationRun> {
    return httpClient.post<IntegrationRun>("/pipeline/integrations", token);
  },

  async getServicesStatus(signal?: AbortSignal): Promise<ServiceStatus[]> {
    const data = await httpClient.get<unknown>("/pipeline/services", signal);

    if (!Array.isArray(data) || !data.every(isServiceStatus)) {
      throw new Error("Invalid monitoring response.");
    }

    return data;
  },
};
