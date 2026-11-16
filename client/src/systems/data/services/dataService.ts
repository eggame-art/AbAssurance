import { httpClient } from "../../../shared/http/httpClient";
import type { ClientsResponse, DataSource } from "../models/Client";
import type { ContractsResponse } from "../models/Contract";
import type { ClaimsResponse } from "../models/Claim";
import type { PaymentsResponse } from "../models/Payment";
import type { ConsolidatedResponse, DataEntity } from "../models/Consolidated";

const SNAPSHOT_PATTERN = /^[0-9a-f]{32}$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasNullableStrings(
  row: Record<string, unknown>,
  fields: string[],
): boolean {
  return fields.every(
    (field) => row[field] === null || typeof row[field] === "string",
  );
}

function isSourceResponse(
  data: unknown,
  source: DataSource,
  field: string,
  validate: (row: Record<string, unknown>) => boolean,
): boolean {
  if (!isRecord(data)) return false;
  if (
    data.source !== source ||
    !["demo", "unspecified"].includes(String(data.data_kind))
  )
    return false;

  return (
    Array.isArray(data[field]) &&
    data[field].every((row: unknown) => isRecord(row) && validate(row))
  );
}

function isConsolidatedResponse(
  data: unknown,
  entity: DataEntity,
  offset: number,
  limit: number,
  integrationId?: string,
): data is ConsolidatedResponse {
  if (!isRecord(data)) return false;
  if (
    data.entity !== entity ||
    !Array.isArray(data.columns) ||
    !Array.isArray(data.records)
  )
    return false;
  if (!data.columns.every((column: unknown) => typeof column === "string"))
    return false;
  if (!Number.isSafeInteger(data.total) || (data.total as number) < 0)
    return false;
  if (
    data.offset !== offset ||
    data.limit !== limit ||
    data.records.length > limit
  )
    return false;
  if (
    typeof data.snapshot !== "string" ||
    !SNAPSHOT_PATTERN.test(data.snapshot)
  )
    return false;
  if (
    typeof data.integration_id !== "string" ||
    !SNAPSHOT_PATTERN.test(data.integration_id)
  )
    return false;
  if (integrationId !== undefined && data.integration_id !== integrationId)
    return false;
  if (data.data_kind !== "demo" && data.data_kind !== "unspecified")
    return false;

  return data.records.every((record: unknown) => {
    if (
      !isRecord(record) ||
      !isRecord(record.data) ||
      !isRecord(record.original)
    )
      return false;

    return [
      ...Object.values(record.data),
      ...Object.values(record.original),
    ].every(
      (value) =>
        value === null ||
        typeof value === "string" ||
        typeof value === "boolean" ||
        (typeof value === "number" && Number.isFinite(value)),
    );
  });
}

export const dataService = {
  async getConsolidated(
    entity: DataEntity,
    offset: number,
    limit: number,
    integrationId?: string,
    signal?: AbortSignal,
  ): Promise<ConsolidatedResponse> {
    const query = new URLSearchParams({
      offset: String(offset),
      limit: String(limit),
    });

    if (integrationId) query.set("integration_id", integrationId);

    const data = await httpClient.get<unknown>(
      `/data/consolidated/${entity}?${query}`,
      signal,
      55_000,
    );

    if (!isConsolidatedResponse(data, entity, offset, limit, integrationId)) {
      throw new Error("Invalid consolidated data response.");
    }

    return data;
  },

  async getClients(
    source: DataSource,
    signal?: AbortSignal,
  ): Promise<ClientsResponse> {
    const data = await httpClient.get<unknown>(
      `/data/clients?source=${source}`,
      signal,
    );

    const valid = isSourceResponse(
      data,
      source,
      "clients",
      (row) =>
        typeof row.source_id === "string" &&
        hasNullableStrings(row, [
          "name",
          "first_name",
          "email",
          "phone",
          "status",
        ]),
    );

    if (!valid) throw new Error("Invalid clients response.");

    return data as ClientsResponse;
  },

  async getContracts(
    source: DataSource,
    signal?: AbortSignal,
  ): Promise<ContractsResponse> {
    const data = await httpClient.get<unknown>(
      `/data/contracts?source=${source}`,
      signal,
    );
    const period = source === "oracle" ? "annual" : "monthly";

    const valid = isSourceResponse(
      data,
      source,
      "contracts",
      (row) =>
        typeof row.source_reference === "string" &&
        typeof row.source_client_id === "string" &&
        row.premium_period === period &&
        hasNullableStrings(row, [
          "product_type",
          "start_date",
          "end_date",
          "premium",
          "status",
        ]),
    );

    if (!valid) throw new Error("Invalid contracts response.");

    return data as ContractsResponse;
  },

  async getClaims(
    source: DataSource,
    signal?: AbortSignal,
  ): Promise<ClaimsResponse> {
    const data = await httpClient.get<unknown>(
      `/data/claims?source=${source}`,
      signal,
    );

    const valid = isSourceResponse(
      data,
      source,
      "claims",
      (row) =>
        typeof row.source_id === "string" &&
        typeof row.source_contract_reference === "string" &&
        hasNullableStrings(row, [
          "incident_date",
          "estimated_amount",
          "status",
          "description",
        ]),
    );

    if (!valid) throw new Error("Invalid claims response.");

    return data as ClaimsResponse;
  },

  async getPayments(
    source: DataSource,
    signal?: AbortSignal,
  ): Promise<PaymentsResponse> {
    const data = await httpClient.get<unknown>(
      `/data/payments?source=${source}`,
      signal,
    );

    const valid = isSourceResponse(
      data,
      source,
      "payments",
      (row) =>
        typeof row.source_id === "string" &&
        typeof row.source_contract_reference === "string" &&
        hasNullableStrings(row, ["payment_date", "amount", "method", "status"]),
    );

    if (!valid) throw new Error("Invalid payments response.");

    return data as PaymentsResponse;
  },
};
