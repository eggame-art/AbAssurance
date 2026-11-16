import type { DataSource } from "./Client";

export type Claim = {
  source_id: string;
  source_contract_reference: string;
  incident_date: string | null;
  estimated_amount: string | null;
  status: string | null;
  description: string | null;
};

export type ClaimsResponse = {
  source: DataSource;
  data_kind: "demo" | "unspecified";
  claims: Claim[];
};
