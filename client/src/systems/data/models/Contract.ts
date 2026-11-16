import type { DataSource } from "./Client";

export type Contract = {
  source_reference: string;
  source_client_id: string;
  product_type: string | null;
  start_date: string | null;
  end_date: string | null;
  premium: string | null;
  premium_period: "annual" | "monthly";
  status: string | null;
};

export type ContractsResponse = {
  source: DataSource;
  data_kind: "demo" | "unspecified";
  contracts: Contract[];
};
