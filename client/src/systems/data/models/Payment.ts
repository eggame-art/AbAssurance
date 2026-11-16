import type { DataSource } from "./Client";

export type Payment = {
  source_id: string;
  source_contract_reference: string;
  payment_date: string | null;
  amount: string | null;
  method: string | null;
  status: string | null;
};

export type PaymentsResponse = {
  source: DataSource;
  data_kind: "demo" | "unspecified";
  payments: Payment[];
};
