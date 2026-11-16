export type DataEntity = "clients" | "contracts" | "claims" | "payments";
export type Scalar = string | number | boolean | null;
export type ConsolidatedRecord = {
  data: Record<string, Scalar>;
  original: Record<string, Scalar>;
};

export type ConsolidatedResponse = {
  entity: DataEntity;
  integration_id: string;
  snapshot: string;
  data_kind: "demo" | "unspecified";
  columns: string[];
  records: ConsolidatedRecord[];
  offset: number;
  limit: number;
  total: number;
};
