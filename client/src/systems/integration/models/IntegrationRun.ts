export type IntegrationRun = {
  id: string;
  status: "queued" | "running" | "succeeded" | "failed";
  stage: "pending" | "etl" | "spark" | "verification" | "completed";
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  etl_verified_at: string | null;
  spark_verified_at: string | null;
  snapshot: string | null;
  counts: {
    input: number;
    accepted: {
      customers: number;
      contracts: number;
      claims: number;
      payments: number;
    };
    rejected: number;
    duplicates: number;
    invalid: number;
  } | null;
  error: string | null;
};
