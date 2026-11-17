export type ServiceStatus = {
  id: "oracle" | "sqlserver" | "kafka" | "spark" | "hdfs" | "etl";
  name: string;
  status: "available" | "unavailable" | "not_checked";
  message: string | null;
};
