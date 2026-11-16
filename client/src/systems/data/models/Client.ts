export type DataSource = "oracle" | "sqlserver";

export type Client = {
  source_id: string;
  name: string | null;
  first_name: string | null;
  email: string | null;
  phone: string | null;
  status: string | null;
};

export type ClientsResponse = {
  source: DataSource;
  data_kind: "demo" | "unspecified";
  clients: Client[];
};
