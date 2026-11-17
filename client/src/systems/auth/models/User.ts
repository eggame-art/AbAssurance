export type User = {
  username: string;
  role: "admin" | "user";
  source: string | null;
  source_id: string | null;
};
