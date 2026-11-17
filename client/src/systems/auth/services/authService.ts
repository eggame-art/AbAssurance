import { httpClient } from "../../../shared/http/httpClient";
import type { User } from "../models/User";
import type {
  DataEntity,
  ConsolidatedResponse,
} from "../../data/models/Consolidated";

const DATA_LIMIT = 25;
const DATA_TIMEOUT = 55_000;

export const authService = {
  getSession(): Promise<User> {
    return httpClient.get<User>("/auth/me");
  },

  login(username: string, password: string): Promise<User> {
    return httpClient.post<User>("/auth/login", undefined, {
      username,
      password,
    });
  },

  logout(): Promise<{ ok: boolean }> {
    return httpClient.post<{ ok: boolean }>("/auth/logout");
  },

  getData(
    entity: DataEntity,
    offset: number,
    signal?: AbortSignal,
  ): Promise<ConsolidatedResponse> {
    return httpClient.get<ConsolidatedResponse>(
      `/me/data/${entity}?offset=${offset}&limit=${DATA_LIMIT}`,
      signal,
      DATA_TIMEOUT,
    );
  },
};
