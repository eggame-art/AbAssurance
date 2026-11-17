export const httpClient = {
  get<T>(path: string, signal?: AbortSignal, timeoutMs = 10_000): Promise<T> {
    return request<T>(path, "GET", signal, undefined, undefined, timeoutMs);
  },

  post<T>(path: string, token?: string, body?: unknown, timeoutMs = 10_000): Promise<T> {
    return request<T>(path, "POST", undefined, token, body, timeoutMs);
  },
};

async function request<T>(
  path: string,
  method: string,
  signal?: AbortSignal,
  token?: string,
  body?: unknown,
  timeoutMs = 10_000,
): Promise<T> {
  const timeout = AbortSignal.timeout(timeoutMs);
  const requestSignal = signal ? AbortSignal.any([signal, timeout]) : timeout;

  let response: Response;

  try {
    response = await fetch(`/api${path}`, {
      method,
      headers: {
        Accept: "application/json",
        "X-Requested-With": "AbAssurance",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      credentials: "include",
      cache: "no-store",
      signal: requestSignal,
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  } catch (error) {
    if (signal?.aborted) throw error;

    throw new Error(
      timeout.aborted
        ? "API request timed out."
        : "Unable to connect to the API.",
    );
  }

  const data: unknown = await response.json().catch(() => undefined);

  if (!response.ok) {
    const detail =
      data && typeof data === "object" && "detail" in data ? data.detail : null;

    if (typeof window !== "undefined" && response.status === 401 && path !== "/auth/login" && path !== "/auth/me") {
      window.dispatchEvent(new Event("session-expired"));
    }
    if (typeof detail === "string" && detail) throw new Error(detail);

    if (Array.isArray(detail)) {
      const messages = detail.flatMap((error: unknown) =>
        error &&
        typeof error === "object" &&
        "msg" in error &&
        typeof error.msg === "string"
          ? [error.msg]
          : [],
      );

      if (messages.length) throw new Error(messages.join(" "));
    }

    throw new Error(`HTTP error ${response.status}.`);
  }

  if (data === undefined) throw new Error("Invalid API response.");

  return data as T;
}
