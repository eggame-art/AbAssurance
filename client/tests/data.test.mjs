import assert from "node:assert/strict";
import { after, afterEach, before, mock, test } from "node:test";
import { createServer } from "vite";

let vite;
let dataController;

before(async () => {
  vite = await createServer({ server: { middlewareMode: true, hmr: false, ws: false }, appType: "custom" });
  ({ dataController } = await vite.ssrLoadModule("/src/systems/data/controllers/dataController.ts"));
});

afterEach(() => mock.restoreAll());
after(async () => vite?.close());

const client = { source_id: "100", name: "Nom source", first_name: null, email: null, phone: "0600000000", status: "ACTIF" };

test("both sources use the common HTTP client and preserve source values", async () => {
  for (const source of ["oracle", "sqlserver"]) {
    const response = { source, data_kind: "demo", clients: [client] };
    const fetch = mock.method(globalThis, "fetch", async () => Response.json(response));
    assert.deepEqual(await dataController.getClients(source), response);
    const [path, options] = fetch.mock.calls[0].arguments;
    assert.equal(path, `/api/data/clients?source=${source}`);
    assert.equal(options.method, "GET");
    assert.equal(options.cache, "no-store");
    mock.restoreAll();
  }
});

test("same original id from different sources is never merged", async () => {
  mock.method(globalThis, "fetch", async (path) => {
    const source = path.endsWith("oracle") ? "oracle" : "sqlserver";
    return Response.json({ source, data_kind: "demo", clients: [{ ...client, name: source }] });
  });
  const oracle = await dataController.getClients("oracle");
  const sqlserver = await dataController.getClients("sqlserver");
  assert.equal(oracle.clients[0].source_id, sqlserver.clients[0].source_id);
  assert.notEqual(`${oracle.source}:${oracle.clients[0].source_id}`, `${sqlserver.source}:${sqlserver.clients[0].source_id}`);
  assert.notEqual(oracle.clients[0].name, sqlserver.clients[0].name);
});

test("empty results and unknown provenance are preserved without fallback data", async () => {
  const response = { source: "oracle", data_kind: "unspecified", clients: [] };
  mock.method(globalThis, "fetch", async () => Response.json(response));
  assert.deepEqual(await dataController.getClients("oracle"), response);
});

test("malformed responses and a response from another source are rejected", async () => {
  for (const response of [
    null, [], {},
    { source: "sqlserver", data_kind: "demo", clients: [client] },
    { source: "oracle", data_kind: ["demo"], clients: [] },
    { source: "oracle", data_kind: "demo", clients: [null] },
    { source: "oracle", data_kind: "demo", clients: [{ ...client, source_id: 100 }] },
    { source: "oracle", data_kind: "demo", clients: [{ ...client, name: { invalid: true } }] },
  ]) {
    mock.method(globalThis, "fetch", async () => Response.json(response));
    await assert.rejects(dataController.getClients("oracle"), /invalid/i);
    mock.restoreAll();
  }
});

test("public API failures reach the caller", async () => {
  mock.method(globalThis, "fetch", async () => Response.json({ detail: "La lecture des clients Oracle a échoué." }, { status: 502 }));
  await assert.rejects(dataController.getClients("oracle"), { message: "La lecture des clients Oracle a échoué." });
});

test("source change cancellation is forwarded without replacing the error", async () => {
  const abort = new AbortController();
  abort.abort();
  mock.method(globalThis, "fetch", async (path, options) => { throw options.signal.reason; });
  await assert.rejects(dataController.getClients("oracle", abort.signal), { name: "AbortError" });
});

const contract = {
  source_reference: "SOURCE-001", source_client_id: "9007199254740993", product_type: "AUTO",
  start_date: "2025-01-01T00:00:00", end_date: null, premium: "12345678.90",
  premium_period: "annual", status: null,
};

test("contracts preserve exact amounts, source ids and premium period", async () => {
  for (const source of ["oracle", "sqlserver"]) {
    const row = { ...contract, premium_period: source === "oracle" ? "annual" : "monthly" };
    const response = { source, data_kind: "demo", contracts: [row] };
    const fetch = mock.method(globalThis, "fetch", async () => Response.json(response));
    assert.deepEqual(await dataController.getContracts(source), response);
    assert.equal(fetch.mock.calls[0].arguments[0], `/api/data/contracts?source=${source}`);
    mock.restoreAll();
  }
});

test("contracts from another source or with the wrong premium unit are rejected", async () => {
  for (const response of [
    { source: "sqlserver", data_kind: "demo", contracts: [contract] },
    { source: "oracle", data_kind: "demo", contracts: [{ ...contract, premium_period: "monthly" }] },
    { source: "oracle", data_kind: "demo", contracts: [{ ...contract, source_client_id: 100 }] },
    { source: "oracle", data_kind: "demo", contracts: [{ ...contract, premium: 12345678.90 }] },
    { source: "oracle", data_kind: "demo", contracts: [null] },
    { source: "oracle", data_kind: "demo", clients: [client] },
  ]) {
    mock.method(globalThis, "fetch", async () => Response.json(response));
    await assert.rejects(dataController.getContracts("oracle"), /invalid/i);
    mock.restoreAll();
  }
});

test("empty contract results stay empty and failures are preserved", async () => {
  const response = { source: "oracle", data_kind: "unspecified", contracts: [] };
  mock.method(globalThis, "fetch", async () => Response.json(response));
  assert.deepEqual(await dataController.getContracts("oracle"), response);
  mock.restoreAll();
  mock.method(globalThis, "fetch", async () => Response.json({ detail: "Contract data is unavailable." }, { status: 502 }));
  await assert.rejects(dataController.getContracts("oracle"), { message: "Contract data is unavailable." });
});

test("changing entity cancels the previous contracts request", async () => {
  const abort = new AbortController();
  abort.abort();
  mock.method(globalThis, "fetch", async (path, options) => { throw options.signal.reason; });
  await assert.rejects(dataController.getContracts("sqlserver", abort.signal), { name: "AbortError" });
});

const claim = {
  source_id: "9007199254740993", source_contract_reference: "SOURCE-001",
  incident_date: "15/03/2025 09:45:00", estimated_amount: "1234567890.12",
  status: null, description: "Description source\nconservée.",
};

test("claims preserve ids, amount, raw date and multiline description", async () => {
  for (const source of ["oracle", "sqlserver"]) {
    const response = { source, data_kind: "demo", claims: [claim] };
    const fetch = mock.method(globalThis, "fetch", async () => Response.json(response));
    assert.deepEqual(await dataController.getClaims(source), response);
    assert.equal(fetch.mock.calls[0].arguments[0], `/api/data/claims?source=${source}`);
    assert.equal(fetch.mock.calls[0].arguments[1].cache, "no-store");
    mock.restoreAll();
  }
});

test("claims from another source or with invalid fields are rejected", async () => {
  for (const response of [
    { source: "sqlserver", data_kind: "demo", claims: [claim] },
    { source: "oracle", data_kind: "demo", claims: [{ ...claim, source_id: 1000 }] },
    { source: "oracle", data_kind: "demo", claims: [{ ...claim, source_contract_reference: null }] },
    { source: "oracle", data_kind: "demo", claims: [{ ...claim, estimated_amount: 100.25 }] },
    { source: "oracle", data_kind: "demo", claims: [{ ...claim, description: {} }] },
    { source: "oracle", data_kind: "demo", contracts: [contract] },
  ]) {
    mock.method(globalThis, "fetch", async () => Response.json(response));
    await assert.rejects(dataController.getClaims("oracle"), /invalid/i);
    mock.restoreAll();
  }
});

test("empty claims and public API errors are preserved", async () => {
  const response = { source: "oracle", data_kind: "unspecified", claims: [] };
  mock.method(globalThis, "fetch", async () => Response.json(response));
  assert.deepEqual(await dataController.getClaims("oracle"), response);
  mock.restoreAll();
  mock.method(globalThis, "fetch", async () => Response.json({ detail: "Claim data is unavailable." }, { status: 502 }));
  await assert.rejects(dataController.getClaims("oracle"), { message: "Claim data is unavailable." });
});

test("changing entity cancels a claims request", async () => {
  const abort = new AbortController();
  abort.abort();
  mock.method(globalThis, "fetch", async (path, options) => { throw options.signal.reason; });
  await assert.rejects(dataController.getClaims("sqlserver", abort.signal), { name: "AbortError" });
});

const payment = { source_id: "00000000-0000-0000-0000-000000002000", source_contract_reference: "AP-AUTO-001", payment_date: "2025-01-05T10:00:00", amount: "100.00", method: "BANK_TRANSFER", status: null };

test("payments preserve UUIDs, amounts and missing status for both sources", async () => {
  for (const source of ["oracle", "sqlserver"]) {
    const response = { source, data_kind: "demo", payments: [payment] };
    const fetch = mock.method(globalThis, "fetch", async () => Response.json(response));
    assert.deepEqual(await dataController.getPayments(source), response);
    assert.equal(fetch.mock.calls[0].arguments[0], `/api/data/payments?source=${source}`);
    mock.restoreAll();
  }
});

test("invalid payment source or numeric amount is rejected", async () => {
  for (const response of [
    { source: "sqlserver", data_kind: "demo", payments: [payment] },
    { source: "oracle", data_kind: "demo", payments: [{ ...payment, amount: 100 }] },
  ]) {
    mock.method(globalThis, "fetch", async () => Response.json(response));
    await assert.rejects(dataController.getPayments("oracle"), /invalid/i);
    mock.restoreAll();
  }
});

const consolidated = {
  entity: "clients", integration_id: "a".repeat(32), snapshot: "b".repeat(32), data_kind: "demo",
  columns: ["customer_id", "source_system", "source_id"], offset: 25, limit: 25, total: 26,
  records: [{ data: { customer_id: "AP-100", source_system: "assureplus", source_id: "100" }, original: { AP_USER_ID: 100 } }],
};

test("consolidated pagination pins the integration and preserves the archived original", async () => {
  const fetch = mock.method(globalThis, "fetch", async () => Response.json(consolidated));
  assert.deepEqual(await dataController.getConsolidated("clients", 25, 25, consolidated.integration_id), consolidated);
  assert.equal(fetch.mock.calls[0].arguments[0], `/api/data/consolidated/clients?offset=25&limit=25&integration_id=${consolidated.integration_id}`);
});

test("wrong entity, integration, page or malformed archived data is rejected", async () => {
  for (const change of [
    { entity: "payments" }, { integration_id: "c".repeat(32) }, { offset: 0 },
    { records: [{ data: {}, original: { AP_USER_ID: { id: 100 } } }] },
  ]) {
    mock.method(globalThis, "fetch", async () => Response.json({ ...consolidated, ...change }));
    await assert.rejects(dataController.getConsolidated("clients", 25, 25, consolidated.integration_id), /invalid/i);
    mock.restoreAll();
  }
});

test("a missing HDFS snapshot is exposed without fallback rows", async () => {
  mock.method(globalThis, "fetch", async () => Response.json({ detail: "No completed consolidated snapshot is available." }, { status: 404 }));
  await assert.rejects(dataController.getConsolidated("clients", 0, 25), /No completed consolidated snapshot/);
});

test('contract selection sends the customer filter and preserves pagination context', async () => {
  const customer = '0ce23490-7b17-57a3-933d-829abb01a605';
  const response = { ...consolidated, entity: 'contracts', records: [{ data: { contract_id: 'contract', customer_id: customer }, original: {} }] };
  const fetch = mock.method(globalThis, 'fetch', async () => Response.json(response));
  assert.deepEqual(await dataController.getConsolidated('contracts', 25, 25, response.integration_id, undefined, customer), response);
  assert.equal(fetch.mock.calls[0].arguments[0], `/api/data/consolidated/contracts?offset=25&limit=25&integration_id=${response.integration_id}&customer_id=${customer}`);
});
