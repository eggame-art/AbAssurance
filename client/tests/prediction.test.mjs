import assert from "node:assert/strict";
import { after, afterEach, before, mock, test } from "node:test";
import { createServer } from "vite";

let vite;
let predictionController;

before(async () => {
  vite = await createServer({ server: { middlewareMode: true, hmr: false, ws: false }, appType: "custom" });
  ({ predictionController } = await vite.ssrLoadModule("/src/systems/prediction/controllers/predictionController.ts"));
});
afterEach(() => mock.restoreAll());
after(async () => vite?.close());

const model = {
  model_id: "a".repeat(32), algorithm: "logistic-regression", status: "experimental", business_validated: false,
  validation_scope: "Technical checks", experimental_warning: "Synthetic experiment",
  feature_columns: ["claim_count", "payment_count"], metrics: {},
};
const prediction = { cancellation_probability: 0.3, predicted_class: 0, model_id: model.model_id, status: model.status, business_validated: false };

test("model features and experimental limits are preserved", async () => {
  const fetch = mock.method(globalThis, "fetch", async () => Response.json(model));
  assert.deepEqual(await predictionController.getModel(), model);
  assert.equal(fetch.mock.calls[0].arguments[0], "/api/model");
});

test("prediction uses a JSON POST through FastAPI without a contract id or admin key", async () => {
  const fetch = mock.method(globalThis, "fetch", async () => Response.json(prediction));
  const features = { claim_count: 2, payment_count: null, payment_amount_known_total: 123.45 };
  assert.deepEqual(await predictionController.predict(features), prediction);
  const [path, options] = fetch.mock.calls[0].arguments;
  assert.equal(path, "/api/predict");
  assert.equal(options.method, "POST");
  assert.equal(options.headers["Content-Type"], "application/json");
  assert.equal(options.headers.Authorization, undefined);
  assert.deepEqual(JSON.parse(options.body), features);
});

test("unsupported model features and invalid prediction results are rejected", async () => {
  for (const feature_columns of [[], ["contract_id"], ["claim_count", "claim_count"]]) {
    mock.method(globalThis, "fetch", async () => Response.json({ ...model, feature_columns }));
    await assert.rejects(predictionController.getModel(), /invalid/i);
    mock.restoreAll();
  }
  for (const change of [{ cancellation_probability: 1.1 }, { predicted_class: 2 }, { status: "validated" }, { business_validated: "false" }]) {
    mock.method(globalThis, "fetch", async () => Response.json({ ...prediction, ...change }));
    await assert.rejects(predictionController.predict({}), /invalid/i);
    mock.restoreAll();
  }
});

test("model availability and API feature validation errors reach the caller", async () => {
  mock.method(globalThis, "fetch", async () => Response.json({ detail: "Model runtime is unavailable." }, { status: 503 }));
  await assert.rejects(predictionController.getModel(), { message: "Model runtime is unavailable." });
  mock.restoreAll();
  mock.method(globalThis, "fetch", async () => Response.json({ detail: [{ loc: ["body", "claim_count"], msg: "Input should be greater than or equal to 0" }] }, { status: 422 }));
  await assert.rejects(predictionController.predict({ claim_count: -1 }), /greater than or equal to 0/);
});

const contractInput = { customer_id: '0ce23490-7b17-57a3-933d-829abb01a605', contract_id: '5b128423-a27b-5c20-b078-25cc44399530', integration_id: 'a'.repeat(32), observation_date: '2025-07-01' };
const contractResult = { ...contractInput, snapshot: 'b'.repeat(32), features: { contract_tenure_days: 181, claim_count: 1, payment_count: 1, payment_amount_known_total: 1200, payment_amount_missing_count: 0, days_since_last_payment: 177 }, prediction };

test('contract prediction sends pinned ids and date rather than invented features', async () => {
  const fetch = mock.method(globalThis, 'fetch', async () => Response.json(contractResult));
  assert.deepEqual(await predictionController.predictContract(contractInput), contractResult);
  const [path, options] = fetch.mock.calls[0].arguments;
  assert.equal(path, '/api/predict/contract');
  assert.equal(options.method, 'POST');
  assert.deepEqual(JSON.parse(options.body), contractInput);
});

test('wrong client, snapshot context or invalid calculated values are rejected', async () => {
  for (const change of [{ customer_id: 'other' }, { contract_id: 'other' }, { integration_id: 'c'.repeat(32) }, { features: { ...contractResult.features, claim_count: '1' } }]) {
    mock.method(globalThis, 'fetch', async () => Response.json({ ...contractResult, ...change }));
    await assert.rejects(predictionController.predictContract(contractInput), /invalid/i);
    mock.restoreAll();
  }
});

test('personal prediction sends only the contract, integration and observation date with the session', async () => {
  const fetch = mock.method(globalThis, 'fetch', async () => Response.json(contractResult));
  const { customer_id, ...body } = contractInput;
  assert.deepEqual(await predictionController.predictOwnContract(body), contractResult);
  const [path, options] = fetch.mock.calls[0].arguments;
  assert.equal(path, '/api/me/predict/contract');
  assert.equal(options.method, 'POST');
  assert.equal(options.credentials, 'include');
  assert.deepEqual(JSON.parse(options.body), body);
  assert.equal('customer_id' in JSON.parse(options.body), false);
});

test('personal prediction rejects a mismatched result and reports denied contracts', async () => {
  const { customer_id, ...body } = contractInput;
  for (const change of [{ contract_id: 'other' }, { integration_id: 'c'.repeat(32) },
    { observation_date: '2000-01-01' }, { snapshot: 'invalid' }, { features: {} },
    { prediction: { ...prediction, cancellation_probability: 1.1 } }]) {
    mock.method(globalThis, 'fetch', async () => Response.json({ ...contractResult, ...change }));
    await assert.rejects(predictionController.predictOwnContract(body), /invalid/i);
    mock.restoreAll();
  }
  mock.method(globalThis, 'fetch', async () => Response.json({ detail: 'Contract not found for this customer.' }, { status: 404 }));
  await assert.rejects(predictionController.predictOwnContract(body), /Contract not found/);
});
