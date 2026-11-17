import assert from 'node:assert/strict';
import { after, afterEach, before, mock, test } from 'node:test';
import { createServer } from 'vite';

let vite;
let pipelineController;

before(async () => {
  vite = await createServer({ server: { middlewareMode: true, hmr: false, ws: false }, appType: 'custom' });
  ({ pipelineController } = await vite.ssrLoadModule(
    '/src/systems/integration/controllers/pipelineController.ts',
  ));
});

afterEach(() => mock.restoreAll());
after(async () => vite?.close());

test('controller calls FastAPI through the common client without cache', async () => {
  const rows = [{ id: 'oracle', name: 'Oracle', status: 'available', message: null }];
  const fetch = mock.method(globalThis, 'fetch', async () => Response.json(rows));

  assert.deepEqual(await pipelineController.getServicesStatus(), rows);
  const [path, options] = fetch.mock.calls[0].arguments;
  assert.equal(path, '/api/pipeline/services');
  assert.equal(options.cache, 'no-store');
  assert.equal(options.credentials, 'include');
  assert.equal(options.headers.Accept, 'application/json');
});

test('unavailable and not checked are preserved without inventing availability', async () => {
  const rows = [
    { id: 'oracle', name: 'Oracle', status: 'unavailable', message: 'Connexion refusée.' },
    { id: 'spark', name: 'Spark', status: 'not_checked', message: 'Traitement batch.' },
  ];
  mock.method(globalThis, 'fetch', async () => Response.json(rows));
  assert.deepEqual(await pipelineController.getServicesStatus(), rows);
});

test('public API error text and validation messages reach the page', async () => {
  for (const [detail, expected] of [
    ['Service indisponible.', 'Service indisponible.'],
    [[{ loc: ['body', 'value'], msg: 'Valeur invalide.' }, { msg: 'Champ requis.' }], 'Valeur invalide. Champ requis.'],
  ]) {
    mock.method(globalThis, 'fetch', async () => Response.json({ detail }, { status: 503 }));
    await assert.rejects(pipelineController.getServicesStatus(), { message: expected });
  }
});

test('non JSON errors use a short HTTP message', async () => {
  mock.method(globalThis, 'fetch', async () => new Response('<html>Internal details</html>', { status: 502 }));
  await assert.rejects(pipelineController.getServicesStatus(), { message: 'HTTP error 502.' });
});

test('invalid successful responses are rejected before rendering', async () => {
  for (const data of [
    {},
    null,
    [null],
    [{ id: 'oracle', name: 'Oracle', status: 'unknown', message: null }],
    [{ id: 'oracle', name: 'Oracle', status: ['available'], message: null }],
    [{ id: 'oracle', name: 'Oracle', status: 'available', message: { secret: 'internal' } }],
  ]) {
    mock.method(globalThis, 'fetch', async () => Response.json(data));
    await assert.rejects(pipelineController.getServicesStatus(), /invalid/i);
  }
});

test('network errors do not disclose internal diagnostics', async () => {
  mock.method(globalThis, 'fetch', async () => { throw new Error('private-host private-password'); });
  await assert.rejects(pipelineController.getServicesStatus(), { message: 'Unable to connect to the API.' });
});

test('request timeout has a distinct message', async () => {
  mock.method(AbortSignal, 'timeout', () => AbortSignal.abort(new DOMException('timeout', 'TimeoutError')));
  mock.method(globalThis, 'fetch', async (_path, options) => { throw options.signal.reason; });
  await assert.rejects(pipelineController.getServicesStatus(), { message: 'API request timed out.' });
});

test('unmount cancellation remains an abort instead of an API failure', async () => {
  const controller = new AbortController();
  controller.abort();
  mock.method(globalThis, 'fetch', async (_path, options) => { throw options.signal.reason; });
  await assert.rejects(pipelineController.getServicesStatus(controller.signal), { name: 'AbortError' });
});

test('no previous integration is a valid null response', async () => {
  mock.method(globalThis, 'fetch', async () => Response.json(null));
  assert.equal(await pipelineController.getLatestIntegration(), null);
});

test('integration starts only through a POST with the admin key in a header', async () => {
  const run = { id: 'a'.repeat(32), status: 'queued', stage: 'pending' };
  const fetch = mock.method(globalThis, 'fetch', async () => Response.json(run, { status: 202 }));
  assert.deepEqual(await pipelineController.startIntegration('test-admin-key'), run);
  const [path, options] = fetch.mock.calls[0].arguments;
  assert.equal(path, '/api/pipeline/integrations');
  assert.equal(options.method, 'POST');
  assert.equal(options.headers.Authorization, 'Bearer test-admin-key');
  assert.equal(options.body, undefined);
  assert.ok(!path.includes('test-admin-key'));
});

test('authorization and concurrent integration errors are preserved', async () => {
  for (const [status, detail] of [[401, 'Clé requise.'], [403, 'Accès refusé.'], [409, 'Une intégration est en cours.']]) {
    mock.method(globalThis, 'fetch', async () => Response.json({ detail }, { status }));
    await assert.rejects(pipelineController.startIntegration('test-key'), { message: detail });
  }
});
