import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createServer } from "vite";

let vite;
let PredictionSummary;

before(async () => {
  vite = await createServer({ server: { middlewareMode: true, hmr: false, ws: false }, appType: "custom" });
  ({ PredictionSummary } = await vite.ssrLoadModule("/src/systems/prediction/components/PredictionSummary.tsx"));
});
after(async () => vite?.close());

const prediction = {
  cancellation_probability: 0.1096,
  predicted_class: 0,
  model_id: "a".repeat(32),
  status: "experimental",
  business_validated: false,
};

function render(changes = {}) {
  return renderToStaticMarkup(createElement(PredictionSummary, { result: { ...prediction, ...changes } }));
}

test("the result explains the cancellation probability as a French percentage", () => {
  const html = render();
  assert.match(html, /Probabilité estimée de résiliation/);
  assert.match(html, /Pour ce contrat, le modèle estime la probabilité de résiliation à 10,96\s*%/);
  assert.match(html, /une estimation, pas une certitude/);
  assert.doesNotMatch(html, /Classe prédite/);
  assert.match(render({ cancellation_probability: 0 }), /à 0\s*%/);
  assert.match(render({ cancellation_probability: 1 }), /à 100\s*%/);
});

test("synthetic results clearly remain demonstrations", () => {
  const html = render();
  assert.match(html, /entraîné sur des données fictives/);
  assert.match(html, /ne mesure pas le risque réel/);
});

test("real-data models retain the business-validation qualification without a synthetic claim", () => {
  const html = render({ status: "data_validated" });
  assert.match(html, /pas encore été validée pour un usage métier/);
  assert.doesNotMatch(html, /données fictives/);
  assert.doesNotMatch(render({ status: "data_validated", business_validated: true }), /pas encore été validée/);
});
