import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { createServer } from "vite";

let vite;
let PredictionSummary;
let PersonalPage;

before(async () => {
  vite = await createServer({
    server: { middlewareMode: true, hmr: false, ws: false },
    appType: "custom",
  });
  ({ PredictionSummary } = await vite.ssrLoadModule(
    "/src/systems/prediction/components/PredictionSummary.tsx",
  ));
  ({ PersonalPage } = await vite.ssrLoadModule(
    "/src/systems/auth/pages/PersonalPage.tsx",
  ));
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
  return renderToStaticMarkup(
    createElement(PredictionSummary, { result: { ...prediction, ...changes } }),
  );
}

test("the result shows a plain French score and its original model value", () => {
  const html = render();
  assert.match(html, /Score de résiliation : <strong>10,96\s*%/);
  assert.match(html, /Score brut, entre 0 et 1/);
  assert.match(html, /<dd>0\.1096<\/dd>/);
  assert.match(html, /<dd>a{32}<\/dd>/);
  assert.doesNotMatch(html, /Classe prédite/);
  assert.doesNotMatch(html, /class=/);
  assert.match(render({ cancellation_probability: 0 }), /<strong>0\s*%/);
  assert.match(render({ cancellation_probability: 1 }), /<strong>100\s*%/);
});

test("the details identify the model that actually returned the score", () => {
  const html = render({
    model_id: "b".repeat(32),
    cancellation_probability: 0.27123,
  });
  assert.match(html, /<dd>b{32}<\/dd>/);
  assert.doesNotMatch(html, /<dd>a{32}<\/dd>/);
  assert.match(html, /<strong>27,12\s*%/);
  assert.match(html, /<dd>0\.27123<\/dd>/);
});

test("the percentage comes from the probability rather than the binary class", () => {
  assert.match(
    render({ cancellation_probability: 0.38, predicted_class: 1 }),
    /<strong>38\s*%/,
  );
  assert.match(
    render({ cancellation_probability: 0.81, predicted_class: 0 }),
    /<strong>81\s*%/,
  );
});

test("rounding never turns an intermediate probability into zero or one hundred percent", () => {
  const nearOne = render({ cancellation_probability: 0.99999999 });
  assert.match(nearOne, /Plus de 99,99 %/);
  assert.match(nearOne, /<dd>0\.99999999<\/dd>/);
  assert.doesNotMatch(nearOne, /<strong>100\s*%/);
  const nearZero = render({ cancellation_probability: 0.00000001 });
  assert.match(nearZero, /Moins de 0,01 %/);
  assert.doesNotMatch(nearZero, /<strong>0\s*%/);
});

test("an exact maximum keeps the unrounded value available in the details", () => {
  const html = render({ cancellation_probability: 1 });
  assert.match(html, /<dd>1<\/dd>/);
  assert.match(html, /<strong>100\s*%/);
});

test("personal predictions are accessible from the initial profile without selecting contracts", () => {
  const html = renderToStaticMarkup(createElement(PersonalPage));
  assert.match(html, /aria-label="Mon espace"/);
  assert.match(html, /Faire une prédiction<\/button>/);
  assert.match(html, /<option value="clients" selected=""/);
});
