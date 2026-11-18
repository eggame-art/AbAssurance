
import assert from "node:assert/strict";
import { test } from "node:test";

import { checkNetworks } from "./check-networks.mjs";

function config() {
  const roles = {
    frontend: ["edge", "api"],
    backend: ["api", "data", "backend-egress"],
    oracle: ["data"],
    sqlserver: ["data"],
    "sqlserver-init": ["data"],
    "demo-seed": ["data"],
    kafka: ["data"],
    namenode: ["data"],
    datanode: ["data"],
  };

  const services = Object.fromEntries(
    Object.entries(roles).map(([name, networks]) => [
      name,
      {
        networks: Object.fromEntries(
          networks.map((network) => [network, null]),
        ),
      },
    ]),
  );

  services.frontend.ports = [
    { published: "80", target: 80, protocol: "tcp" },
    { published: "443", target: 443, protocol: "tcp" },
    { published: "443", target: 443, protocol: "udp" },
  ];

  services.kafka.environment = {
    KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:19092",
  };

  return {
    services,
    networks: {
      edge: {},
      api: { internal: true },
      data: { internal: true },
      "backend-egress": {},
    },
  };
}

test("accepts the isolated production configuration", () => {
  const model = config();

  assert.doesNotThrow(() => checkNetworks(model));
});

test("rejects a frontend connected directly to data", () => {
  const model = config();
  model.services.frontend.networks.data = null;

  assert.throws(
    () => checkNetworks(model),
    /frontend: unexpected networks/,
  );
});

test("rejects inherited database port publication", () => {
  const model = config();
  model.services.oracle.ports = [
    { target: 1521, published: "1521" },
  ];

  assert.throws(
    () => checkNetworks(model),
    /oracle: published port/,
  );
});

test("rejects a non-internal data network", () => {
  const model = config();
  model.networks.data.internal = false;

  assert.throws(
    () => checkNetworks(model),
    /data: must be internal/,
  );
});

test("rejects host networking and privileged containers", () => {
  const host = config();
  host.services.backend.network_mode = "host";

  assert.throws(
    () => checkNetworks(host),
    /network.mode|network_mode/,
  );

  const privileged = config();
  privileged.services.frontend.privileged = true;

  assert.throws(
    () => checkNetworks(privileged),
    /privileged/,
  );
});
