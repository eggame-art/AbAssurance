
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const DOCKER_BIN = process.env.DOCKER_BIN || "docker";
const RELEASE_TAG = process.env.RELEASE_TAG || "local";

const expectedNetworks = {
  frontend: ["api", "edge"],
  backend: ["api", "backend-egress", "data"],
  oracle: ["data"],
  sqlserver: ["data"],
  "sqlserver-init": ["data"],
  kafka: ["data"],
  namenode: ["data"],
  datanode: ["data"],
};

export function checkNetworks(config) {
  assert.deepEqual(
    Object.keys(config.services).sort(),
    Object.keys(expectedNetworks).sort(),
    "Unexpected services",
  );

  for (const [service, networks] of Object.entries(expectedNetworks)) {
    const entry = config.services[service];

    assert.deepEqual(
      Object.keys(entry.networks).sort(),
      [...networks].sort(),
      `${service}: unexpected networks`,
    );

    assert(!entry.network_mode, `${service}: network mode bypasses isolation`);
    assert(!entry.privileged, `${service}: privileged mode bypasses isolation`);

    if (service !== "frontend") {
      assert.equal(entry.ports?.length ?? 0, 0, `${service}: published ports`);
    }
  }

  for (const name of ["api", "data"]) {
    assert.equal(config.networks[name].internal, true, `${name}: must be internal`);
  }

  for (const name of ["edge", "backend-egress"]) {
    assert(!config.networks[name].internal, `${name}: outbound access is required`);
  }

  const ports = config.services.frontend.ports
    .map((port) => `${port.published}:${port.target}/${port.protocol}`)
    .sort();

  assert.deepEqual(ports, ["443:443/tcp", "443:443/udp", "80:80/tcp"]);

  assert.equal(
    config.services.kafka.environment.KAFKA_ADVERTISED_LISTENERS,
    "INTERNAL://kafka:19092",
  );
}

function docker(args) {
  return execFileSync(DOCKER_BIN, args, {
    encoding: "utf8",
    timeout: 120_000,
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function runtimeCheck(config) {
  const prefix = `abassurance-netcheck-${process.pid}-${Date.now()}`;
  const networks = [];
  const containers = [];

  const names = Object.fromEntries(
    Object.keys(config.networks).map((name) => [name, `${prefix}-${name}`]),
  );

  const backendImage = `abassurance-backend:${RELEASE_TAG}`;
  const frontendImage = `abassurance-frontend:${RELEASE_TAG}`;

  function start(role, port) {
    const memberships = Object.keys(config.services[role].networks);
    const name = `${prefix}-${role}`;
    const frontend = role === "frontend";

    const command = frontend
      ? ["-c", "sleep 300"]
      : ["-m", "http.server", String(port)];

    docker([
      "create",
      "--name", name,
      "--network", names[memberships[0]],
      "--network-alias", role,
      "--entrypoint", frontend ? "sh" : "python",
      frontend ? frontendImage : backendImage,
      ...command,
    ]);

    containers.push(name);

    for (const network of memberships.slice(1)) {
      docker(["network", "connect", "--alias", role, names[network], name]);
    }

    docker(["start", name]);

    return name;
  }

  function probe(container, host, port) {
    return spawnSync(
      DOCKER_BIN,
      ["exec", container, "nc", "-z", "-w", "2", host, String(port)],
      { timeout: 5_000 },
    );
  }

  try {
    for (const [name, settings] of Object.entries(config.networks)) {
      docker([
        "network",
        "create",
        ...(settings.internal ? ["--internal"] : []),
        names[name],
      ]);

      networks.push(names[name]);
    }

    const database = start("oracle", 5432);
    const backend = start("backend", 8000);
    const frontend = start("frontend");

    const inspection = JSON.parse(docker(["inspect", database]));
    const address = inspection[0].NetworkSettings.Networks[names.data].IPAddress;

    let available = false;

    for (let attempt = 0; attempt < 15; attempt++) {
      if (probe(frontend, "backend", 8000).status === 0) {
        available = true;
        break;
      }

      docker(["exec", frontend, "sleep", "1"]);
    }

    assert(available, "Frontend cannot reach API");

    docker([
      "exec",
      backend,
      "python",
      "-c",
      "import socket; socket.create_connection(('oracle', 5432), 5).close()",
    ]);

    const denied = probe(frontend, address, 5432);

    assert.equal(denied.error, undefined, "Isolation probe did not finish");
    assert.notEqual(denied.status, 0, "Frontend reached data network by IP");

    docker([
      "exec",
      database,
      "python",
      "-c",
      "from pathlib import Path; assert not any(line.split()[1] == '00000000' for line in Path('/proc/net/route').read_text().splitlines()[1:])",
    ]);

    console.log("Network checks passed: frontend → API → data; direct frontend → data blocked.");
  } finally {
    for (const name of containers.reverse()) {
      try {
        docker(["rm", "-f", name]);
      } catch {
        // Continue cleaning up remaining containers.
      }
    }

    for (const name of networks.reverse()) {
      try {
        docker(["network", "rm", name]);
      } catch {
        // Continue cleaning up remaining networks.
      }
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const compose = process.env.COMPOSE_BIN;

    const config = JSON.parse(
      execFileSync(
        compose || DOCKER_BIN,
        [
          ...(compose ? [] : ["compose"]),
          "-f",
          "compose.production.yaml",
          "config",
          "--format",
          "json",
        ],
        {
          encoding: "utf8",
          stdio: ["ignore", "pipe", "pipe"],
        },
      ),
    );

    checkNetworks(config);

    console.log("Production network configuration passed.");

    if (process.argv.includes("--runtime")) {
      runtimeCheck(config);
    }
  } catch (error) {
    const message = error instanceof assert.AssertionError
      ? error.message
      : "Network check failed; verify Docker and Compose configuration.";

    console.error(message);
    process.exitCode = 1;
  }
}
