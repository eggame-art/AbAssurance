import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const script = fileURLToPath(new URL("../Infrastructure/database/sqlserver/init/wait-ready.sh", import.meta.url));

function run(readyOn) {
  return spawnSync(process.env.BASH_BIN || "bash", ["-c", `
    export counter=0
    probe() {
      counter=$((counter + 1))
      printf 'probe:%s\\n' "$counter" >&3
      [[ "$*" == *"-d master"* && "$*" == *"USE [AssurePlus]"* ]] || exit 2
      ((counter >= ${readyOn}))
    }
    sleep() { :; }
    export -f probe sleep
    bash "$1" probe 3>&1
  `, "readiness-test", script], { encoding: "utf8", timeout: 10_000 });
}

test("waits for recovery failures before allowing initialization", () => {
  const result = run(3);
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /probe:1[\s\S]*probe:2[\s\S]*probe:3/);
  assert.doesNotMatch(result.stdout, /probe:4/);
  assert.match(result.stdout, /database is accessible/);
});

test("returns immediately when SQL Server and the application database are ready", () => {
  const result = run(1);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /probe:1/);
  assert.doesNotMatch(result.stdout, /probe:2/);
});

test("fails after a bounded number of attempts without initializing an unavailable database", () => {
  const result = run(61);
  assert.equal(result.error, undefined);
  assert.equal(result.status, 1);
  assert.equal((result.stdout.match(/probe:/g) || []).length, 60);
  assert.match(result.stderr, /readiness timed out/);
});
