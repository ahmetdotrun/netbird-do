import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

test("WASM relay dials preserve TLS hostnames and recover from asynchronous errors", () => {
  const result = spawnSync(process.execPath, ["test/websocket-error/runner.js"], { timeout: 5000, encoding: "utf8" });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /websocket error returned/);
});
