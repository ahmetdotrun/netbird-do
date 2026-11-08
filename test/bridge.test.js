import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

test("the WASM bridge supports more connections, idle TCP, and large HTTP bodies", () => {
  const result = spawnSync(process.execPath, ["test/bridge/runner.js"], { timeout: 45_000, encoding: "utf8" });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /PASS\s*$/);
});
