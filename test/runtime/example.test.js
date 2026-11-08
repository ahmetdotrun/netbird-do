import test from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

async function runtime(bindings = {}, testWorker = false) {
  const dir = testWorker ? "dist/test" : "dist/example";
  const main = `${dir}/${testWorker ? "worker" : "index"}.js`;
  const source = await readFile(main, "utf8");
  const wasm = (await readdir(dir)).find(name => name.endsWith(".wasm") && source.includes(name));
  assert.ok(wasm);
  const options = await convertV4MiniflareOptions({
    name: "netbird-do",
    modules: [{ type: "ESModule", path: main }, { type: "CompiledWasm", path: `${dir}/${wasm}` }],
    compatibilityDate: "2026-10-01",
    durableObjects: { NETBIRD: { className: testWorker ? "Peer" : "ExitNode", useSQLite: true },
      ...(testWorker ? { RAW: { className: "RawPeer", useSQLite: true } } : {}) },
    bindings,
  });
  return new Miniflare(options);
}

test("the exit example needs no admin token and exposes no lifecycle endpoints", async () => {
  const mf = await runtime({ NB_ENABLED: "false", NB_LOCATION_HINT: "weur" });
  try {
    const response = await mf.dispatchFetch("https://example.test/");
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { state: "stopped", enabled: false });
    assert.equal((await mf.dispatchFetch("https://example.test/start", { method: "POST" })).status, 404);
    assert.equal((await mf.dispatchFetch("https://example.test/session", { method: "DELETE" })).status, 404);
    assert.equal((await mf.dispatchFetch("https://example.test/", { method: "POST" })).status, 405);
  } finally { await mf.dispose(); }
});

test("a normal request attempts NetBird startup through the configured management URL", { timeout: 20000 }, async () => {
  const mf = await runtime({ NB_MANAGEMENT_URL: "http://invalid" });
  try {
    const response = await mf.dispatchFetch("https://example.test/");
    assert.equal(response.status, 502, "startup must be attempted without a start request or admin token");
    assert.deepEqual(await response.json(), { error: "NetBird peer could not start" });
  } finally { await mf.dispose(); }
});

test("direct NetBird.start shares concurrent startup, retries failure, and preserves identity", { timeout: 20000 }, async () => {
  const token = "test-runtime-token-".repeat(3);
  const mf = await runtime({ NB_TEST_TOKEN: token }, true);
  try {
    const response = await mf.dispatchFetch("https://example.test/raw-check", { headers: { authorization: `Bearer ${token}` } });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { concurrentStartShared: true, failedStartRetried: true, identityPreserved: true });
  } finally { await mf.dispose(); }
});

test("socket cancellation closes both streams and restarts preserve exclusive identity ownership", { timeout: 20000 }, async () => {
  const token = "test-runtime-token-".repeat(3);
  const mf = await runtime({ NB_TEST_TOKEN: token }, true);
  try {
    const response = await mf.dispatchFetch("https://example.test/lifecycle-check", { headers: { authorization: `Bearer ${token}` } });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      waitedForStop: true, restartShared: true, overlap: false,
      abortClosesReader: true, cancelClosesWriter: true,
      crashedPeerReplaced: true, identityPreserved: true,
    });
  } finally { await mf.dispose(); }
});
