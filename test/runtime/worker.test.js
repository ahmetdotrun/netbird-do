import test from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const token = "local-test-token-".repeat(3);
const auth = { authorization: `Bearer ${token}` };

async function runtime(bindings = {}, persistence) {
  const source = await readFile("dist/test/worker.js", "utf8");
  const wasm = (await readdir("dist/test")).find(name => name.endsWith(".wasm") && source.includes(name));
  assert.ok(wasm, "bundle the Worker before running runtime tests");
  const options = await convertV4MiniflareOptions({
    name: "netbird-do",
    modules: [{ type: "ESModule", path: "dist/test/worker.js" }, { type: "CompiledWasm", path: `dist/test/${wasm}` }],
    compatibilityDate: "2026-10-01",
    durableObjects: { NETBIRD: { className: "Peer", useSQLite: true } },
    bindings: { NB_TEST_TOKEN: token, ...(process.env.NB_TEST_URL ? { NB_TEST_URL: process.env.NB_TEST_URL } : {}), ...bindings },
  });
  if (persistence) options.resourcePersistencePath = persistence;
  return new Miniflare(options);
}

function call(mf, path, method = "GET", body) {
  return mf.dispatchFetch(`https://local.test${path}`, {
    method, headers: { ...auth, "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body),
  });
}

test("the real Worker rejects unauthorized requests and invalid sessions without starting a peer", async () => {
  const mf = await runtime();
  try {
    assert.equal((await mf.dispatchFetch("https://local.test/session", { method: "POST" })).status, 401);
    assert.deepEqual(await (await call(mf, "/status")).json(), { state: "stopped", expiresAt: null });
    for (const durationMs of [0, 300_001, "60000"]) {
      assert.equal((await call(mf, "/session", "POST", { durationMs })).status, 400);
    }
    for (const body of [null, [], "invalid", { hostname: "unexpected" }]) {
      assert.equal((await call(mf, "/session", "POST", body)).status, 400);
    }
    assert.equal((await call(mf, "/fetch")).status, 503);
    assert.equal((await call(mf, "/self-check")).status, 409);
    assert.deepEqual(await (await call(mf, "/session", "DELETE")).json(), { state: "stopped", expiresAt: null });
  } finally { await mf.dispose(); }
});

test("actual WASM startup to an unreachable server stays within its one-second lease", { timeout: 20_000 }, async () => {
  const mf = await runtime({ NB_MANAGEMENT_URL: "https://127.0.0.1:1" });
  try {
    const start = Date.now();
    const response = await call(mf, "/session", "POST", { durationMs: 1_000 });
    assert.equal(response.status, 502);
    assert.ok(Date.now() - start < 8_000, "startup and forced shutdown must remain bounded");
    assert.deepEqual(await (await call(mf, "/status")).json(), { state: "stopped", expiresAt: null });
    assert.equal((await call(mf, "/self-check")).status, 409);
  } finally { await mf.dispose(); }
});

test("NetBird Cloud enrollment, HTTP binary round trip, identity restore and stop", {
  skip: !process.env.NB_SETUP_KEY,
  timeout: 110_000,
}, async () => {
  const persistence = process.env.NB_TEST_STATE_DIR ?? join(tmpdir(), "netbird-do-live");
  await mkdir(persistence, { recursive: true, mode: 0o700 });
  let mf = await runtime({ NB_SETUP_KEY: process.env.NB_SETUP_KEY, NB_HOSTNAME: "netbird-do-local-test" }, persistence);
  try {
    let response = await call(mf, "/session", "POST", { durationMs: 90_000 });
    const first = await response.json();
    assert.equal(response.status, 200, JSON.stringify(first));
    assert.equal(first.state, "running");
    assert.ok(first.address);
    assert.ok(first.wasmMemoryBytes < 128 * 1024 * 1024, "WASM memory alone must fit the isolate budget");
    response = await call(mf, "/self-check");
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), new Uint8Array([0, 1, 127, 128, 255]));
    if (process.env.NB_TEST_URL) {
      response = await call(mf, "/fetch");
      assert.equal(response.status, 200);
      assert.deepEqual(new Uint8Array(await response.arrayBuffer()), new Uint8Array([0, 1, 127, 128, 255]));
      const connected = await (await call(mf, "/status")).json();
      assert.ok(connected.peers.some(peer => peer.connected && peer.relayed), "cross-peer traffic must use an encrypted relay connection");
    }
    response = await call(mf, "/tcp-self-check");
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), new Uint8Array([0, 1, 127, 128, 255]));
    await mf.dispose();
    mf = await runtime({ NB_HOSTNAME: "netbird-do-local-test" }, persistence);
    const dormant = await (await call(mf, "/status")).json();
    assert.equal(dormant.state, "inactive");
    assert.equal(dormant.expiresAt, first.expiresAt);
    response = await call(mf, "/session", "POST", { durationMs: 300_000 });
    const restored = await response.json();
    assert.equal(response.status, 200, JSON.stringify(restored));
    assert.equal(restored.publicKey, first.publicKey);
    assert.equal(restored.expiresAt, first.expiresAt);
    response = await call(mf, "/self-check");
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), new Uint8Array([0, 1, 127, 128, 255]));
    assert.equal((await call(mf, "/session", "DELETE")).status, 200);
    assert.equal((await (await call(mf, "/status")).json()).state, "stopped");
    response = await call(mf, "/session", "POST", { durationMs: 10_000 });
    const short = await response.json();
    assert.equal(response.status, 200, JSON.stringify(short));
    assert.equal(short.publicKey, first.publicKey);
    await new Promise(resolve => setTimeout(resolve, Math.max(1, short.expiresAt - Date.now() + 500)));
    assert.equal((await (await call(mf, "/status")).json()).state, "stopped");
    assert.equal((await call(mf, "/self-check")).status, 409);
    console.log(JSON.stringify({ livePeer: restored.fqdn, wasmMemoryBytes: restored.wasmMemoryBytes, identityRestored: true, automaticShutdown: true }));
  } finally {
    try { await call(mf, "/session", "DELETE"); } catch {}
    await mf.dispose();
  }
});
