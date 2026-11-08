import test from "node:test";
import assert from "node:assert/strict";
import { SessionController } from "../src/session.js";

function fixture() {
  const values = new Map();
  let alarm = null, created = 0, stopped = 0;
  const storage = {
    async get(key) { return structuredClone(values.get(key)); },
    async put(key, value) { values.set(key, structuredClone(value)); },
    async delete(key) { values.delete(key); },
    async setAlarm(value) { alarm = value; },
    async deleteAlarm() { alarm = null; },
  };
  const factory = async expiresAt => {
    created++;
    assert.equal(values.get("netbird:lease").expiresAt, expiresAt, "deadline must be durable before network startup");
    assert.equal(alarm, expiresAt);
    return { async stop() { stopped++; }, async status() { return { state: "running", publicKey: "stable-key" }; } };
  };
  return { storage, factory, values, get alarm() { return alarm; }, get created() { return created; }, get stopped() { return stopped; } };
}

test("passive status reads and inactive alarms never start networking", async () => {
  const f = fixture(), sessions = new SessionController(f.storage, f.factory);
  assert.deepEqual(await sessions.status(), { state: "stopped", expiresAt: null });
  await sessions.alarm();
  await assert.rejects(sessions.get(), /no active session/);
  assert.equal(f.created, 0);
  assert.equal(f.alarm, null);
});

test("concurrent starts create one peer without extending its deadline", async () => {
  const f = fixture(), sessions = new SessionController(f.storage, f.factory);
  await Promise.all([sessions.start(10_000), sessions.start(300_000), sessions.start(20_000)]);
  const before = await sessions.status();
  await sessions.get();
  assert.equal(f.created, 1);
  assert.equal((await sessions.status()).expiresAt, before.expiresAt);
  await sessions.stop();
  assert.equal(f.stopped, 1);
  assert.equal(f.alarm, null);
  await sessions.stop();
  assert.equal(f.stopped, 1);
});

test("a reconstructed object restores only a live session, keeping the same deadline", async () => {
  const f = fixture();
  const first = new SessionController(f.storage, f.factory);
  await first.start(10_000);
  const deadline = (await first.status()).expiresAt;
  const restored = new SessionController(f.storage, f.factory);
  assert.equal((await restored.status()).state, "inactive");
  assert.equal(f.created, 1, "status must not restore the engine");
  await restored.get();
  assert.equal((await restored.status()).expiresAt, deadline);
  f.values.set("netbird:lease", { expiresAt: Date.now() - 1 });
  await restored.alarm();
  assert.equal(f.stopped, 1);
  assert.equal(f.alarm, null);
  await assert.rejects(restored.get(), /no active session/);
  assert.equal(f.created, 2);
});

test("failed startup removes both the lease and its alarm", async () => {
  const f = fixture();
  const sessions = new SessionController(f.storage, async () => { throw new Error("offline"); });
  await assert.rejects(sessions.start(10_000), /offline/);
  assert.equal(f.values.has("netbird:lease"), false);
  assert.equal(f.alarm, null);
});

test("an alarm setup failure prevents network startup", async () => {
  const f = fixture();
  f.storage.setAlarm = async () => { throw new Error("storage unavailable"); };
  const sessions = new SessionController(f.storage, f.factory);
  await assert.rejects(sessions.start(10_000), /storage unavailable/);
  assert.equal(f.created, 0);
  assert.equal(f.values.has("netbird:lease"), false);
});

test("storage cleanup failure still shuts down the active peer", async () => {
  const f = fixture(), sessions = new SessionController(f.storage, f.factory);
  await sessions.start(10_000);
  f.storage.delete = async () => { throw new Error("storage unavailable"); };
  await assert.rejects(sessions.stop(), /storage unavailable/);
  assert.equal(f.stopped, 1);
});

test("persistent mode survives reconstruction and alarms until explicitly stopped", async () => {
  const f = fixture();
  let created = 0, stopped = 0;
  const factory = async deadline => {
    assert.equal(deadline, null);
    assert.equal(f.values.get("netbird:lease").mode, "persistent");
    assert.ok(f.alarm > Date.now());
    created++;
    return { async status() { return { state: "running" }; }, async stop() { stopped++; } };
  };
  const first = new SessionController(f.storage, factory);
  await first.startPersistent();
  await first.startPersistent();
  await first.alarm();
  assert.equal(created, 1);
  assert.equal((await first.status()).expiresAt, null);
  const restored = new SessionController(f.storage, factory);
  assert.equal((await restored.status()).state, "inactive");
  await restored.alarm();
  assert.equal(created, 2);
  assert.equal((await restored.status()).mode, "persistent");
  await restored.stop();
  await restored.alarm();
  assert.equal(stopped, 1);
  assert.equal(created, 2, "stop must disable automatic resurrection");
  assert.equal(f.alarm, null);
});

test("failed persistent startup preserves intent and schedules a slower retry", async () => {
  const f = fixture();
  let attempts = 0;
  const sessions = new SessionController(f.storage, async () => {
    if (++attempts === 1) throw new Error("temporarily offline");
    return { async status() { return { state: "running" }; }, async stop() {} };
  });
  await assert.rejects(sessions.startPersistent(), /temporarily offline/);
  assert.equal(f.values.get("netbird:lease").mode, "persistent");
  assert.ok(f.alarm > Date.now() + 50_000);
  assert.equal((await sessions.status()).state, "failed");
  await sessions.alarm();
  assert.equal((await sessions.status()).state, "running");
  await sessions.stop();
});

test("persistent start cannot open networking if its alarm cannot be saved", async () => {
  const f = fixture();
  f.storage.setAlarm = async () => { throw new Error("storage unavailable"); };
  let opened = false;
  const sessions = new SessionController(f.storage, async () => { opened = true; });
  await assert.rejects(sessions.startPersistent(), /storage unavailable/);
  assert.equal(opened, false);
  assert.equal(f.values.has("netbird:lease"), false);
});

test("concurrent first uses start one persistent peer without an explicit start", async () => {
  const f = fixture();
  let created = 0;
  const sessions = new SessionController(f.storage, async deadline => {
    assert.equal(deadline, null);
    assert.equal(f.values.get("netbird:lease").mode, "persistent");
    assert.ok(f.alarm > Date.now());
    created++;
    return { async status() { return { state: "running" }; }, async stop() {} };
  });
  const peers = await Promise.all([sessions.get(true), sessions.get(true), sessions.get(true)]);
  assert.equal(created, 1);
  assert.equal(peers[0], peers[1]);
  assert.equal((await sessions.status()).mode, "persistent");
  await sessions.stop();
});

test("keepAlive=false starts on demand without background restoration", async () => {
  const f = fixture();
  let created = 0;
  const create = async () => {
    created++;
    return { async status() { return { state: "running" }; }, async stop() {} };
  };
  const sessions = new SessionController(f.storage, create, { keepAlive: () => false });
  await sessions.get(true);
  assert.equal(f.alarm, null);
  const restored = new SessionController(f.storage, create, { keepAlive: () => false });
  await restored.alarm();
  assert.equal(created, 1);
  await restored.get(true);
  assert.equal(created, 2);
  assert.equal(f.alarm, null);
  await restored.stop();
});

test("first use after a peer stops replaces it once and preserves restart intent", async () => {
  const f = fixture();
  const peers = [];
  const sessions = new SessionController(f.storage, async () => {
    assert.ok(peers.every(peer => peer.closed), "stop the old client before opening another identity owner");
    const peer = {
      state: "running", closed: false,
      async status() { return { state: this.state }; },
      async stop() { this.closed = true; this.state = "stopped"; },
    };
    peers.push(peer);
    return peer;
  });
  const first = await sessions.get(true);
  const lease = await f.storage.get("netbird:lease");
  first.state = "stopped";
  const restored = await Promise.all([sessions.get(true), sessions.get(true)]);
  assert.equal(peers.length, 2);
  assert.equal(restored[0], peers[1]);
  assert.equal(restored[0], restored[1]);
  assert.deepEqual(await f.storage.get("netbird:lease"), lease);
  await sessions.stop();
});
