import test from "node:test";
import assert from "node:assert/strict";
import { EngineRuntime, installGoTimeouts } from "../src/runtime-host.js";

test("an early Go timer yields to the event loop before retrying", async () => {
  let resumes = 0, yielded = false;
  const go = {
    importObject: { gojs: {} }, mem: new DataView(new ArrayBuffer(32)),
    _nextCallbackTimeoutID: 7, _scheduledTimeouts: new Map(), exited: false,
  };
  await new Promise((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error("timer did not complete")), 1000);
    go._resume = () => {
      if (++resumes === 1) { queueMicrotask(() => { yielded = true; }); return; }
      try {
        assert.equal(yielded, true, "a synchronous retry would starve workerd's frozen clock");
        assert.equal(go.mem.getInt32(16, true), 7);
        go._scheduledTimeouts.delete(7);
        clearTimeout(deadline);
        resolve();
      } catch (error) { reject(error); }
    };
    installGoTimeouts(go);
    go.mem.setBigInt64(8, 0n, true);
    go.importObject.gojs["runtime.scheduleTimeoutEvent"](0);
  });
  assert.equal(resumes, 2);
  assert.equal(go._scheduledTimeouts.size, 0);
});

test("WebSocket tracking preserves subclasses and closes only the owning runtime's sockets", async () => {
  const native = globalThis.WebSocket;
  let owned, outside, runtime;
  class FakeWebSocket extends EventTarget {
    closed = false;
    close() { this.closed = true; this.dispatchEvent(new Event("close")); }
  }
  class FakeGo {
    importObject = { gojs: {} };
    _scheduledTimeouts = new Map();
    _resume() {}
    run() {
      owned = new globalThis.WebSocket("wss://owned.test");
      globalThis.__netbirdDO = { terminate: () => { this.exited = true; } };
      return new Promise(() => {});
    }
  }
  try {
    globalThis.WebSocket = FakeWebSocket;
    const module = new WebAssembly.Module(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]));
    runtime = await EngineRuntime.create(module, FakeGo);
    class CustomSocket extends globalThis.WebSocket { custom() { return "ok"; } }
    outside = new CustomSocket("wss://consumer.test");
    assert.ok(outside instanceof CustomSocket);
    assert.equal(outside.custom(), "ok");
    assert.equal(runtime.sockets.size, 1);
    runtime.terminate();
    assert.equal(owned.closed, true);
    assert.equal(outside.closed, false);
    assert.equal(runtime.sockets.size, 0);
  } finally {
    runtime?.terminate();
    outside?.close();
    globalThis.WebSocket = native;
    delete globalThis.__netbirdDO;
  }
});
