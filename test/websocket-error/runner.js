import fs from "node:fs";
import assert from "node:assert/strict";
import "../../src/wasm_exec.js";

// Match workerd: close accepts protocol error codes and delivers its event
// only after the current error callback returns to the event loop.
const urls = [];
globalThis.WebSocket = class extends EventTarget {
  constructor(url) {
    super();
    urls.push(url);
    setTimeout(() => {
      const error = new Event("error");
      error.message = "simulated relay handshake failure";
      this.dispatchEvent(error);
    }, 0);
  }
  close(code, reason) {
    setTimeout(() => {
      const event = new Event("close");
      Object.assign(event, { code, reason, wasClean: false });
      this.dispatchEvent(event);
    }, 0);
  }
};
const go = new Go();
const { instance } = await WebAssembly.instantiate(fs.readFileSync(new URL("engine.wasm", import.meta.url)), go.importObject);
await go.run(instance);
for (const timer of go._scheduledTimeouts.values()) clearTimeout(timer);
go._scheduledTimeouts.clear();
assert.deepEqual(urls, ["wss://relay.invalid/relay", "wss://relay.example:443/relay"]);
