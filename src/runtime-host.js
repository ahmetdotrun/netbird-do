let owner;
let installed = false;

// workerd freezes its clock while JavaScript is executing. Go's stock timer
// shim retries an early timer synchronously, so that clock can never advance.
// Yield between retries and retain Go's timer IDs/cancellation semantics.
export function installGoTimeouts(go) {
  go.importObject.gojs["runtime.scheduleTimeoutEvent"] = sp => {
    sp >>>= 0;
    const id = go._nextCallbackTimeoutID++;
    const delay = Number(go.mem.getBigInt64(sp + 8, true));
    const fire = () => {
      if (go.exited || !go._scheduledTimeouts.has(id)) return;
      go._resume();
      if (!go.exited && go._scheduledTimeouts.has(id)) {
        go._scheduledTimeouts.set(id, setTimeout(fire, 1));
      }
    };
    go._scheduledTimeouts.set(id, setTimeout(fire, delay));
    go.mem.setInt32(sp + 16, id, true);
  };
}

function withOwner(runtime, work) {
  const previous = owner;
  owner = runtime;
  try { return work(); } finally { owner = previous; }
}

function installSocketTracking() {
  if (installed) return;
  const NativeWebSocket = globalThis.WebSocket;
  globalThis.WebSocket = new Proxy(NativeWebSocket, {
    construct(target, args, newTarget) {
      if (owner?.closing) throw new Error("NetBird runtime is stopping");
      const socket = Reflect.construct(target, args, newTarget);
      if (owner) {
        const runtime = owner;
        runtime.sockets.add(socket);
        socket.addEventListener("open", () => {
          if (runtime.closing) { try { socket.close(1000, "session ended"); } catch {} }
        }, { once: true });
        socket.addEventListener("close", () => runtime.sockets.delete(socket), { once: true });
      }
      return socket;
    },
  });
  installed = true;
}

// Each object owns its Go runtime and outbound transports. Closing transports
// and terminating Go also bounds failed or stalled upstream startup/teardown.
export class EngineRuntime {
  sockets = new Set();
  closing = false;
  #go;
  #api;

  static async create(module, GoClass, options = {}) {
    installSocketTracking();
    const runtime = new EngineRuntime();
    const go = runtime.#go = new GoClass();
    go.env = { GOMEMLIMIT: "72MiB", GOGC: "50", NB_WASM_HOST: options.host ?? "" };
    go.exit = () => {};
    installGoTimeouts(go);
    const resume = go._resume.bind(go);
    go._resume = () => {
      if (!go.exited) return withOwner(runtime, resume);
    };
    const instance = await WebAssembly.instantiate(module, go.importObject);
    const run = withOwner(runtime, () => go.run(instance));
    run.catch(() => { runtime.terminate(); });
    runtime.#api = globalThis.__netbirdDO;
    delete globalThis.__netbirdDO;
    if (!runtime.#api) { runtime.terminate(); throw new Error("NetBird engine failed to initialize"); }
    return runtime;
  }

  get api() { return this.#api; }
  get memoryBytes() { return this.#go?._inst?.exports.mem.buffer.byteLength ?? 0; }

  terminate() {
    this.closing = true;
    for (const socket of this.sockets) { try { socket.close(1000, "session ended"); } catch {} }
    this.sockets.clear();
    const go = this.#go;
    if (go && !go.exited && this.#api) this.#api.terminate();
    for (const timer of go?._scheduledTimeouts?.values() ?? []) clearTimeout(timer);
    go?._scheduledTimeouts?.clear();
    this.#api = null;
    this.#go = null;
  }
}
