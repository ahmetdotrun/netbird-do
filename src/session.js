import { sessionDuration, KEEPALIVE_MS } from "./policy.js";

const LEASE_KEY = "netbird:lease";

// Save restart intent before network startup. A bounded session keeps its
// deadline when restored; normal first use creates a persistent peer.
export class SessionController {
  #storage;
  #create;
  #node = null;
  #queue = Promise.resolve();
  #phase = "stopped";
  #lastError;
  #keepAlive;

  constructor(storage, create, { keepAlive = () => true } = {}) {
    this.#storage = storage; this.#create = create; this.#keepAlive = keepAlive;
  }

  #serial(work) {
    const result = this.#queue.then(work);
    this.#queue = result.catch(() => {});
    return result;
  }

  #live(lease) { return lease?.mode === "persistent" || lease?.expiresAt > Date.now(); }

  async #schedule(lease, delay = KEEPALIVE_MS) {
    if (lease.mode === "persistent" && !this.#keepAlive()) { await this.#storage.deleteAlarm(); return; }
    await this.#storage.setAlarm(lease.mode === "persistent" ? Date.now() + delay : lease.expiresAt);
  }

  async #open(lease) {
    this.#phase = "starting";
    try {
      this.#node = await this.#create(lease.mode === "persistent" ? null : lease.expiresAt);
      if (!this.#live(lease)) throw new Error("session expired during startup");
      this.#phase = "running";
      this.#lastError = undefined;
      return this.#node;
    } catch (error) {
      if (this.#node) await this.#node.stop();
      this.#node = null;
      if (lease.mode === "persistent") {
        this.#phase = "failed";
        this.#lastError = error.message;
        await this.#schedule(lease, 60_000);
      } else await this.#close();
      throw error;
    }
  }

  async #close() {
    const node = this.#node;
    this.#node = null;
    this.#phase = "stopped";
    this.#lastError = undefined;
    try {
      await this.#storage.delete(LEASE_KEY);
      await this.#storage.deleteAlarm();
    } finally {
      if (node) await node.stop();
    }
  }

  async #current(lease) {
    if (this.#node && (await this.#node.status()).state !== "running") {
      await this.#node.stop();
      this.#node = null;
    }
    return this.#node ?? this.#open(lease);
  }

  start(durationMs) {
    const duration = sessionDuration(durationMs);
    return this.#serial(async () => {
      const lease = await this.#storage.get(LEASE_KEY);
      if (this.#live(lease)) {
        return this.#current(lease);
      }
      await this.#close();
      const expiresAt = Date.now() + duration;
      await this.#storage.put(LEASE_KEY, { expiresAt });
      try {
        await this.#schedule({ expiresAt });
        return await this.#open({ expiresAt });
      } catch (error) {
        await this.#close();
        throw error;
      }
    });
  }

  startPersistent() {
    return this.#serial(async () => {
      let lease = await this.#storage.get(LEASE_KEY);
      if (lease?.mode !== "persistent") {
        await this.#close();
        lease = { mode: "persistent", startedAt: Date.now() };
        await this.#storage.put(LEASE_KEY, lease);
        try { await this.#schedule(lease); }
        catch (error) { await this.#close(); throw error; }
      }
      return this.#current(lease);
    });
  }

  get(autoStart = false) {
    return this.#serial(async () => {
      let lease = await this.#storage.get(LEASE_KEY);
      if (!this.#live(lease)) {
        await this.#close();
        if (!autoStart) throw new Error("no active session; start one explicitly");
        lease = { mode: "persistent", startedAt: Date.now() };
        await this.#storage.put(LEASE_KEY, lease);
        try { await this.#schedule(lease); }
        catch (error) { await this.#close(); throw error; }
      }
      return this.#current(lease);
    });
  }

  async status() {
    const lease = await this.#storage.get(LEASE_KEY);
    if (!this.#live(lease)) return { state: "stopped", expiresAt: null };
    return {
      ...(this.#node ? await this.#node.status() : { state: ["starting", "failed"].includes(this.#phase) ? this.#phase : "inactive" }),
      expiresAt: lease.mode === "persistent" ? null : lease.expiresAt,
      ...(lease.mode === "persistent" ? { mode: "persistent", ...(this.#lastError ? { error: this.#lastError } : {}) } : {}),
    };
  }

  stop() { return this.#serial(() => this.#close()); }

  alarm() {
    return this.#serial(async () => {
      const lease = await this.#storage.get(LEASE_KEY);
      if (lease?.mode === "persistent") {
        if (!this.#keepAlive()) { await this.#storage.deleteAlarm(); return; }
        await this.#current(lease);
        await this.#schedule(lease);
        return;
      }
      if (this.#live(lease)) { await this.#schedule(lease); return; }
      await this.#close();
    });
  }
}
