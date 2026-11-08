import { DurableObject } from "cloudflare:workers";
import { connect as connectTCP } from "cloudflare:sockets";
import "./wasm_exec.js";
import engineWasm from "./netbird.wasm";
import { EngineRuntime } from "./runtime-host.js";
import { KEEPALIVE_MS, MAX_SESSION_MS, readBody, withTimeout } from "./policy.js";
import { SessionController } from "./session.js";

const nodes = new WeakMap();

export class NetBird {
  #runtime;
  #node;
  #expiresAt;
  #timer;
  #stopping;
  #sockets = new Set();
  #hostSockets = new Set();
  #dnsRequests = new Set();
  #egress = { tcp4Attempts: 0, tcp6Attempts: 0, tcpFailures: 0 };
  #ctx;
  #keepAlive;
  #dohURL;

  static start(ctx, options = {}) {
    let record = nodes.get(ctx);
    const previous = record?.peer;
    if (!record || previous?.#stopping || previous?.#runtime.closing) {
      record = {};
      nodes.set(ctx, record);
      record.promise = (async () => {
        if (previous) await previous.stop();
        return NetBird.#create(ctx, options);
      })().then(peer => { record.peer = peer; return peer; });
      record.promise.catch(() => { if (nodes.get(ctx) === record) nodes.delete(ctx); });
    }
    return record.promise;
  }

  static async #create(ctx, options) {
    const expiresAt = options.persistent ? null : options.expiresAt ?? null;
    if ((options.persistent === false && expiresAt === null) || (expiresAt !== null && (!Number.isFinite(expiresAt) || expiresAt <= Date.now() || expiresAt > Date.now() + MAX_SESSION_MS))) {
      throw new RangeError("a session deadline within five minutes is required");
    }
    const runtime = await EngineRuntime.create(engineWasm, globalThis.Go, { host: "cloudflare" });
    const peer = new NetBird(runtime, expiresAt, ctx, options);
    try {
      const saved = await ctx.storage.get("netbird:identity");
      const privateKey = saved?.privateKey ?? options.privateKey ?? runtime.api.generateKey();
      if (!privateKey) throw new Error("device identity generation failed");
      if (!saved) await ctx.storage.put("netbird:identity", { privateKey });
      peer.#node = await runtime.api.newNode({
        privateKey, setupKey: options.setupKey ?? "", hostname: options.hostname ?? "netbird-do",
        managementURL: options.managementURL ?? "https://api.netbird.io:443",
        exitNode: !!options.exitNode,
        host: { dialTCP: (host, port) => peer.#dialHost(host, port), dnsQuery: query => peer.#dnsQuery(query) },
      });
      await withTimeout(peer.#node.start(), expiresAt === null ? 28_000 : Math.min(28_000, expiresAt - Date.now()), "NetBird startup timed out");
      await peer.alarm();
      return peer;
    } catch (error) {
      await peer.stop();
      throw error;
    }
  }

  constructor(runtime, expiresAt, ctx, options) {
    this.#runtime = runtime;
    this.#expiresAt = expiresAt;
    this.#ctx = ctx;
    this.#keepAlive = options.keepAlive !== false && expiresAt === null;
    this.#dohURL = options.dohURL ?? "https://cloudflare-dns.com/dns-query";
    if (expiresAt !== null) this.#timer = setTimeout(() => { void this.stop(); }, Math.max(1, expiresAt - Date.now()));
  }

  #active() {
    if (this.#stopping || this.#runtime.closing || (this.#expiresAt !== null && Date.now() >= this.#expiresAt)) throw new Error("NetBird session ended");
  }

  async #dialHost(hostname, port) {
    this.#active();
    // SocketAddress concatenates hostname and port verbatim in workerd.
    // Use an explicit authority so IPv6 literals cannot absorb the port.
    const authority = `${hostname.includes(":") ? `[${hostname}]` : hostname}:${port}`;
    this.#egress[hostname.includes(":") ? "tcp6Attempts" : "tcp4Attempts"]++;
    let socket;
    try {
      socket = connectTCP(authority, { allowHalfOpen: true });
      this.#hostSockets.add(socket);
      socket.closed.then(() => this.#hostSockets.delete(socket), () => this.#hostSockets.delete(socket));
      await socket.opened;
    } catch (error) {
      this.#egress.tcpFailures++;
      this.#egress.lastError = String(error.message ?? error).slice(0, 256);
      this.#hostSockets.delete(socket);
      socket?.close().catch(() => {});
      throw error;
    }
    const reader = socket.readable.getReader(), writer = socket.writable.getWriter();
    return {
      async read() { const {done,value} = await reader.read(); return done ? null : value; },
      write: bytes => writer.write(bytes),
      closeWrite: () => writer.close(),
      close: () => { void socket.close().catch(() => {}); },
    };
  }

  async #dnsQuery(query) {
    this.#active();
    const controller = new AbortController();
    this.#dnsRequests.add(controller);
    const timer = setTimeout(() => controller.abort(), 5_000);
    try {
      const response = await fetch(this.#dohURL, {
        method: "POST", body: query, signal: controller.signal,
        headers: { "content-type": "application/dns-message", accept: "application/dns-message" },
      });
      if (!response.ok) throw new Error("exit DNS resolver failed");
      return await readBody(response.body);
    } finally { clearTimeout(timer); this.#dnsRequests.delete(controller); }
  }

  async status() {
    if (this.#stopping || this.#runtime.closing) return { state: "stopped", expiresAt: this.#expiresAt };
    return { ...await this.#node.status(), expiresAt: this.#expiresAt, wasmMemoryBytes: this.#runtime.memoryBytes,
      egress: { ...this.#egress, activeTCP: this.#hostSockets.size, activeDNSRequests: this.#dnsRequests.size } };
  }

  #socket(sock) {
    let finished = false, readEnded = false, writeEnded = false;
    let readController, writeController, resolveClosed;
    const closed = new Promise(resolve => { resolveClosed = resolve; });
    const finish = (error) => {
      if (finished) return;
      finished = true;
      if (error !== undefined) { try { readController?.error(error); } catch {} try { writeController?.error(error); } catch {} }
      try { sock.close(); } catch {}
      this.#sockets.delete(finish);
      resolveClosed();
    };
    this.#sockets.add(finish);
    return {
      readable: new ReadableStream({
        start(controller) { readController = controller; },
        async pull(controller) {
          if (finished) return;
          try {
            const value = await sock.read();
            if (finished) return;
            if (value === null) { readEnded = true; controller.close(); if (writeEnded) finish(); }
            else if (value.byteLength) controller.enqueue(value);
          } catch (error) { finish(error); }
        },
        cancel: reason => finish(reason ?? new Error("socket read canceled")),
      }),
      writable: new WritableStream({
        start(controller) { writeController = controller; },
        async write(value) {
          if (finished) throw new Error("socket closed");
          const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
          for (let offset = 0; offset < bytes.byteLength; offset += 32_768) {
            await sock.write(bytes.subarray(offset, offset + 32_768));
          }
        },
        async close() { if (!finished) { await sock.closeWrite(); writeEnded = true; if (readEnded) finish(); } },
        abort: reason => finish(reason ?? new Error("socket write aborted")),
      }),
      opened: Promise.resolve({ localAddress: sock.localAddress, remoteAddress: sock.remoteAddress }),
      closed,
      close: async () => finish(new Error("socket closed")),
    };
  }

  async connect(address) {
    this.#active();
    return this.#socket(await this.#node.dial(address));
  }

  async fetch(input, init) {
    this.#active();
    const request = new Request(input, init);
    const result = await this.#node.fetch({
      url: request.url, method: request.method, headers: [...request.headers], body: await readBody(request.body),
    });
    return new Response([101, 204, 205, 304].includes(result.status) || request.method === "HEAD" ? null : result.body,
      { status: result.status, headers: result.headers });
  }

  async listen(port, onSocket) {
    this.#active();
    await this.#node.listen(port, (sock, who) => {
      const socket = this.#socket(sock);
      Promise.resolve().then(() => onSocket(socket, who)).catch(() => socket.close());
    });
    return { close: () => this.#node.unlisten(port) };
  }

  async serve(port, handler) {
    this.#active();
    await this.#node.serveHTTP(port, async (input, who) => {
      const request = new Request(input.url, {
        method: input.method, headers: input.headers,
        body: ["GET", "HEAD"].includes(input.method) ? null : input.body,
      });
      const response = await handler(request, who);
      return { status: response.status, headers: [...response.headers], body: await readBody(response.body) };
    });
    return { close: () => this.#node.unlisten(port) };
  }

  whois(address) { this.#active(); return this.#node.whois(address); }

  async alarm() {
    if (this.#keepAlive && !this.#stopping) await this.#ctx.storage.setAlarm(Date.now() + KEEPALIVE_MS);
  }

  stop() {
    if (!this.#stopping) this.#stopping = (async () => {
      clearTimeout(this.#timer);
      for (const socket of this.#hostSockets) { void socket.close().catch(() => {}); }
      this.#hostSockets.clear();
      for (const request of this.#dnsRequests) request.abort();
      this.#dnsRequests.clear();
      for (const close of this.#sockets) close(new Error("NetBird session ended"));
      try {
        if (this.#keepAlive) await this.#ctx.storage.deleteAlarm();
        if (this.#node) await withTimeout(this.#node.stop(), 1_000, "NetBird shutdown timed out");
      } catch {} finally {
        this.#runtime.terminate();
        this.#node = null;
        if (nodes.get(this.#ctx)?.peer === this) nodes.delete(this.#ctx);
      }
    })();
    return this.#stopping;
  }
}

export class NetBirdObject extends DurableObject {
  #sessions;

  constructor(ctx, env) {
    super(ctx, env);
    this.#sessions = new SessionController(ctx.storage, async (expiresAt) => {
      const options = this.netbirdOptions();
      const node = await NetBird.start(ctx, { ...options, setupKey: options.setupKey ?? env.NB_SETUP_KEY, privateKey: options.privateKey ?? env.NB_PRIVATE_KEY, keepAlive: false, persistent: expiresAt === null, expiresAt });
      try { await this.onNetBird(node); return node; }
      catch (error) { await node.stop(); throw error; }
    }, { keepAlive: () => this.netbirdOptions().keepAlive !== false });
  }

  netbirdOptions() {
    return { managementURL: this.env.NB_MANAGEMENT_URL, hostname: this.env.NB_HOSTNAME, exitNode: this.env.NB_EXIT_NODE === "true", dohURL: this.env.NB_DOH_URL };
  }

  async onNetBird(netbird) {}
  async netbird() {
    if (this.env.NB_ENABLED === "false") { await this.#sessions.stop(); throw new Error("NetBird is disabled"); }
    return this.#sessions.get(this.netbirdOptions().autoStart !== false);
  }
  async startSession(durationMs) {
    if (this.env.NB_ENABLED === "false") throw new Error("NetBird is disabled");
    return (await this.#sessions.start(durationMs)).status();
  }
  async startPersistent() {
    if (this.env.NB_ENABLED === "false") throw new Error("NetBird is disabled");
    return (await this.#sessions.startPersistent()).status();
  }
  async stopSession() { await this.#sessions.stop(); return { state: "stopped", expiresAt: null }; }
  async netbirdStatus() {
    if (this.env.NB_ENABLED === "false") { await this.#sessions.stop(); return { state: "stopped", enabled: false }; }
    if (this.netbirdOptions().autoStart !== false) await this.netbird();
    return this.#sessions.status();
  }
  async netbirdFetch(input, init) { return (await this.netbird()).fetch(input, init); }
  async netbirdConnect(address) {
    const socket = await (await this.netbird()).connect(address);
    return { readable: socket.readable, writable: socket.writable };
  }
  async alarm() {
    if (this.env.NB_ENABLED === "false") { await this.#sessions.stop(); return; }
    await this.#sessions.alarm();
  }
}
