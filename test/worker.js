import { DurableObject } from "cloudflare:workers";
import { NetBird, NetBirdObject } from "../src/index.js";
import { authorized, sessionDuration, readBody } from "../src/policy.js";

export class RawPeer extends DurableObject {
  async lifecycle() {
    const NativeGo = globalThis.Go;
    const engines = [];
    let active = 0, overlap = false, releaseStop;
    const stopped = new Promise(resolve => { releaseStop = resolve; });
    let current;
    // Exercise the real JS lifecycle without enrolling a cloud peer.
    globalThis.Go = class extends NativeGo {
      run() {
        const index = engines.length;
        engines.push(this);
        let terminated = false;
        globalThis.__netbirdDO = {
          generateKey: () => "local-test-identity",
          newNode: async () => ({
            start: async () => { active++; overlap ||= active > 1; },
            status: async () => ({ state: "running" }),
            stop: async () => { if (index === 0) await stopped; },
            dial: async () => {
              let end;
              const read = new Promise(resolve => { end = resolve; });
              return { read: () => read, write: async () => {}, closeWrite: async () => {}, close: () => end(null) };
            },
          }),
          terminate: () => {
            if (!terminated) { terminated = true; active--; }
            this.exited = true;
          },
        };
        return new Promise((_, reject) => { this.crash = () => reject(new Error("test crash")); });
      }
    };
    try {
      const options = { keepAlive: false };
      current = await NetBird.start(this.ctx, options);
      const identity = await this.ctx.storage.get("netbird:identity");
      const socket = await current.connect("local.test:80");
      const read = socket.readable.getReader().read().then(() => false, () => true);
      await socket.writable.abort(0);
      const abortClosesReader = await Promise.race([read, new Promise(resolve => setTimeout(() => resolve(false), 20))]);
      const other = await current.connect("local.test:80");
      const write = other.writable.getWriter().closed.then(() => false, () => true);
      await other.readable.cancel();
      const cancelClosesWriter = await Promise.race([write, new Promise(resolve => setTimeout(() => resolve(false), 20))]);
      const stopping = current.stop();
      const next = NetBird.start(this.ctx, options);
      const concurrent = NetBird.start(this.ctx, options);
      await new Promise(resolve => setTimeout(resolve, 20));
      const waitedForStop = engines.length === 1;
      releaseStop();
      await stopping;
      current = await next;
      engines[1].crash();
      await new Promise(resolve => setTimeout(resolve, 0));
      const crashed = current;
      current = await NetBird.start(this.ctx, options);
      return {
        waitedForStop, restartShared: next === concurrent, overlap, abortClosesReader, cancelClosesWriter,
        crashedPeerReplaced: current !== crashed && engines.length === 3,
        identityPreserved: (await this.ctx.storage.get("netbird:identity")).privateKey === identity.privateKey,
      };
    } finally {
      releaseStop();
      await current?.stop();
      globalThis.Go = NativeGo;
    }
  }

  async check() {
    const options = { managementURL: "http://invalid", keepAlive: false };
    const first = NetBird.start(this.ctx, options);
    const concurrent = NetBird.start(this.ctx, options);
    await Promise.allSettled([first, concurrent]);
    const saved = await this.ctx.storage.get("netbird:identity");
    const retry = NetBird.start(this.ctx, options);
    await retry.catch(() => {});
    const restored = await this.ctx.storage.get("netbird:identity");
    return { concurrentStartShared: first === concurrent, failedStartRetried: retry !== first,
      identityPreserved: !!saved?.privateKey && saved.privateKey === restored?.privateKey };
  }
}

export class Peer extends NetBirdObject {
  netbirdOptions() { return { ...super.netbirdOptions(), autoStart: false }; }
  async onNetBird(netbird) {
    await netbird.serve(8080, (request, who) => {
      const path = new URL(request.url).pathname;
      if (path === "/binary") return new Response(new Uint8Array([0, 1, 127, 128, 255]), { headers: { "content-type": "application/octet-stream" } });
      if (path !== "/health") return new Response("Not found", { status: 404 });
      return Response.json({ ok: true, service: "netbird-do", peer: who });
    });
    await netbird.listen(8081, async socket => {
      await socket.readable.pipeTo(socket.writable);
    });
  }
}

export default {
  async fetch(request, env) {
    if (!env.NB_TEST_TOKEN || env.NB_TEST_TOKEN.length < 32) return new Response("Configure NB_TEST_TOKEN (at least 32 characters)", { status: 503 });
    if (!await authorized(request, env.NB_TEST_TOKEN)) return new Response("Unauthorized", { status: 401 });
    const path = new URL(request.url).pathname;
    if (path === "/raw-check") return Response.json(await env.RAW.getByName("test").check());
    if (path === "/lifecycle-check") return Response.json(await env.RAW.getByName("lifecycle").lifecycle());
    // All requests use one identity. Neither a URL nor a caller chooses an object.
    const peer = env.NETBIRD.getByName("main");
    try {
      if (path === "/status" && request.method === "GET") return Response.json(await peer.netbirdStatus());
      if (path === "/start" && request.method === "POST") return Response.json(await peer.startPersistent());
      if (path === "/session" && request.method === "POST") {
        if (Number(request.headers.get("content-length") ?? 0) > 1024) return new Response("Request too large", { status: 413 });
        const reader = request.body?.getReader();
        const chunks = [];
        let size = 0;
        if (reader) for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > 1024) { await reader.cancel(); return new Response("Request too large", { status: 413 }); }
          chunks.push(value);
        }
        const bytes = new Uint8Array(size);
        let offset = 0;
        for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
        const body = size ? JSON.parse(new TextDecoder().decode(bytes)) : {};
        if (!body || typeof body !== "object" || Array.isArray(body) || Object.keys(body).some(key => key !== "durationMs")) {
          return new Response("Expected an object containing only durationMs", { status: 400 });
        }
        const duration = sessionDuration(body.durationMs);
        return Response.json(await peer.startSession(duration));
      }
      if (path === "/session" && request.method === "DELETE") return Response.json(await peer.stopSession());
      if (path === "/self-check" && request.method === "GET") {
        const status = await peer.netbirdStatus();
        if (status.state !== "running" || !status.address) return new Response("Start a session first", { status: 409 });
        const address = status.address.split("/")[0];
        return peer.netbirdFetch(`http://${address}:8080/binary`);
      }
      if (path === "/tcp-self-check" && request.method === "GET") {
        const status = await peer.netbirdStatus();
        if (status.state !== "running" || !status.address) return new Response("Start a session first", { status: 409 });
        const socket = await peer.netbirdConnect(`${status.address.split("/")[0]}:8081`);
        const writer = socket.writable.getWriter();
        await writer.write(new Uint8Array([0, 1, 127, 128, 255]));
        await writer.close();
        return new Response(await readBody(socket.readable), { headers: { "content-type": "application/octet-stream" } });
      }
      if (path === "/fetch" && request.method === "GET") {
        if (!env.NB_TEST_URL) return new Response("Configure NB_TEST_URL", { status: 503 });
        return peer.netbirdFetch(env.NB_TEST_URL);
      }
      return new Response("Not found", { status: 404 });
    } catch (error) {
      if (error instanceof SyntaxError || error instanceof RangeError) return new Response("Invalid session duration or JSON", { status: 400 });
      return Response.json({ error: error.message ?? "NetBird operation failed" }, { status: 502 });
    }
  },
};
