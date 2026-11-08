import { NetBirdObject } from "netbird-do";

export class ExitNode extends NetBirdObject {
  netbirdOptions() {
    return { ...super.netbirdOptions(), hostname: this.env.NB_HOSTNAME ?? "cf-netbird", exitNode: true };
  }

  async onNetBird(netbird) {
    await netbird.serve(80, (request, who) => Response.json({
      service: "netbird-do", peer: who?.fqdn ?? null, path: new URL(request.url).pathname,
    }));
  }

  async fetch() {
    const status = await this.netbirdStatus();
    return Response.json({ state: status.state, address: status.address, enabled: status.enabled });
  }
}

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path !== "/" && path !== "/status") return new Response("Not found", { status: 404 });
    if (request.method !== "GET") return new Response("Method not allowed", { status: 405 });
    const id = env.NETBIRD.idFromName("main");
    const options = env.NB_LOCATION_HINT ? { locationHint: env.NB_LOCATION_HINT } : undefined;
    const peer = env.NETBIRD.get(id, options);
    try { return await peer.fetch(request); }
    catch { return Response.json({ error: "NetBird peer could not start" }, { status: 502 }); }
  },
};
