# netbird-do

A NetBird peer inside a Cloudflare Durable Object. Host private HTTP and TCP services, or use it as an exit node.

It runs NetBird's upstream Go client, built to WebAssembly with Workers patches. The peer keeps its identity across restarts and connects through NetBird's WebSocket relays.

Adapted from [littledivy/tailnet-do](https://github.com/littledivy/tailnet-do), including parts of its Go/JavaScript socket bridge.

## Use in a Worker

```js
import { NetBirdObject } from "netbird-do";

export class Peer extends NetBirdObject {
  async onNetBird(netbird) {
    await netbird.serve(80, (request, who) =>
      new Response(`Hello ${who?.fqdn ?? "peer"}`),
    );
  }
}

export default {
  async fetch(request, env) {
    const peer = env.NETBIRD.getByName("main");
    return peer.netbirdFetch("http://server.netbird.cloud:8080/");
  },
};
```

Add the binding, migration, and WASM rule to your Wrangler config:

```jsonc
{
  "durable_objects": {
    "bindings": [{ "name": "NETBIRD", "class_name": "Peer" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Peer"] }],
  "rules": [{ "type": "CompiledWasm", "globs": ["**/*.wasm"], "fallthrough": true }]
}
```

Set `NB_SETUP_KEY` with `npx wrangler secret put NB_SETUP_KEY` for the first enrollment. The peer starts on first use and keeps its identity. `NB_HOSTNAME` is optional; `NB_MANAGEMENT_URL` selects a self-hosted server instead of NetBird Cloud.

Inside `onNetBird`, use:

- `connect("host:port")` and `fetch(url, init)` to reach other peers.
- `serve(port, handler)` and `listen(port, handler)` to accept HTTP and TCP.
- `whois("ip:port")`, `status()`, and `stop()` for peer lookup, status, and shutdown.

Worker RPC methods are `netbirdFetch`, `netbirdConnect`, and `netbirdStatus`. Add access control before exposing private services publicly.

## Build and run

To build from source, you need Go 1.27.3, Node.js 22 or newer, and Git.

```sh
npm install
npm run build
cd examples/exit-node
npx wrangler dev
```

Copy [.dev.vars.example](examples/exit-node/.dev.vars.example) to `.dev.vars` in the example directory and set `NB_SETUP_KEY`.

To deploy the example, run `npx wrangler secret put NB_SETUP_KEY`, then `npx wrangler deploy` from its directory.

See [test/README.md](test/README.md) for tests.

## Exit nodes and location

Add the example peer as an exit node in the NetBird dashboard and choose its distribution groups. TCP exits through `cloudflare:sockets`. Forwarded UDP DNS uses Cloudflare DoH instead of the requested resolver; set `NB_DOH_URL` to change it.

Set `NB_LOCATION_HINT` in the example, or pass `locationHint` when creating the object:

```js
const id = env.NETBIRD.idFromName("main");
const peer = env.NETBIRD.get(id, { locationHint: "weur" });
```

`weur` requests Western Europe. [Location hints](https://developers.cloudflare.com/durable-objects/reference/data-location/#provide-a-location-hint) are best effort and do not move existing objects.

## Notes

- Exit forwarding supports TCP and DNS. Other UDP traffic and ICMP are not supported.
- Peers stay active by default and use [Durable Object quotas](https://developers.cloudflare.com/durable-objects/platform/pricing/). Set `keepAlive: false` to disable background restoration, or `NB_ENABLED="false"` to suspend the example.
- Enrollment uses setup keys. Interactive login is not implemented.

NetBird and its dependencies retain their respective licenses.
