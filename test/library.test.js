import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout: 60_000,
    env: { ...process.env, WRANGLER_SEND_METRICS: "false" } });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  return result.stdout;
}

test("the built library runs the README example in a separate Worker", async () => {
  const root = resolve(".");
  const dir = await mkdtemp(join(tmpdir(), "netbird-do-library-"));
  let mf;
  try {
    const consumer = join(dir, "consumer");
    await mkdir(consumer);
    await writeFile(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
    run("npm", ["install", "--no-audit", "--no-fund", "--package-lock=false", root], consumer);
    const notices = await readFile(join(consumer, "node_modules/netbird-do/src/THIRD_PARTY_NOTICES.txt"), "utf8");
    assert.match(notices, /github\.com\/netbirdio\/netbird/);
    const readme = await readFile(join(root, "README.md"), "utf8");
    const source = readme.match(/```js\n([\s\S]*?)\n```/)[1];
    assert.match(source, /import \{ NetBirdObject \} from "netbird-do"/);
    await writeFile(join(consumer, "index.js"), source);
    await writeFile(join(consumer, "consumer.js"), `
      import app, { Peer } from "./index.js";
      import { NetBird } from "netbird-do";
      export { Peer };
      export default {
        async fetch(request, env) {
          if (new URL(request.url).pathname === "/status") {
            return Response.json({ ...await env.NETBIRD.getByName("main").netbirdStatus(), rawClient: typeof NetBird.start === "function" });
          }
          return app.fetch(request, env);
        }
      };
    `);
    const config = { ...JSON.parse(readme.match(/```jsonc\n([\s\S]*?)\n```/)[1]),
      name: "netbird-library-consumer", main: "consumer.js", compatibility_date: "2026-10-01", workers_dev: false };
    await writeFile(join(consumer, "wrangler.json"), JSON.stringify(config));
    run(process.execPath, [join(root, "node_modules/wrangler/bin/wrangler.js"), "deploy", "--dry-run", "--outdir", "dist"], consumer);
    const bundleDir = join(consumer, "dist");
    const main = join(bundleDir, "consumer.js");
    const bundle = await readFile(main, "utf8");
    const wasm = (await readdir(bundleDir)).find(file => file.endsWith(".wasm") && bundle.includes(file));
    assert.ok(wasm, "Wrangler must bundle the installed library's WASM");
    const options = await convertV4MiniflareOptions({
      name: "netbird-library-consumer",
      modulesRoot: bundleDir,
      modules: [{ type: "ESModule", path: main }, { type: "CompiledWasm", path: join(bundleDir, wasm) }],
      compatibilityDate: "2026-10-01",
      durableObjects: { NETBIRD: { className: "Peer", useSQLite: true } },
      bindings: { NB_ENABLED: "false" },
    });
    mf = new Miniflare(options);
    const response = await mf.dispatchFetch("https://consumer.test/status");
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { state: "stopped", enabled: false, rawClient: true });
  } finally {
    await mf?.dispose();
    await rm(dir, { recursive: true, force: true });
  }
});
