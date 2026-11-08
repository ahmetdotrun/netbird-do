import fs from "node:fs";
import "../../src/wasm_exec.js";

const go = new Go();
go.argv = ["bridge.test", "-test.v", "-test.timeout=35s"];
go.exit = code => { process.exitCode = code; };
try {
  const { instance } = await WebAssembly.instantiate(fs.readFileSync(new URL("engine.wasm", import.meta.url)), go.importObject);
  await go.run(instance);
} finally {
  for (const timer of go._scheduledTimeouts.values()) clearTimeout(timer);
  go._scheduledTimeouts.clear();
}
