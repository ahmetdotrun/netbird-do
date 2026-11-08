import test from "node:test";
import assert from "node:assert/strict";
import { authorized, readBody, sessionDuration } from "../src/policy.js";

test("session durations are bounded and cannot be disabled", () => {
  assert.equal(sessionDuration(), 60_000);
  assert.equal(sessionDuration(300_000), 300_000);
  for (const invalid of [0, -1, 999, 300_001, Infinity, NaN, "60000", null, 1000.1]) {
    assert.throws(() => sessionDuration(invalid), RangeError);
  }
});

test("authentication rejects unset, short and incorrect tokens", async () => {
  const token = "a".repeat(40);
  const request = value => new Request("https://example.test", { headers: { authorization: value } });
  assert.equal(await authorized(request(`Bearer ${token}`), token), true);
  assert.equal(await authorized(request(`Bearer ${token}x`), token), false);
  assert.equal(await authorized(request(token), token), false);
  assert.equal(await authorized(request("Bearer short"), "short"), false);
  assert.equal(await authorized(request(""), undefined), false);
});

test("HTTP buffering preserves binary bodies larger than 1 MiB", async () => {
  const bytes = Uint8Array.from({ length: 3 * 1024 * 1024 + 5 }, (_, index) => index % 256);
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(bytes.subarray(0, 1024 * 1024));
      controller.enqueue(bytes.subarray(1024 * 1024));
      controller.close();
    },
  });
  assert.deepEqual(await readBody(stream), bytes);
  assert.deepEqual(await readBody(null), new Uint8Array());
});

test("HTTP buffering propagates stream failures", async () => {
  const failure = new Error("source disconnected");
  const stream = new ReadableStream({ start(controller) { controller.error(failure); } });
  await assert.rejects(readBody(stream), error => error === failure);
});
