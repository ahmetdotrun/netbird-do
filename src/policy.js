export const MAX_SESSION_MS = 300_000;
export const DEFAULT_SESSION_MS = 60_000;
export const KEEPALIVE_MS = 20_000;

export function sessionDuration(value = DEFAULT_SESSION_MS) {
  if (!Number.isInteger(value) || value < 1_000 || value > MAX_SESSION_MS) {
    throw new RangeError(`durationMs must be an integer between 1000 and ${MAX_SESSION_MS}`);
  }
  return value;
}

export async function readBody(body) {
  if (!body) return new Uint8Array();
  return new Uint8Array(await new Response(body).arrayBuffer());
}

export function withTimeout(promise, durationMs, message) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), Math.max(1, durationMs)); }),
  ]).finally(() => clearTimeout(timer));
}

export async function authorized(request, secret) {
  if (typeof secret !== "string" || secret.length < 32) return false;
  const supplied = request.headers.get("authorization") ?? "";
  const encoder = new TextEncoder();
  const [expected, actual] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(`Bearer ${secret}`)),
    crypto.subtle.digest("SHA-256", encoder.encode(supplied)),
  ]);
  const a = new Uint8Array(expected), b = new Uint8Array(actual);
  let different = 0;
  for (let i = 0; i < a.length; i++) different |= a[i] ^ b[i];
  return different === 0;
}
