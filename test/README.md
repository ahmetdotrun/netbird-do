# Tests

Run `npm run build`, `npm test`, and `npm run test:runtime`. Ordinary tests use
local workerd and do not enroll peers or contact NetBird Cloud.

The library test links the built project into a separate Worker, bundles the
README example using `import { NetBirdObject } from "netbird-do"`, and runs it
in workerd. `npm test` builds the Go/WASM test fixtures too.

The Go/WASM bridge regression tests open eight connections, hold raw reads and
writes for 16 seconds, and round-trip HTTP bodies larger than 3 MiB. HTTP bodies
are buffered like tailnet-do, with no fixed library size cap.

`worker.js` is a test fixture with explicit start/stop controls protected by
`NB_TEST_TOKEN`. Those controls and the token are not part of the normal example.

The live runtime test is opt-in with `NB_SETUP_KEY`. Keep `NB_TEST_STATE_DIR`
between runs so it reuses its enrolled identity. Do not run it with the identity
of an online deployment. `NB_TEST_URL` can select a separate relay target.
