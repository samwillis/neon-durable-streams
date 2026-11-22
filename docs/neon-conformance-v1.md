# Full conformance on Neon

`pnpm test:conformance:neon` deploys the actual Vitest coordinator beside the test branch's Postgres, Realtime, and Object Storage. It runs the existing `test/conformance/server.test.ts`, including its loopback HTTP adapter and upstream loopback webhook fixtures. Required consumer ingress and public HTTPS webhook tests remain separate: the adapter is a fixture inside the coordinator, not the production Function ingress.

The upstream package remains pinned to `@durable-streams/server-conformance-tests@0.3.7`. This command applies no name filters, exclusions, retries, clock substitutions, sample reductions, or upstream edits. Success requires a zero exit code, a successful report, exactly 338 total/passed cases, zero failures, zero pending/todo cases, and completed resource cleanup. Subscriptions stay enabled.

## Packaging and execution

The builder snapshots an explicit list of source, fixture, migration, configuration, and lock files into a temporary directory. Its frozen dependency installation cannot see cloud/database credentials and does not run install scripts. It packages the required dependency graph, including the existing locked WASI variant of the test compiler. Library runtime dependencies and upstream test code are unchanged.

Observed Function constraints require two parts:

- A private, hash-pinned suite archive on the disposable branch's Object Storage. The launcher verifies the exact size and SHA-256 before unpacking it.
- The small authenticated launcher and executable/native dependencies in the deployed ZIP. Functions rejected ZIP uploads over 32 MiB and rejected the large native Rolldown addon during source-image creation. The locked WASI compiler avoids that addon. The writable `/tmp` mount rejects executable mappings, so ELF binaries/addons are deployed on the code mount and linked into the extracted graph instead.

The qualified launcher targets Linux ARM64 with glibc and rejects another architecture. Each report records the actual Function Node version, architecture/libc, compiler variant, source commit/dirty state, source snapshot, fixture, upstream entrypoint, dependency lock, suite archive, native file hashes, launcher, and deployment ZIP. These identify the tested inputs; a matching package version alone does not.

The runtime receives only the isolated data-service credentials and a random per-deployment caller key. It receives no Neon management credential and accepts no source code, commands, test patterns, or environment overrides from requests. Authenticated SSE carries bounded, redacted log lines, heartbeats, and the full sanitized JSON result. Cancellation/shutdown stops the child process group; the coordinator has a finite execution deadline. Temporary files, its exact Function slug, and the exact suite object are removed afterward. Whole-branch cleanup and the scheduled janitor cover runner loss.

## Running locally

Use an already provisioned, recorded disposable environment:

```sh
NEON_TEST_STATE_FILE=.neon-test-state-smoke.json \
  mise exec node@24 -- node --env-file=.env.neon.local \
  --env-file=.env.neon.smoke scripts/neon-test-conformance.mjs
```

For the ordinary `.neon-test-state.json` / `.env.neon.test` environment, omit the state override and use that env file instead. The command refuses default/protected branches and mismatched ledgers. CI supplies these variables from provisioning rather than copying a developer's credentials.

`conformance-results.json` contains every upstream result. `qualification-results/colocated-conformance.json` contains sanitized provenance and the overall gate. CI retains both even on failure when available. The original `pnpm test:conformance` remains available for running the same suite from a local coordinator; cross-region SQL latency can exceed deadlines inside upstream cases, independently of Vitest's outer timeout.
