# Development and current validation

Use Node.js 24 (pinned in `.node-version` and `mise.toml`) and pnpm 10.32.1. Integration and conformance qualification runs on **real Neon Postgres 18, Realtime, and Object Storage**. Local Postgres is an optional development fixture.

GitHub Actions uses project `bitter-water-06585105` (`durable-streams-ci-us-east-1`) in the personal Sam organization, region `aws-us-east-1`. Frankfurt project `weathered-base-83674772` remains available for local development and regional Function qualification. CI uses a project-scoped `NEON_API_KEY` secret, plus `NEON_PROJECT_ID` and `NEON_PARENT_BRANCH_ID` repository variables. The US CI location reduces runner-to-database network delay for upstream assertions with short internal SSE/TTL/lease deadlines; no upstream assertions or samples are changed. Qualification of each region still needs its own evidence. Each run provisions an isolated branch with a 24-hour expiry, enables Realtime, creates a private Object Storage bucket and expiring branch-scoped storage credentials, uses its pooled application and direct migration connections, and deletes the branch in an `always()` cleanup step. The point-in-time recovery drill temporarily removes expiry from that disposable parent because Neon forbids children of expiring branches; it restores expiry after removing its one-hour child. A scheduled janitor removes recognized CI test branches older than 26 hours if the runner disappears during that window. Expiry provides a fallback after runner loss. The parent branch is never a test target.

For a local run, place the project-scoped management credential and project/parent IDs in the ignored `.env.neon.local`, then:

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
mise exec node@24 -- pnpm neon:test:create
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run neon:test:deploy
mise exec node@24 -- node --env-file=.env.neon.test --env-file=.env.neon.function --run test:function
mise exec node@24 -- node --env-file=.env.neon.test --run test:neon
mise exec node@24 -- node --env-file=.env.neon.test --run test:integration
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run test:recovery
mise exec node@24 -- node --env-file=.env.neon.test --run test:conformance
mise exec node@24 -- pnpm neon:test:delete
```

Always run cleanup even when tests fail. The script writes credentials to ignored, owner-readable files and refuses to overwrite an existing environment. The direct connection is required for migrations and tests of persistent session settings. Production requests use the pooled connection. Missing Realtime or Object Storage credentials fail the Neon tests; they never silently skip or switch to polling.

Fork PRs do not receive the management secret and cannot pass Neon qualification until a maintainer reviews the change and runs it on a trusted branch. The workflow does not use `pull_request_target` to execute fork code with secrets.

Optional local development:

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
docker compose up -d --wait
export TEST_DATABASE_URL=postgresql://streams@127.0.0.1:55432/streams
export PGPASSWORD=local-only
mise exec node@24 -- pnpm check
mise exec node@24 -- pnpm test:integration
mise exec node@24 -- pnpm test:package
```

If Node.js 24 is already on PATH, omit `mise exec node@24 --`. `TEST_DATABASE_URL` must identify a disposable database. Tests create random `ds_test_*` schemas and remove those schemas during teardown. They never fall back to an application's `DATABASE_URL`. `docker compose down` stops local services; its volume remains available for the next run.

The default `pnpm test` runs unit/property tests without a database. Integration tests exercise real PostgreSQL transactions and start test-only child processes that are killed before/after commit. The conformance fixture starts a local HTTP adapter around the actual Fetch handler and uses explicitly anonymous authentication on an isolated test schema. Neither that adapter nor its process barriers are part of the shipped library. The full upstream suite runs from the test runner because its webhook fixtures listen on runner-local loopback ports. Separate required ingress tests exercise the deployed Function; both paths use real Neon data services.

`test:conformance:hot` explicitly selects the implemented core groups from upstream `@durable-streams/server-conformance-tests@0.3.7`. Its source hash matches pinned upstream commit `461b40267aabd644558f9b19dbb9507dd5f691cf`. This remains an optional focused development command. The fixture already sets `subscriptions: true`; `pnpm test:conformance` runs the entire suite and is the full required gate. A passing hot-log subset is not full protocol certification. The original local Postgres baseline was **338 tests: 198 passed, 140 failed, none skipped**. The first remote-Neon baseline was **196 passed, 142 failed, none skipped**, including two 30-second property-test timeouts; the Neon harness now allows 600 seconds per test across runner regions without changing samples or assertions. Those baseline counts predate live modes and forks; they are not the current combined result. The first combined full run is now **338 passed, 0 failed, 0 skipped**, with subscriptions enabled; the original baselines are retained here as history.

`test:package` builds/packs the library, installs the tarball in a clean temporary consumer, checks its public types, imports the runtime and migration entrypoints, and validates that SQL assets are included. It prefers the pnpm cache and can fetch missing package metadata, so it also works on a clean CI runner after a frozen-lockfile install. The package remains protected from publication by `private: true` while implementation and release qualification are incomplete.

GitHub Actions has separate build/unit/package and required Neon qualification jobs. The latter deploys a consumer Neon Function and its scheduled recovery trigger, checks actual ingress, and runs real Realtime delivery/isolation tests, transaction/process-loss tests on Neon, and the **entire unfiltered upstream conformance suite with subscriptions enabled**. The 127-case live/lifecycle/offset/browser/TTL group passes against Neon; the other 211 cases are excluded only in that focused development run. Full-suite failure makes CI red; the complete JSON report is retained as an artifact. Selected-group success does not qualify a release.

Verified against an actual isolated Neon branch: **75 integration/Neon tests passed**. They cover transaction/process loss, real Realtime append/close delivery, cancellation and tenant filtering, seven Object Storage fault/replay cases, six fork race/security cases, thirteen subscription cases, six operations cases, two maintenance/paging cases, a stopped-server schema upgrade, and thirteen committed-payload delivery/fencing cases. All 82 upstream fork cases also pass in a focused real-Neon run. Separate qualification adds real point-in-time child-branch adoption and stopped-Function in-place restore drills, plus **nine tests through deployed Neon Functions**, including public HTTPS webhooks, streaming, sealed replay, tenant isolation, forged-trigger rejection, and scheduled recovery. See [operations](operations-v1.md) for recovery scope. Restore scale/fault profiles, reclamation, mixed-version upgrades, and the remaining production qualification gates are still pending.

Thirteen subscription tests cover real SQL concurrency barriers, commit reordering, persisted retries, lease/token fencing, atomic ack validation, quota recovery, Realtime wakes, signing-key rotation, inherited sealed offsets, independent membership modes, destination authorization, bounded prefix coalescing, and resumed fan-out with new registrations. Unit/property coverage includes bounded glob matching, encrypted envelopes, and webhook address policy. Passing conformance does not close restore, reclamation, capacity, or remaining operational gates.

The required Function deployment also provisions a separate test-only HTTPS receiver on the same isolated branch. Three public-network cases verify Ed25519 over the exact body bytes, synchronous completion, scoped asynchronous callbacks, duplicate fencing, and a persisted retry after HTTP 500. The receiver is warmed and authenticated before qualification and is removed with the branch. It is never bundled into the consumer example.

Qualification runs are serialized per ref without canceling an in-flight run when a newer commit arrives. Full remote property tests retain every sample/assertion and have a 600-second case budget; the Neon job has a 90-minute ceiling to account for CI runner regions. Load/soak runs use the separate manual qualification workflow and always delete their own branch.

The test coordinator uses explicit 120-second transaction/90-second lock budgets and a 120-second pool wait for concurrency bursts over long-distance SQL connections. Actual deployed Function tests retain the default 10-second transaction/five-second lock limits. The pull-wake Realtime case allows 45 seconds for its entire setup/dispatch/assertion sequence, still below its 60-second SQL fallback. `test/fixtures/sql-latency.mjs` can inject 150 ms before every SQL send to reproduce the remote coordinator's contention while using real Neon. No protocol sample or assertion is removed.

A separate required `Neon append-hint delivery` job provisions its own labeled branch with hints enabled and **no cron trigger**. Two tests verify hinted pull wakes through real Realtime and full 4 MiB chunk publication to Object Storage; they also verify that smaller tails stay in Postgres. The ordinary Function qualification uses hints disabled and proves actual scheduled recovery. Both profiles use the same consumer code.

`test:recovery` also allocates a separate `ds-test-inplace-<UUID>` branch directly from the non-expiring test parent. It stops the deployed Function/cron, drains an actual SSE reader, restores at a flushed WAL LSN, reconciles HOT/SEALED state, and redeploys with a fresh deployment ID before validating public replay and Realtime. It records `.neon-test-state-inplace.json` before allocation/restore so cleanup can resolve both exact names even after an ambiguous API response. `scripts/neon-test-inplace-cleanup.mjs` deletes leaves before the preserved parent; CI always invokes it when a ledger remains. The scheduled janitor recognizes both reserved names. See the [in-place runbook](inplace-restore-v1.md). A local diagnostic run can set `NEON_TEST_KEEP_INPLACE=1`; GitHub Actions ignores that flag. Run the cleanup script with the same project-scoped management environment afterward.
