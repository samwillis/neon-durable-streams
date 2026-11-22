# Development and current validation

Use Node.js 24 (pinned in `.node-version` and `mise.toml`) and pnpm 10.32.1. Integration and conformance qualification runs on **real Neon Postgres 18 and Realtime**. Local Postgres is an optional development fixture.

The dedicated test project is `weathered-base-83674772` in the personal Sam organization, region `aws-eu-central-1`. GitHub Actions uses a project-scoped `NEON_API_KEY` secret, plus `NEON_PROJECT_ID` and `NEON_PARENT_BRANCH_ID` repository variables. Each run provisions an isolated branch with a 24-hour expiry, enables Realtime, uses its pooled application and direct migration connections, and deletes the branch in an `always()` cleanup step. Expiry provides a fallback after runner loss. The parent branch is never a test target.

For a local run, place the project-scoped management credential and project/parent IDs in the ignored `.env.neon.local`, then:

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
mise exec node@24 -- pnpm neon:test:create
mise exec node@24 -- node --env-file=.env.neon.test --run test:neon
mise exec node@24 -- node --env-file=.env.neon.test --run test:integration
mise exec node@24 -- node --env-file=.env.neon.test --run test:conformance
mise exec node@24 -- pnpm neon:test:delete
```

Always run cleanup even when tests fail. The script writes credentials to ignored, owner-readable files and refuses to overwrite an existing environment. The direct connection is required for migrations and tests of persistent session settings. Production requests use the pooled connection. Missing Realtime credentials fail the Neon tests; they never silently skip or switch to polling.

Fork PRs do not receive the management secret and cannot pass Neon qualification until a maintainer reviews the change and runs it on a trusted branch. The workflow does not use `pull_request_target` to execute fork code with secrets.

Optional local development:

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
docker compose up -d --wait
export TEST_DATABASE_URL=postgresql://streams@127.0.0.1:55432/streams
export PGPASSWORD=local-only
mise exec node@24 -- pnpm check
mise exec node@24 -- pnpm test:integration
mise exec node@24 -- pnpm test:conformance:hot
mise exec node@24 -- pnpm test:package
```

If Node.js 24 is already on PATH, omit `mise exec node@24 --`. `TEST_DATABASE_URL` must identify a disposable database. Tests create random `ds_test_*` schemas and remove those schemas during teardown. They never fall back to an application's `DATABASE_URL`. `docker compose down` stops local services; its volume remains available for the next run.

The default `pnpm test` runs unit/property tests without a database. Integration tests exercise real PostgreSQL transactions and start test-only child processes that are killed before/after commit. The conformance fixture starts a local HTTP adapter around the actual Fetch handler and uses explicitly anonymous authentication on an isolated test schema. Neither that adapter nor its process barriers are part of the shipped library.

`test:conformance:hot` explicitly selects the implemented core groups from upstream `@durable-streams/server-conformance-tests@0.3.7`. Its source hash matches pinned upstream commit `461b40267aabd644558f9b19dbb9507dd5f691cf`. Other groups are filtered out because later features are pending. The fixture already sets `subscriptions: true`; `pnpm test:conformance` runs the entire suite and is expected to fail until live reads, forks, and subscriptions are implemented. A passing hot-log subset is not full protocol certification. The complete unfiltered baseline is **338 tests: 198 passed, 140 failed, none skipped**. Live reads, forks, and subscriptions account for most failures; they remain release blockers.

`test:package` builds/packs the library, installs the tarball in a clean temporary consumer, checks its public types, imports the runtime and migration entrypoints, and validates that SQL assets are included. It prefers the pnpm cache and can fetch missing package metadata, so it also works on a clean CI runner after a frozen-lockfile install. The package remains protected from publication by `private: true` while implementation and release qualification are incomplete.

GitHub Actions has separate build/unit/package and required Neon qualification jobs. The latter runs real Realtime delivery/isolation tests, transaction/process-loss tests on Neon, and the **entire unfiltered upstream conformance suite with subscriptions enabled**. The 127-case live/lifecycle/offset/browser/TTL group passes against Neon; the other 211 cases are excluded only in that focused development run. Full-suite failure makes CI red; the complete JSON report is retained as an artifact. Selected-group success does not qualify a release.

Verified against an actual isolated Neon branch: **27 integration tests passed**, including Realtime append/close delivery, tenant filtering, pooled writes, direct-session setting restoration, concurrent producer retries, and writer SIGKILL before/after commit. This exercises a local HTTP adapter with real hosted data services. Deployed Neon Function ingress, Object Storage, chunk-format fixtures, and the remaining production qualification gates are still pending.
