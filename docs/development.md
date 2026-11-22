# Development and current validation

Use Node.js 24 (the version is pinned in `.node-version` and `mise.toml`), pnpm 10.32.1, and Docker for local Postgres 18.

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

`test:conformance:hot` explicitly selects the implemented core groups from upstream `@durable-streams/server-conformance-tests@0.3.7`. Its source hash matches pinned upstream commit `461b40267aabd644558f9b19dbb9507dd5f691cf`. Other groups are filtered out because later features are pending. The fixture already sets `subscriptions: true`; `pnpm test:conformance` runs the entire suite and is expected to fail until live reads, forks, and subscriptions are implemented. A passing hot-log subset is not full protocol certification.

`test:package` builds/packs the library, installs the tarball in a clean temporary consumer, checks its public types, imports the runtime and migration entrypoints, and validates that SQL assets are included. It prefers the pnpm cache and can fetch missing package metadata, so it also works on a clean CI runner after a frozen-lockfile install. The package remains protected from publication by `private: true` while implementation and release qualification are incomplete.

GitHub Actions runs typechecking/build, property tests, Postgres integration/process-loss tests, the selected upstream groups, and the packed-consumer check. Real Neon deployment tests, S3-compatible development storage, chunk-format fixtures, Realtime, and the remaining M0 contracts are still pending.
