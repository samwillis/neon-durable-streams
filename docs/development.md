# Development and testing

Use Node.js 24 (pinned in `.node-version` and `mise.toml`) and pnpm 10.32.1. Database-backed tests use actual Neon Postgres 18, Realtime, and Object Storage on disposable branches. Docker is not required.

## Fast checks

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
mise exec node@24 -- pnpm check
mise exec node@24 -- pnpm test:package
```

`pnpm test` runs the unit/property suite without a database or cloud credentials. `pnpm check` adds the build, typecheck, and consumer example typecheck. `pnpm test:package` installs a packed tarball in a temporary consumer and verifies public declarations, runtime exports, SQL assets, and licensing; it can fetch missing dependencies from the registry.

## Neon tests from a development machine

Copy [.env.example](../.env.example) to `.env.neon.local` and fill in a project-scoped `NEON_API_KEY`, `NEON_PROJECT_ID`, and non-expiring `NEON_PARENT_BRANCH_ID`. Provisioning creates a fresh branch and writes `.env.neon.test`; deployment also writes `.env.neon.function`. These files contain generated credentials and are ignored by Git.

```sh
mise exec node@24 -- pnpm neon:test:create
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run neon:test:deploy
mise exec node@24 -- node --env-file=.env.neon.test --env-file=.env.neon.function --run test:function
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run test:neon
mise exec node@24 -- node --env-file=.env.neon.test --run test:integration
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run test:recovery
mise exec node@24 -- node --env-file=.env.neon.local --env-file=.env.neon.test --run test:conformance:neon
mise exec node@24 -- pnpm neon:test:delete
```

Always run cleanup after a failure too. Provisioning records ownership before fetching credentials, refuses to overwrite an existing environment, and creates a branch with a 24-hour expiry. Cleanup deletes the recorded branch and its matching generated environment files. Do not use the parent branch as a test target.

Tests create random `ds_test_*` schemas and remove them during teardown. They use explicit test connection strings, never an application's `DATABASE_URL`. Pooled connections serve requests; direct connections run migrations and tests of persistent session settings. The complete [four-file baseline](migrations.md) is installed before serving; earlier development histories require a fresh schema.

| Command | Coverage |
| --- | --- |
| `test:integration` | Postgres transactions, concurrency, producer semantics, and child-process loss before/after commit |
| `test:neon` | Real Realtime and Object Storage, replay, forks, subscriptions, collection, migration safety, operations, and eight packed multi-instance checks |
| `test:function` | Deployed consumer ingress, authenticated streaming, sealed replay, public HTTPS webhooks, and cron recovery |
| `test:recovery` | Point-in-time child-branch adoption and stopped-Function in-place restore |
| `test:conformance:neon` | All 338 pinned upstream cases colocated on Neon, with subscriptions and collection enabled |

`pnpm test:conformance` runs the same full suite from the coordinator on your machine against Neon services. Cross-region SQL latency can exceed deadlines inside upstream cases; the colocated command is the CI qualification gate. Use Vitest's file or test-name filters for focused debugging, then run the full suite for qualification. See [colocated conformance](neon-conformance-v1.md) for packaging and provenance.

The loopback HTTP adapter and webhook receivers are active test fixtures. They run beside the test coordinator, including inside the conformance Neon Function, while Postgres, Realtime, and Object Storage remain actual Neon services. Separate deployed-ingress tests use a public HTTPS receiver. These fixtures are excluded from the library build.

## CI and qualification

[Required CI](../.github/workflows/ci.yml) has separate build/unit/package, Neon qualification, and append-hint jobs. Provisioning supplies generated credentials to later steps. Missing required credentials fail qualification. Fork PRs need review and a trusted maintainer run; the workflow does not execute fork code with secrets through `pull_request_target`.

The ordinary Function profile disables append hints and tests scheduled recovery. The separate hints job uses a fresh branch with hints enabled and no cron: `test:hints` verifies Realtime pull wakes, full-chunk sealing, and smaller tails remaining in Postgres. It requires that profile's Function environment.

The packed multi-instance fixture installs the current tarball into an independent consumer and deploys two Functions on a dedicated schema. It records four reserved Function slugs, including fresh deployments after recovery, before allocation. All eight checks and cleanup must pass. This tests coordination between instances; it does not establish cross-version compatibility. See the [compatibility contract](upgrade-compatibility-v1.md).

Recovery tests allocate additional recorded branches. The child-branch drill temporarily removes its disposable parent's expiry because Neon forbids children of expiring branches, then restores it after cleanup. The in-place drill stops its Function/cron, drains SSE, restores a recorded WAL point, reconciles HOT/SEALED data, and deploys a fresh instance before checking replay, Realtime payloads, and token fencing. CI retries interrupted cleanup, and the [scheduled janitor](../.github/workflows/test-janitor.yml) removes recognized stale test branches after runner loss. See the [in-place runbook](inplace-restore-v1.md) for diagnostic preservation and cleanup.

Remote coordinator tests allow explicit 120-second transaction/90-second lock budgets; deployed Function tests retain production defaults. Realtime delivery cases keep their shorter deadlines below the SQL fallback interval. The opt-in `test/fixtures/sql-latency.mjs` can delay SQL sends to reproduce distant-coordinator contention against real Neon; it is not shipped or enabled by default.

[Load qualification](../.github/workflows/load-qualification.yml) is a separate manual workflow. Run it for capacity and sustained delivery evidence. Current test counts, exact source revisions, historical measurements, and unresolved qualification limits belong in the [qualification index](qualification/README.md).
