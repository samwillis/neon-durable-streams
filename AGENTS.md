# Development guide

## Project and scope

This repository builds `@samwillis/durable-streams-neon`, an embeddable TypeScript Durable Streams server for applications running on Neon. It is a library, not a managed Neon product. Consumers own infrastructure, credentials, authentication/authorization, scheduling, monitoring, and recovery.

The package is ESM, uses Node.js 24, and remains unpublished with `private: true`. Keep Apache-2.0 licensing and Databricks, Inc. copyright. Keep the README focused on library users; development instructions belong here and in [docs/development.md](docs/development.md).

The project is still in development. Backward compatibility with earlier development schemas, offsets, or object formats is not required. Use fresh disposable state rather than adding compatibility codecs, backfills, or migration history for superseded experiments. This does not relax checkpoint continuity within the current design or permit resetting an unrelated installation.

## Repository map

| Location | Responsibility |
| --- | --- |
| `src/server.ts`, `src/index.ts` | Fetch protocol handler, public options, authentication and exports |
| `src/store.ts`, `src/transaction.ts` | Transactional stream operations and replay planning |
| `src/storage.ts`, `src/block-codec.ts`, `src/s3.ts` | Chunk publication, verification, and object I/O |
| `src/replay*.ts`, `src/chunk-cache.ts`, `src/response-body.ts`, `src/sse.ts` | Pins, admission, buffering, cancellation, and response delivery |
| `src/neon.ts`, `src/live*.ts` | Realtime subscriptions, durable reconciliation, and live-reader lifecycle |
| `src/subscription*.ts`, `src/wake-token.ts`, `src/webhook.ts` | Durable subscriptions, leases, acknowledgements, and delivery |
| `src/installation.ts`, `src/operations.ts`, `src/reclamation.ts` | Installation identity, recovery, and collection |
| `migrations/` | Four logical baseline SQL files and their schema constraints |
| `examples/neon-function/` | Consumer deployment wiring; not part of the library build |
| `test/`, `scripts/` | Vitest projects, process-loss fixtures, provisioning, and qualification tools |
| `docs/qualification/` | Recorded measurements and source-scoped qualification evidence |

Start from the current [storage](docs/compact-storage-v2.md), [offset](docs/offset-design.md), [lifecycle](docs/lifecycle-design.md), [subscription](docs/subscriptions-v1.md), and [operations](docs/operations-v1.md) contracts. Early investigation/design documents and historical reports describe their recorded revisions; they can differ from the shipping implementation. The [qualification index](docs/qualification/README.md) identifies the latest completed checks.

## Local setup and fast checks

Use the versions pinned in `mise.toml`, `.node-version`, and `package.json`: Node.js 24 and pnpm 10.32.1.

```sh
mise exec node@24 -- pnpm install --frozen-lockfile
mise exec node@24 -- pnpm check
mise exec node@24 -- pnpm test:package
```

- `pnpm test` runs unit/property tests without Neon credentials.
- `pnpm check` builds, typechecks, runs unit/property tests, and checks the Function example.
- `pnpm test:package` builds and installs the tarball in an independent temporary consumer, checking declarations, runtime exports, SQL assets, and licensing.
- For a focused unit case, use `pnpm exec vitest run --project unit test/unit/<file>.test.ts`.

The build cleans `dist/` before compiling. Do not overlap root builds, package checks, colocated test-artifact preparation, or Function bundling when they share `dist/`; serialize those preparation steps. Independently prepared deployments can run concurrently on isolated branches. Generated `dist/` is not committed.

## Testing against Neon

Database, Realtime, and Object Storage qualification uses **actual Neon services**, not Docker or a local substitute. Loopback HTTP servers are valid protocol/webhook fixtures; they do not replace the data services. Missing credentials must fail a required gate, not silently skip it.

See [docs/development.md](docs/development.md) for the full command sequence. For an isolated local environment:

1. Reuse available project-scoped credentials. If needed, copy `.env.example` to ignored `.env.neon.local` and configure a test project and non-expiring parent branch.
2. Run `mise exec node@24 -- pnpm neon:test:create`. The script records ownership in `.neon-test-state.json` and writes branch-scoped credentials to `.env.neon.test`.
3. Build the library before deploying a consumer or preparing a packed fixture.
4. Run the appropriate tests with the generated environment; clean up even after failure using `mise exec node@24 -- pnpm neon:test:delete`.

Do not test against the parent or an application's `DATABASE_URL`. Fixtures use explicit `TEST_DATABASE_URL` and `TEST_DATABASE_URL_UNPOOLED`, randomized `ds_test_*` schemas, and owned disposable branches. Use pooled connections for serving and direct connections for migrations/session-sensitive administration.

For a full data-service regression near the database:

```sh
NEON_QUALIFICATION_PROFILE=neon mise exec node@24 -- node \
  --env-file=.env.neon.local --env-file=.env.neon.test \
  scripts/neon-test-conformance.mjs
```

Set the profile to `integration` for transaction/process-loss coverage. Omit `NEON_QUALIFICATION_PROFILE` for the complete upstream conformance run. The runner packages the unchanged fixtures into an authenticated temporary Neon Function and checks file coverage, failures, and skips. Control-plane credentials stay with the provisioning process, outside that Function.

| Change | Relevant validation |
| --- | --- |
| Documentation only | Check links, commands, defaults, and API snippets against current source; no cloud qualification needed |
| Local state, parsing, encoding, or resource ownership | Focused unit/property tests and `pnpm check` |
| Public exports, dependencies, SQL assets, or package layout | `pnpm test:package` plus affected runtime checks |
| SQL, transactions, migrations, storage, replay, or live delivery | Affected real-Neon fixtures; full Neon data and integration profiles for broad changes |
| Protocol behavior | Full pinned conformance, with subscriptions and collection enabled, in addition to focused regressions |
| Deployment, coordination, recovery, or lifecycle contracts | Packed multi-instance, Function/webhook/cron, hints-only, and provider recovery gates as applicable |

The Neon data profile excludes only `test/neon/rolling.test.ts`, which requires management credentials and runs separately through independently packed consumer Functions. Recovery fixtures also require management credentials and allocate their own recorded branches. The ordinary Function profile disables hints to prove cron recovery; hints-only checks need a separate fresh branch with `NEON_TEST_MAINTENANCE_HINTS=on` and `NEON_TEST_NO_CRON=1`.

The pinned upstream suite currently contains **338 cases**. The colocated full run is the conformance gate; filters are for debugging. Preserve upstream assertions, generated samples, and delivery deadlines. A passing subset, retry, or earlier source revision is not a passing full qualification. Record failed attempts and unchanged-source rechecks honestly. Dependency changes can change the gate; review the pin and harness together.

## Correctness and design constraints

- **Postgres is authoritative.** Serialize mutations using the existing transaction/lock discipline. Preserve producer deduplication, generation isolation, subscription registration barriers, and atomic acknowledgement batches. Do not automatically retry ambiguous commits.
- **Realtime carries committed payloads.** Keep live SQL simple and unordered, using an authorized stream UUID and lower-offset predicate. Order, deduplicate, and enforce contiguous offsets in the Function. Retain durable replay for gaps, replacements, sealing, reconnects, and lifecycle validation. EOF follows the final contiguous bytes. Do not attribute latency to query shape without a matched measurement.
- **Offsets identify history.** Preserve lexical ordering, the canonical zero token, inherited fork prefixes, and rejection of abandoned history after restore/rewrite at the same ordinal. Keep per-frame lengths and JSON indexes in object blocks rather than permanent sealed SQL rows/arrays.
- **Publication is atomic.** Upload and read-back verification occur outside SQL transactions. Publish the verified manifest and delete its exact staging range together, guarded by claim identity, epoch, and deadline. Preserve current reader/fork/subscription references during collection.
- **Verification precedes exposure.** Validate selected blocks and exact range responses before ordinary response headers. An SSE checkpoint follows its complete data event. Preserve exact JSON numeric lexemes, UTF-8, newline framing, and binary/base64 behavior.
- **Resources have owners and bounds.** Keep buffer ownership explicit, queues/budgets finite, backpressure demand-driven, and shared download interests independent. Cancellation, expiry, rotation, shutdown, and late callbacks must release resources without reviving closed work. Server shutdown does not close consumer-owned clients.
- **Lifecycle state stays explicit.** Prefer tagged unions and pure transition decisions around authoritative SQL state. The current design does not need XState or a second persisted workflow/actor journal. See the [lifecycle rationale](docs/lifecycle-design.md).
- **Administration is explicit.** Constructors/imports never provision resources or execute DDL. Recovery, adoption, and collection enablement stay on trusted operator paths. Recovery must verify every reachable sealed object; provider historical object recovery is a separate contract, not something branch cloning alone proves.

## Schema and package changes

The baseline is organized as installation, streams, storage, and subscriptions in `migrations/0001_*.sql` through `0004_*.sql`. Keep development changes in their logical baseline file and update constraints, types, codec fixtures, examples, and contracts together. Migrations validate receipts/checksums and reject incompatible installations; never silently rewrite an installed ledger to make it pass.

One object format and codec ship. Fresh development state does not require preserving old readers. The current golden fixture is `test/fixtures/chunks/v3.hex`. Check public entrypoints in `package.json`; internal modules and test fixtures are not public API. All claims about package contents should be checked against a built tarball.

## Credentials, cleanup, and evidence

Never commit or print `.env.neon.*`, `.neon-test-state*.json`, connection strings, Realtime secrets, storage credentials, or subscription keys. `.env.example` contains names only. Use `node --env-file=...` to load ignored files; avoid dumping environments into logs. Runtime consumers do not need a Neon management API key.

Record branch/Function ownership before allocation and use the existing cleanup scripts. `NEON_TEST_STATE_FILE`, `NEON_TEST_ENV_FILE`, and `NEON_TEST_FUNCTION_ENV_FILE` allow separately recorded environments. Never overwrite an existing ledger or delete another run's resources to unblock your own. Clean owned branches, Functions, temporary consumers, and matching generated credential files, including recovery fixtures after failures. The CI janitor supplements cleanup; it does not replace it.

Keep raw qualification output in ignored `qualification-results/`. Commit only sanitized evidence under `docs/qualification/`, with source/bundle/harness/dependency identities, settings, counts, scope, failures, and cleanup. Distinguish response-header snapshots from body-lifetime measurements, per-instance memory from fleet memory, and measured closed-loop throughput from a capacity limit. Do not regenerate historical reports to make them appear current.

## Finishing work

Preserve unrelated uncommitted edits and stage only the task's files. Run checks appropriate to the change, summarize their actual outcome and limits, and update the relevant user/developer documentation when behavior changes.

Use **`[skip ci]` in commit messages when pushing**, as requested for this repository, unless the user changes that instruction. Run required validation directly and report it accurately; a skipped workflow is not evidence of a passing CI run. Keep the package private/unpublished unless publication is explicitly requested.
