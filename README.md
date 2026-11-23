# Durable Streams server library for Neon

Licensed under [Apache-2.0](LICENSE). Copyright 2026 Databricks, Inc.

Design research dated **22 November 2026** for a production-grade TypeScript library. Users embed the library in their own server, deploy it to their own Neon Function, and operate their own Lakebase Postgres, Object Storage, and Realtime resources.

**Target:** full Durable Streams protocol conformance, durable recovery, safe upgrades, tenant isolation, bounded resource use, and documented operating limits. Production readiness requires evidence from fault testing, deployment tests, restore drills, rolling upgrades, and workload benchmarks, in addition to the conformance suite.

Start with the [production library design](docs/production-library.md) for the proposed package API, ownership boundaries, reliability contract, migration and recovery design, and release gates. The [protocol and platform investigation](docs/investigation.md) contains the underlying protocol/storage analysis. The [source manifest](docs/research-sources.json) records upstream source revisions and retrieved documentation hashes.

The [plan review](docs/plan-review.md) finds enough direction to start, with explicit transaction, lifecycle, subscription, and reclamation contracts to settle before those implementations are frozen. The [implementation plan](docs/implementation-plan.md) gives the current milestone order and acceptance criteria.

The main findings are:

- Current protocol scope includes recursive stream forks and a subscription control plane, alongside append, replay, long-poll, SSE, JSON mode, closure, TTL, and idempotent producers.
- The conformance CLI leaves six subscription tests disabled. The full target needs a programmatic harness with `subscriptions: true`.
- Neon Functions currently defaults to **100 concurrent invocations per account**. Treat open live reads as consumers of that budget until deployment measurements establish otherwise.
- Realtime requires **Postgres 18**, is in beta, and should signal changes to durable state. Reads should resume from persisted offsets.
- Sealing needs a recoverable Postgres-to-object-storage handoff. Historical restore requires Neon or a matching backup to recover every referenced object; the library protects current stream/fork/reader references.
- The upstream Cloudflare server is already distributed as an embeddable library, providing a useful API precedent. Its platform coordination mechanisms need a transactional Postgres implementation here.
- Neon's published SLA covers Postgres compute endpoints; it does not establish an availability guarantee for the complete Functions/Storage/Realtime stack.

The quality target is a production library. The unpublished release candidate provides an authenticated Fetch handler, explicit migrations, and a durable Postgres hot log. Realtime-backed long-poll/SSE forwards committed append payloads after authorized durable catch-up, with bounded raw windows and replay for recovery; see the [payload contract](docs/realtime-payload-v1.md). Recoverable sealed storage is implemented and tested against Neon Object Storage. Recursive forks and partial binary/JSON forks are implemented. Durable subscriptions, installation-wide live admission, and explicit branch recovery are implemented. The remaining production qualification gates are still open.

## Current implementation

The four-file [database baseline](docs/migrations.md) installs a compact hot/chunk layout: sealing deletes append rows and keeps one verified directory entry per object block. Ordinal/history offsets eliminate per-append sealed SQL metadata. Defaults seal at 4 MiB, 131,072 frames, or one hour; replay uses verified HTTP ranges and a bounded block cache. Earlier development migration histories require a fresh schema.

The API supports PUT/POST/GET/HEAD/DELETE, JSON batching, producer deduplication/fencing, Stream-Seq, closure, TTL/absolute expiry, conditional reads, tenant isolation, and bounded replay pages. Long-poll and SSE are available with an explicitly configured notifier. Subscription control APIs are available with an explicitly configured persistent keyring. Consumer-owned database pools remain open when the handler closes; importing the library never provisions resources or migrates a database.

```ts
import { Pool } from 'pg'
import { createDurableStreams } from '@samwillis/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@samwillis/durable-streams-neon/neon'

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 })
const notifier = createNeonRealtimeNotifier({
  url: process.env.NEON_REALTIME_URL!,
  secret: process.env.NEON_REALTIME_SECRET!,
  database: new URL(process.env.DATABASE_URL!).pathname.slice(1),
})
const streams = createDurableStreams({
  postgres: { pool, schema: 'durable_streams' },
  live: { notifier },
  auth: {
    authenticate: verifyCaller, // Return { tenantId, subject }, or null.
    authorize: checkPermission, // Check operation and path for that principal.
  },
  publicBaseUrl: 'https://streams.example.com/v1/stream',
})
export default { fetch: streams.fetch }
```

On Neon Functions, register the consumer's pool with `attachDatabasePool` from `@neon/functions`. Run `migrate` from the package's `/migrations` entrypoint through a direct connection as an explicit deployment step before serving requests. The package is currently consumed from this checkout/tarball; it has not been published to npm.

See [development and tests](docs/development.md) for commands and the [compact storage contract](docs/compact-storage-v2.md) for current ownership and resource bounds. Clean runtime `03ad7b6` passed direct qualification against actual Neon: **68 unit/property, 97 Neon regression, 21 transaction/process-loss, nine deployed Function/webhook, two hints-only, both recovery drills, and all 338 conformance cases**. Conformance enables subscriptions and collection. Automatic CI was skipped as requested; [evidence](docs/qualification/README.md) records the complete direct runs, the initial coordinator timeout and unchanged-source recheck, and earlier workload measurements. The [lifecycle design](docs/lifecycle-design.md) describes explicit ownership and typed transitions without an actor runtime. The package remains unpublished; workload capacity and provider historical object recovery retain their documented qualification limits.



For tiered storage, supply `storage: { objects }` using `createS3ChunkStorage` from `/s3`. Call `migrate(directPool, { storageIdentity: objects.identity })` explicitly and schedule bounded `streams.maintain()` batches from an authenticated worker or cron Function. Read the [sealed storage contract](docs/sealed-storage-v1.md) before operating the worker. Physical object deletion is disabled by default; the canonical schema supports an explicit collection opt-in during stopped administration and delegates historical object recovery to Neon or matching backups. Read the [reclamation contract](docs/reclamation-v1.md) before enabling it. Explicit quiesced recovery and branch adoption are available through `/operations`; read the [operations contract](docs/operations-v1.md) before cutover.

See the [Neon Function consumer](examples/neon-function/README.md) for deployment, ownership, authentication, and scheduled recovery wiring.

See the [subscription contract](docs/subscriptions-v1.md) for registration barriers, token/key management, worker scheduling, and delivery guarantees.

Consumer deployment, monitoring, recovery, supported dependencies, and traffic/storage responsibilities are documented in the [operator runbook](docs/operator-runbook.md).
