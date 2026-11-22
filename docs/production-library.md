# A production Durable Streams server library for Neon

Research/design date: **2026-11-22**. Requirement: an embeddable library that users deploy and operate on their own Neon resources. Production quality is the release target. The full protocol is implemented and the pinned 338-case suite passes against real Neon data services. This document describes the complete production target; remaining operational qualification is tracked in the implementation plan.

The [protocol/platform investigation](investigation.md) provides the detailed storage design and pinned protocol target. This document defines the proposed library contract and the additional work needed for production use. All package names, APIs, and commands below are proposals.

The [plan review](plan-review.md) identifies correctness gaps and decisions needed before particular contracts are frozen. Follow the [implementation plan](implementation-plan.md) for the current milestone order and acceptance criteria.

## Recommended product shape

Ship a TypeScript npm library that implements the complete Durable Streams protocol and mounts as a standard Fetch handler. Include explicit maintenance, migrations, diagnostics, and a deployable Neon Functions example. The consumer owns its URL, authentication, policies, deployment, cloud account, and operations. Library maintainers own the protocol/storage implementation, compatibility policy, fixes, tests, and operational documentation.

There is an upstream precedent: [`@durable-streams/server-cloudflare`](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/server-cloudflare/src/index.ts) exports a handler factory that consumers mount in their own Worker. Its [package metadata](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/server-cloudflare/package.json) describes exactly that library model. A Neon implementation should offer similarly small integration code, with Postgres providing coordination and recovery.

Use one package with explicit subpath exports initially:

| Surface | Proposed responsibility |
| --- | --- |
| Root export | `createDurableStreams`, typed configuration, Fetch handler, lifecycle, health, bounded maintenance, observability interfaces |
| `/neon` | Realtime notifier, Neon Function lifecycle integration, trusted trigger-delivery adapter |
| `/migrations` | Versioned SQL, migration planner/runner, schema compatibility checks; usable without an ORM |
| CLI | Validate installation, plan/apply migrations, inspect jobs/storage, reconcile after restore, adopt a cloned environment |
| Example project | Consumer-owned `neon.ts`, API Function, maintenance Function, auth integration, migrations/deploy workflow, monitoring/runbook examples |

A placeholder name such as `@example/durable-streams-neon` avoids implying a published package or affiliation. Decide the actual name and license before release, preserving upstream Apache-2.0 attribution for reused code.

Keep public configuration focused on supported behavior. Internally separate protocol, transaction/storage, chunk encoding, notifications, and maintenance so each can be tested and evolved. Avoid promising arbitrary public storage adapters or exposing mutable log internals in the first release; each extension point becomes a compatibility and correctness obligation.

## Proposed consumer API

The integration should look approximately like this. This is an API sketch, not executable code in this repository:

```ts
import { createDurableStreams } from '@example/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@example/durable-streams-neon/neon'
import { pool, s3, authorize, signingKeys, recoveryPolicy } from './infrastructure'

const streams = createDurableStreams({
  basePath: '/v1/stream',
  publicBaseUrl: 'https://streams.example.com/v1/stream',
  postgres: { pool, schema: 'durable_streams' },
  objects: { client: s3, bucket: 'stream-chunks', keyPrefix: 'streams/' },
  notifier: createNeonRealtimeNotifier({
    url: process.env.NEON_REALTIME_URL!,
    secret: process.env.NEON_REALTIME_SECRET!,
    database: 'neondb',
  }),
  authorize,
  subscriptions: { signingKeys },
  recovery: recoveryPolicy,
})

export default { fetch: streams.fetch }
```

Consumers can route other paths in the same Function or mount the handler in Hono. The library owns paths beneath its configured mount, including reserved subscription routes, and returns ordinary `Response` objects. It must not bind a TCP port, install global signal handlers, set process-wide database configuration, or silently intercept unrelated application routes.

Proposed instance operations:

| Operation | Contract |
| --- | --- |
| `fetch(request)` | Protocol handler with request cancellation and bounded work; compatible with mounting below an application route |
| `runMaintenance({signal, timeBudgetMs, kinds})` | Claim and drain a bounded set of durable jobs; safe across concurrent workers, retries, and process loss |
| `inspectHealth()` | Structured capability, migration, notifier, and maintenance status; caller decides whether/how to expose it |
| `close({deadlineMs})` | Stop local admission, end tails cleanly where possible, cancel subscriptions, and stop owned resources within a bounded deadline |

Constructors/imports do not run migrations, provision resources, scan buckets, or start immortal workers. Database/schema compatibility is checked before serving work. Network work happens through explicit runtime operations and their documented initialization paths. A caller-supplied pool or S3 client stays caller-owned; `close()` does not terminate it. A convenience factory may create owned clients if the ownership is explicit.

Runtime dependencies should use data-plane credentials. Requiring an account-wide Neon management API key just to serve streams would unnecessarily expand privileges and couple availability to the control plane. Provisioning and deployment live in the example/tooling workflow.

Begin with the deployed Node.js 24 runtime and documented tested dependency ranges. Expose small library-owned configuration/result types, with a deliberately narrow `pg`/S3 integration boundary. Keep Neon SDK details behind `/neon`; isolate beta API changes there. The consumer's ORM and application schema remain independent of library SQL migrations.

## Who owns what

| Concern | Library responsibility | Consumer/operator responsibility |
| --- | --- | --- |
| Protocol | Complete semantics, error/header compatibility, test coverage | Choose mount and stable public URL |
| Storage | Transactions, chunk format, sealing, recovery/reconciliation, safe GC | Supply resources, capacity, backup retention policy, credentials |
| Identity | Require auth integration, enforce returned tenant scope throughout operations | Verify identities and define permissions |
| Maintenance | Durable jobs, fenced claims, idempotent workers, observable failure state | Deploy/schedule workers, set budgets, respond to backlog |
| Migrations | Reviewed, versioned plans and compatibility rules | Execute through a controlled deployment workflow |
| Reliability | Tested process-failure behavior, bounded degradation, upgrade/restore procedures | Select cloud plan/limits, operate alerts, run recovery drills, publish service SLOs |
| Releases | Supported-version policy, compatibility matrix, migration notes, fixes | Pin dependencies, stage/test upgrades, retire old instances |

Production examples should implement these responsibilities concretely. Leaving migration, worker scheduling, recovery, or authentication as unexplained TODOs would make the package hard to operate correctly.

## Reliability contract

Separate protocol compliance, data safety, delivery latency, and availability. Publish each guarantee with its assumptions and test evidence.

| Property | Required library guarantee |
| --- | --- |
| Acknowledged append | Payload, offset, producer state, closure, and recoverable wake intent commit atomically before success. Process eviction cannot erase an acknowledged append. This relies on the database's durability and excludes deliberate point-in-time rollback. |
| Retry safety | Producer tuple deduplication survives process restart, seal migration, and rolling upgrade. Ordinary appends retain their protocol semantics and may duplicate when a client retries an ambiguous result. |
| Read consistency | A read starting after append success on the same stream sees that append through the primary database path. Replay stays gap-free and ordered across hot/sealed/fork ranges. |
| Storage migration | Every committed byte remains readable through at least one published storage representation. Worker death cannot produce a missing or double-counted range. |
| Live delivery | Realtime accelerates notification. Durable catch-up and bounded fallback preserve correctness through notifier failures; latency degradation is visible. |
| Job recovery | Pending jobs and deadlines survive workers. Claims use database time, leases, and fencing. Timers and trigger deliveries never become the sole record of work. |
| Webhook delivery | Retried external delivery is at least once. Generation fencing protects internal cursor updates; it cannot make a receiver's external side effects exactly once. Receivers deduplicate by wake identity. |
| Isolation | Every stream, producer, fork, subscription, cache, job, and object lookup is qualified by installation/tenant/stream identity. A guessable path does not authorize access. |
| Upgrade safety | Old and new supported binaries may overlap without corrupting storage or changing existing offsets. Unsupported format/schema combinations fail before writes. |
| Corruption | Validate an emitted batch before advancing its cursor. Detect missing/corrupt sealed bytes, return an operational failure, and retain evidence; never substitute empty content. |

A lost connection while `COMMIT` executes has an unknown outcome. Retry known-aborted transactions on appropriate serialization/deadlock failures with a bounded budget. An uncertain commit is safe to retry automatically only with a durable operation identity, such as the producer tuple. A generic automatic POST retry must not introduce silent duplicates.

Do not promise a numeric service availability or throughput from source inspection. Since no workload is specified, qualify a benchmark envelope: stream counts and sizes, payload distributions, append concurrency, live readers, fork depth, retention, cold starts, noisy-neighbor load, and component failures. Publish curves and configuration with each result. Safety invariants apply throughout the supported envelope; overload is handled through bounded queues/admission rather than unbounded memory growth.

## Production architecture requirements

### Storage and transaction invariants

Use the hot-append rows, chunk manifest, fork edges, producer state, and durable work queue from the [storage investigation](investigation.md#logical-schema). Put an installation identity and schema version beside them. Library SQL uses fully qualified, safely quoted identifiers and explicit collations; it must not depend on the host application's `search_path` or locale defaults.

Version the chunk format independently from the package, SQL schema, and protocol. Persist offset boundaries and JSON message indexes inside sealed objects so dropping hot rows does not discard replay or sub-offset fork information. Use content-addressed object keys that include installation, stream, chunk, format, and body hash. A restored chunk may reuse its UUID while eventually containing different bytes; a UUID alone is insufficient to make its object key immutable.

Stream and producer state survive chunk migration. A short deduplication TTL copied from an in-memory implementation is inappropriate while the corresponding stream remains readable. Keep exact integer arithmetic and validate overflow; existing offsets remain stable across chunk rotation, backfills, and upgrades.

Require deterministic lock ordering and bound lock/statement/transaction times. Scope serialization to the stream or affected subscription; a database-wide append lock would unnecessarily limit independent streams. Keep uploads, webhook HTTP, and other external work outside database transactions. Shared read plans must use an explicit consistent snapshot rather than assuming several READ COMMITTED statements see one state.

Every write transaction explicitly selects READ COMMITTED, sets transaction-local `synchronous_commit = on`, and applies bounded transaction/statement/lock timeouts. Neon documents that synchronous commit and timeout settings can carry over through pooled backend connections; never inherit the durability policy from an ambient session. Multi-statement replay planning explicitly uses REPEATABLE READ. Lifecycle operations coordinate through persistent path-claim rows before resolving/locking stream identities, including creation of a path that does not yet exist.

Bound per-request body size, response batch size, queued work, fork traversal work, JSON parsing work, database pool wait, object downloads, and per-connection output buffers. Process oversized bodies incrementally up to the configured cap instead of trusting Content-Length or buffering an unlimited body. Avoid making one complete historical stream or an unlimited manifest part of process memory. Enforce documented resource limits using appropriate protocol errors; validate the defaults against the full suite.

### Realtime degradation and recovery

Realtime stays the preferred notifier, with a supported polling fallback inside the library. Subscribe to durable heads; share one watch per interested stream per isolate. Deduplicate/coalesce wakeups and batch head checks to avoid one SQL polling loop per reader. Bound the number of active watches and retry attempts, with jitter and cleanup after the last local reader leaves.

Fallback must cover a subscription that remains connected but falls behind, as well as explicit connection errors. A health/timeout policy periodically rechecks durable head state. Verify stream closure, deletion, expiry, and generation replacement as well as appends. Recovery never replaces a client's persisted protocol cursor with the notifier's internal cursor.

The current [Realtime overview](https://neon.com/docs/realtime/overview.md) marks it beta with no performance guarantees. Removing it from the data-safety path allows the library to retain safety during its failure; it does not guarantee the same latency, cost, or database load. Publish the degraded polling envelope separately.

### Durable maintenance and subscription timing

Separate maintenance classes and their budgets: sealing, garbage collection, subscription wake discovery/publication, webhook delivery, lease/retry scanning, and backfills. A large cold-data cleanup must not starve pending notifications. Each class needs indexed runnable-state queries, bounded batches, attempt/error records, retry policy, and a way to inspect or repair terminal failures.

Claim jobs in short transactions using database-clock deadlines and fencing tokens. External work may outlive a lease; publication and internal state changes must recheck the fence. Concurrent stale workers may make the same idempotent object upload or duplicate an external webhook, but may not publish an obsolete manifest or advance an ack for a later generation. Abandonment and lease expiry remain recoverable without an orderly shutdown.

The append path records a bounded stream-change outbox entry independently of subscription count. Durable matching/fan-out must define the registration/backfill handoff and per-stream processing rules before its implementation. Complete outbox work by processing state rather than assuming globally allocated sequence IDs reflect commit order. Registration and dispatch races are part of the correctness model, beyond merely persisting a delivery job.

The [reference subscription manager](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/server/src/subscription-manager.ts#L144) stores subscriptions in a Map; its retry deadlines and `setTimeout` handles are attached to those in-memory records. The reference HTTP server constructs this manager even when its stream store is file-backed. This is useful behavioral reference code, but needs a persisted state machine here. Existing conformance cases do not certify restart durability.

The Neon deployment template should provide both immediate bounded draining after relevant requests and a scheduled maintenance Function. Scheduled triggers support minute-level cron, whereas subscriptions have one-second minimum leases and short retry backoff. Persisting deadlines makes retry timing correct when a worker is available; it does not wake an evicted process at that deadline.

**Release condition:** demonstrate and document the recovery behavior after all workers disappear, including scheduler delay and overload. If the desired delivery/recovery contract needs a finer or stronger wake mechanism than documented Neon triggers provide, add a qualified worker/scheduling integration or keep that deployment profile unreleased. A running timer, self-invocation chain, or `waitUntil` alone does not establish durable scheduling. A separate maintenance Function still shares account concurrency limits.

### Authentication and tenant scope

Require an authorization integration in production configuration; an omitted callback must not make the server public. A development/conformance fixture can explicitly choose anonymous access. Support the consumer's identity provider or API keys without requiring Neon Auth or a particular web framework.

Authorization needs operation-aware context and a verified tenant identity. Enforce source read permission and destination creation permission for forks, stream access for explicit subscription membership, and permission over the whole possible namespace of a glob subscription, including future streams. Keep subscription identifiers and callback/claim token audiences tenant-scoped. Recheck authorization on bounded long-read renewal and define revocation/expiry behavior for active connections.

User-request auth, internal callback/claim tokens, and Neon maintenance-trigger provenance are distinct policies. A single JWT middleware applied to every path would reject valid protocol callbacks or platform triggers. The Neon trigger adapter must check provenance using the documented deployed platform behavior; a body/header equality test alone is not authentication outside that environment.

The [Functions auth documentation](https://neon.com/docs/compute/functions/authentication.md) confirms its URL is publicly reachable and the handler authenticates requests. Durable Streams clients use Fetch for SSE, so they can retain Authorization headers; an EventSource limitation should not force credentials into query strings. Use an explicit CORS policy, expose protocol response headers, and keep authenticated/private responses out of shared caches by default. Set the canonical public URL in configuration for Location, callback, and JWKS links; do not trust arbitrary Host or forwarding headers.

Scope runtime SQL privileges to the library schema, keeping migration DDL credentials separate. Realtime currently rejects RLS-enabled tables, so enforce tenant isolation in library queries and test it extensively; do not imply database RLS protects a table that must have it disabled for Realtime. Credentials, webhook signing keys, callback tokens, and payloads stay out of logs. Use persistent signing keys with rotation overlap, never a new key on each cold start.

### Admission and overload

Expose per-verified-tenant quotas and in-flight budgets, alongside configurable installation limits. Local counters only bound one isolate; shared quotas or leases are needed for a deployment-wide budget. Limit concurrent live reads independently from request rate, with headroom for appends and maintenance.

The current [Functions limits](https://neon.com/docs/compute/functions/reference/runtime-limits.md) document an account-wide default of 100 concurrent invocations. A consumer's other functions use the same budget. Application admission cannot reserve a platform slot for maintenance, and platform 429s may happen before library code runs. Separate Functions improve application control and observability but do not create independent platform quotas. Load-test the complete deployment and obtain sufficient account capacity for its published operating envelope.

Likewise, rejected traffic still reaches the native Function URL before application checks run. Offer guidance for consumers' chosen ingress protection without promising the library can provide volumetric protection or disable that URL. An optional proxy must preserve methods, query parameters, headers, streaming, and cancellation, and must be qualified against the SSE/long-poll tests.

## Migrations, upgrades, and distribution

Own the SQL schema and migrations as part of the library distribution. Track migration IDs/checksums, minimum/maximum compatible schema versions, chunk readers/writers, and enabled storage capabilities. Readiness reports mismatches before accepting writes; an instance must not partly operate against an unknown schema.

Migration execution belongs in deployment tooling using a dedicated connection and migration lock. A plan should show DDL, expected locks, online backfills, minimum runtime versions, and irreversible steps. It must coexist with the consumer's migration system. No startup DDL races or surprise table changes from importing a newer package.

Use expand/backfill/contract upgrades: first deploy readers that understand old and new representations, then activate new writers, then remove obsolete structures after incompatible instances and recovery requirements have retired. Backfills need durable progress and restart safety. Run mixed-version tests under writes, sealing, live readers, and subscriptions. A rollback that cannot understand newly written data requires an explicit forward-recovery procedure.

Ship compiled JavaScript, declarations, explicit exports, SQL/format assets, notices, and changelog. Start with a clear tested ESM/Node.js contract; add CommonJS only if it is deliberately supported and tested. Verify the packed npm tarball from a clean consumer project: install, typecheck, bundle for Neon Functions, import each documented entrypoint, migrate, and run a real protocol session. Tests against monorepo workspace imports alone miss missing files and dependency/export problems.

Pin development/conformance inputs and publish the supported dependency/protocol matrix. Use reproducible builds and package provenance, verify the actual published artifact, and document an update/security-reporting policy. Optional instrumentation integrations must not force consumers onto one logging or tracing vendor.

## Restore, branching, and garbage collection

Recovery spans database and object storage. Neon [documents](https://neon.com/docs/storage/buckets.md#bucket-branching) that a database restore leaves Object Storage unchanged; [database backups](https://neon.com/docs/postgres/backup-restore/backups.md) also do not constitute a complete archive of sealed bytes. The library needs a recovery procedure and a way to verify that every restored manifest reference resolves.

Garbage collection must protect current manifests, all dependent forks, active bounded reads, in-flight fenced uploads, and every supported historical backup/restore point. The [history window](https://neon.com/docs/postgres/backup-restore/history-window.md) is configurable project-wide, while snapshots and exported backups may live longer. Runtime code with data-plane credentials cannot discover every external backup. Require an explicit operator recovery/retention policy; when it is unknown, suspend destructive object GC and surface capacity warnings rather than guessing.

Object retirement must be transactional with the checks that prevent new read pins, manifest references, or upload publication from racing physical deletion. Specify that acquisition/barrier protocol and validate it with concurrency/failure tests before enabling destructive GC. A generous orphan grace period is an additional protection, not the reference-safety mechanism.

Recovery tooling should verify manifests and content, reconcile FROZEN/SEALED jobs, expire stale worker claims, and confirm keys/configuration before reopening writes. Test restore across append, sealing, deletion, and upgrade boundaries. Post-restore handling of offsets, producer epochs, delivery generations, and in-flight callbacks must be explicit: a rollback can invalidate data or tokens already observed outside the database. It cannot honestly be described as preserving every acknowledged write. Use an operator-controlled recovery incarnation to fence pre-restore work and define the resynchronization contract for rolled-back cursors.

Forked environments also clone pending subscriptions and work records. Neon [disables inherited platform triggers on child branches](https://neon.com/docs/compute/functions/triggers/overview.md#triggers-and-branching), which helps, but requests or opportunistic draining could still activate copied application jobs. Bind an installation to its serving environment and require an explicit adoption step after cloning. The default adoption profile disables inherited external delivery, issues environment-specific keys/tokens, and requires new callback/public URLs before delivery is enabled. Separate branch credentials and validate resource binding; a bucket prefix alone cannot prove that the database and object client point to the same branch.

## Operations and observability

Provide structured events and an optional OpenTelemetry integration. Instrument append commit latency and ambiguous outcomes; read latency and bytes by hot/sealed source; live-reader/watch counts; notifier reconnect, lag and fallback; pool/lock waits; frozen bytes and oldest seal age; pending-job age and attempts; subscription delivery lag/lease loss; GC deferred by recovery policy; corruption/missing objects; and resource admission failures.

Use bounded-cardinality metric labels. Per-stream paths and producer IDs belong in carefully redacted diagnostic context when needed, not unbounded metric dimensions. An instrumentation callback failure must not turn an already committed append into an apparent failed append. Application hooks that need durable business side effects require a durable outbox or an explicitly designed transaction API; a generic post-commit callback is not a reliable event bus.

Keep liveness, write readiness, historical-read readiness, and degraded notifier/maintenance status distinguishable. Realtime failure alone need not reject durable writes. Storage failure can leave hot reads/writes operational until a configured backlog high-water mark; historical ranges requiring missing storage must return operational errors. Missing migrations or invalid key configuration block the affected capability. Health probes need bounded cost and a documented effect on scale-to-zero.

Ship runbooks for database disconnect/unknown commit, notifier outage, object-storage throttling, maintenance starvation, schema mismatch, corrupted chunk, restore, branch adoption, signing-key rotation, pool exhaustion, and a slow consumer. Neon [database metrics/logs](https://neon.com/docs/reference/metrics-logs.md) complement these; they do not replace application-level job and correctness signals. Default library logs should have enough IDs and state to investigate without stream bodies or secrets.

## Production release gates

The full protocol gate has passed (338/338 with subscriptions enabled), alongside focused crash, isolation, real Function ingress, and point-in-time branch recovery checks. The complete release matrix below remains open; passing conformance alone does not qualify production. See [development](development.md) and [operations](operations-v1.md) for current evidence.

| Gate | Required evidence |
| --- | --- |
| Protocol completeness | Full pinned upstream server suite, including `subscriptions: true`, with no unexplained skips. Resolve and record specification/test ambiguities. Preserve the exact suite source and seeds. |
| Invariant testing | Model/property tests for append ordering, deduplication, closure, forks, lifecycle, and ack fencing; exact bytes/cursors compared with the model. |
| Crash/restart safety | Terminate processes before/after database commit, upload, manifest publication, lease changes, wake publication and callback completion. Prove no lost acknowledged data or corrupted ranges. |
| Network ambiguity | Drop successful responses and connections during commit/upload/delivery. Retry using operation identities; prove expected duplicate/unknown-result behavior. |
| Real Neon deployment | Test the packaged handler through actual Function ingress, real PG18, Object Storage, Realtime, triggers, scale-to-zero, and multiple isolates. |
| Isolation and resource bounds | Cross-tenant/fork/subscription authorization tests, cancellation, slow readers/bodies, bounded buffers, key rotation, overload, and credential failure. |
| Upgrade compatibility | Fresh installs, supported old-schema upgrades, rolling mixed binaries, resumed producer sessions, backfill restart, chunk-reader compatibility, and documented rollback limits. |
| Recovery | Database rollback, retained object verification, missing-object failure, branch adoption, stale-job fencing, disabled copied delivery, and key/token recovery. |
| Sustained operation | Documented-duration load/soak tests with repeated seals/GC, cold starts and injected dependency failures; no unexplained memory, connection, job, or hot-storage growth. |
| Published artifact | Install the packed/published package in a separate example app; verify declarations, exports, migrations, licensing, reproducibility/provenance, and deployment. |
| Operational handoff | Runnable example deployment, capacity/limit measurements, dashboards/alerts, backup policy, dependency support matrix, and tested runbooks. |

The unchanged upstream webhook fixtures bind to test-runner loopback, so they can run against the local handler with real remote storage. Required deployed webhook qualification now uses a separate Neon Function as a reachable HTTPS receiver, checking signatures, synchronous/asynchronous completion, and retries. Both paths are required; a green local suite alone does not certify Neon ingress or lifecycle behavior.

## Platform qualifications still needed

1. **Deployment capacity:** certify live-connection accounting, limit behavior, pool growth, cancellation, SSE flushing, and cold starts. The default account invocation ceiling is a material operating constraint, even for a well-written library.
2. **Recovery timing:** establish the usable scheduler/retry profile after total worker eviction. Document lateness and the measured recovery distribution; a minute-level sweep is not a hard one-second wake guarantee.
3. **Object semantics:** qualify upload visibility, immutable retry behavior, checksums/ranges, throttling, branch/reset ordering, and restore retention. SDK compatibility is not evidence for every needed consistency property.
4. **Service guarantees:** Neon's [published SLA](https://neon.com/docs/legal/sla.md) expressly applies to Postgres Compute Endpoints, defined as PostgreSQL compute instances, and excludes other platform components. The cited public SLA does not establish availability commitments for this complete stack; consumer-specific contract terms may differ. Library correctness and end-to-end service availability need separate claims.
5. **Release compatibility:** verify published upstream/Neon SDK artifacts and dependency ranges when implementation starts. Pinned npm artifacts are now installed and tested; the conformance package source matches the pinned upstream revision. Published-release provenance and a supported dependency update matrix still need qualification.

Proceed with production package boundaries, versioned storage, durable jobs, and these gates from the first implementation. The library can offer a small handler API while carrying the recovery and operational machinery needed to make that API dependable.
