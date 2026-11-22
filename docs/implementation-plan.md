# Implementation plan

Updated **2026-11-22**, following the [plan review](plan-review.md). Target: a production-grade library implementing the full pinned Durable Streams protocol on consumers' Neon resources. Every milestone contributes to the same library; completion of the production release gates defines the releasable result.

Implementation is underway. Package/toolchain, explicit migrations, typed auth/mounting, process-loss tests, packed-consumer validation, and required real Neon CI are in place. The hot-log API, Realtime long-poll/SSE, and recoverable sealed storage have real Neon integration coverage. A deployed consumer Function passes ingress, authentication, streaming, sealed replay, and scheduled recovery checks. All upstream fork cases and additional fork races pass against Neon. Durable subscriptions and the full unfiltered 338-case suite pass with real Neon data services. Explicit quiesced recovery, environment/epoch fencing, installation-wide live admission, and a real point-in-time branch adoption drill are implemented. In-place restore, reclamation, sustained capacity, and the remaining production release gates are open. See the [operations contract](operations-v1.md). See [development](development.md), the [hot-log contract](storage-contract-v1.md), and [sealed storage contract](sealed-storage-v1.md) for current evidence and limits.

## Decisions carried into implementation

- One TypeScript package, ESM, Node.js 24, explicit Neon/migration entrypoints, consumer-owned clients, no implicit provisioning or migrations. Use a checked-in lockfile and Vitest/fast-check-based test tooling, with the exact dependencies qualified during setup.
- Postgres owns the durable hot log, metadata, producer state, fork graph, outbox, jobs, and subscription state. A hot chunk contains append rows. Immutable content-addressed sealed objects hold archived frames and indexes.
- Each write transaction uses one checked-out connection, explicit READ COMMITTED, transaction-local synchronous commit on, bounded timeouts, and scoped locks. Initial replay captures frames under the namespace/stream locks; background generation replay uses REPEATABLE READ. SQL object names are fully qualified.
- Initial path coordination uses persistent `ds_paths` namespace rows keyed by installation/tenant/canonical path. Insert-or-find the claim, then lock it for lifecycle changes and resolve/revalidate the active stream UUID. Unique constraints and a deterministic path/stream lock order supplement this protocol. Account for the retained namespace metadata in capacity tests.
- Append transactions persist bounded stream-change outbox records. Durable matching/fan-out uses per-stream change versions, processing status, and resumable progress; append latency does not scale linearly with subscription count.
- Realtime relays committed append payloads after authorized catch-up, using simple unordered raw queries, contiguous-offset checks in the Function, bounded windows, and durable replay on gaps. Small head and installation watches preserve lifecycle/epoch validation. Neither Realtime delivery nor post-response execution is required for append durability. See the [payload contract](realtime-payload-v1.md).
- Destructive GC requires a transactional retirement/pin/publication protocol and an explicit recovery-retention policy. Retain objects until that protocol is implemented and qualified.
- Complete conformance includes forks and subscriptions. The exact pinned upstream source remains the target until the matching published artifact is verified.

## M0 — Executable foundation and persistent contracts

Create the package skeleton, test harness, local Postgres 18/S3-compatible environment, migration runner, consumer fixture, and CI commands. Unit/property tests run without a Neon account. Required integration and conformance CI uses real Neon Postgres 18 and Realtime on isolated ephemeral branches; local Postgres and scripted notifiers supplement qualification. Missing Neon credentials must fail the qualification job explicitly. Establish a failure-injection mechanism that cannot be enabled through a production request.

Before finalizing migration 0001 and format v1, write short, executable contract tests/decision records for:

1. Offset encoding/arithmetic/overflow, stored append and JSON message boundaries, sub-offset semantics, and golden chunk fixtures. Ordinary page boundaries should end at complete stored append frames; size caps must permit one maximum stored frame to fit a replay page.
2. Canonical mount/path identity and typed identity/authorization hooks, including a fixed-tenant integration and callback/claim/maintenance authentication.
3. `ds_paths` acquisition, deterministic locking, lifecycle/expiry transitions, and stale-generation rejection. Resolve the live-read TTL ambiguity against the pinned protocol/client behavior before implementing reader lease renewal.
4. Object retirement, reference/pin acquisition, publication fencing, and recovery-retention rules. Specify the required transaction ordering even while destructive GC is disabled.

Run an early disposable Neon deployment experiment alongside this foundation: pooler transaction settings, real head notifications, SSE flushing/cancellation, object upload/read/ranges, serving-branch configuration, and pending-job recovery after all workers stop. Record observations and unresolved platform limits. Any need for an additional service is an explicit architecture decision.

**Exit:** a clean consumer can import/bundle the package; local services and migrations start through documented commands; the first storage/API contracts have fixtures; deployment evidence or precisely scoped outstanding qualification work is recorded. No claim of production readiness is made at this stage.

## M1 — Durable append, producer retry, and lifecycle

Implement the transaction wrapper, namespace/stream identity, PUT/POST/HEAD/GET/close/delete, exact offsets, bounded binary and JSON storage, and durable stream-change outbox. Include same-path create races, close/append races, bytewise Stream-Seq, producer fencing/deduplication, and restart replay. Expiry changes use database-time predicates and coordinate with readers/writers; HEAD never renews TTL.

**Exit:** the applicable upstream cases plus focused concurrent/state-model tests pass; acknowledged appends survive process termination; an ambiguous producer retry does not duplicate data; a stale waiter/job cannot attach to a recreated path; pooled ambient settings cannot disable synchronous commit. Unsupported later features remain tracked, without claiming full conformance.

## M2 — Prove tiered storage across failure

Implement HOT → FROZEN → SEALED, bounded worker claims, content-addressed uploads, verified format/index reads, atomic publication/local-copy removal, and snapshot-consistent replay. Exercise tiny thresholds to force frequent rotations. Keep destructive object GC off until its full acquisition/retirement protocol and retention protection are qualified.

**First complete storage scenario:** append → rotate → upload → publish → kill process → restart → replay across sealed/hot data → retry a producer tuple. Inject failure on both sides of every durable transition and compare exact bytes and offsets.

**Exit:** persisted fixtures and fault tests establish ordered, gap-free replay, no premature hot-data removal, safe orphan adoption, and fenced stale workers. Verify the same path against real Neon Object Storage before relying on provider-specific semantics. This milestone precedes implementing the full fork/subscription layers.

## M3 — Live reads and bounded degradation

Implement long-poll/SSE over durable replay, Realtime head watches, notification/setup race handling, coalesced fallback, EOF/lifecycle notifications, response-header behavior, cancellation, and bounded slow-reader buffers. Test real upstream clients and text/JSON/binary transport boundary cases.

**Exit:** append-during-subscribe, silent lag, snapshot replacement, notifier disconnect, process eviction, closure, expiry, and path-recreation cases pass. Real Function ingress validates low-volume flushing and cancellation. Record cold-start, fallback-load, memory, pool, and invocation-capacity measurements.

## M4 — Forks, retention, and qualified reclamation

Implement atomic parent-edge acquisition, inherited ranges, partial binary/JSON forks, recursive reads, independent writer state, soft deletion, and cascading logical cleanup. Implement and qualify reference/pin/publication barriers before enabling object deletion. Expose recovery-policy status and deferred-GC metrics.

**Exit:** all fork cases pass across HOT/FROZEN/SEALED data, including concurrent deletion/expiry, worker interruption, active reads, recreation, and recursive cleanup. No object needed by a live reader, dependent fork, or supported recovery point is deleted.

## M5 — Durable subscription state machine

Before implementing this layer, settle the R3 registration/dispatcher handoff: at-tail baseline, concurrent append, later stream discovery, processing order, fan-out checkpoints, and outbox retention while bootstrapping. Write a concurrency model and regression traces for the proposed SQL protocol.

Implement normalized configuration, glob/explicit membership, durable wake snapshots, pull-wake publication, leases/fences, callbacks, persisted retries, key management, and Ed25519/JWKS. Enforce lease validity with database time independently from when a scheduler happens to run.

**Exit:** the entire pinned server conformance suite, with `subscriptions: true`, passes. Additional restart/race tests cover registration, delayed commits, ack/delete, lease loss, pending events after wake snapshots, and duplicate delivery. The Neon maintenance profile has measured recovery behavior after total worker loss and under account contention; any remaining platform qualification blocks release of that profile.

## M6 — Release qualification and consumer operations

Turn the production design's [release gates](production-library.md#production-release-gates) into executable checks and recorded qualification artifacts. Complete mixed-version migrations/format tests, a quiesced restore/branch-adoption runbook, key rotation, capacity/soak tests, fault scenarios, dependency matrix, observability, and operational examples.

Validate the packed npm artifact in a separate consumer project using documented commands, real Neon deployment, and full protocol sessions. Publish finite supported limits with reproducible benchmark configurations, fault-recovery observations, and clearly scoped guarantees. The first production release includes the full protocol and qualified operations described above.

**Exit:** all release gates are satisfied with reviewable evidence. Known unsupported environments and deployment limits are documented without weakening the supported profile's correctness claims.

## Test selection and evidence

| When | Checks |
| --- | --- |
| Every code change | Typecheck/build, relevant unit/model tests, local database integration, affected protocol cases, and package export checks |
| Before merging storage/transaction changes | Relevant crash/ambiguity/concurrency tests, persisted format fixtures, and migration compatibility |
| Neon adapter or deployment changes | Real-service transaction, notifier, object, trigger, and Function-ingress tests |
| Scheduled/pre-release qualification | Full pinned conformance, all fault/upgrade/restore suites, consumer tarball installation, deployed HTTPS webhook fixture, and recorded load/soak runs |

Map every safety invariant to a test or drill. Record suite commit, package/dependency versions, configuration, workload, failure point, and random seed where relevant. Local Postgres/S3 tests provide repeatable development evidence; they do not certify provider-specific behavior.

## Capacity qualification findings

The ten-minute 1 KiB-record baseline safely replayed 38,360 appends but accumulated 274 frozen chunks and 18,912 pending changes under the old recovery-only cron budget. Live p95 reached 4.43 seconds. This blocks steady-state qualification. The next profile interleaves work classes, coalesces bounded changes, raises catch-up pages within the byte cap, uses full-chunk append hints, and reserves cron recovery. Separate required Neon branches qualify hints with no cron and cron recovery with hints disabled. See the recorded [qualification evidence](qualification/README.md); the revised profile still needs its own sustained run.

## Realtime payload direction

The user requests direct committed-payload delivery through Neon Realtime after initial authorization and durable catch-up. Use simple unordered append queries with a lower-offset predicate; order, deduplicate, and check contiguous durable offsets in the Function. Raw (`materialize: false`) updates avoid accumulating the result in the SDK. Bound both local buffering and the lifetime/lower-offset window of the server-side view. Durable PG/Object Storage replay remains authoritative for gaps, sealing removals/null payloads, reconnect/replacement snapshots, forks, lifecycle, and recovery validation. Final EOF follows contiguous final bytes. Preserve auth expiry and serving-epoch fencing.

Matched head/materialized, head/raw, append/materialized, and append/raw diagnostics on the unchanged baseline were all fast over 30/120 seconds. The head query already has no ORDER BY. These results establish payload-query viability; they do not explain the earlier latency outliers or isolate capacity. The matched scripts/reports supplied by the investigation side chat are retained under `/tmp/neon-rt-query-shapes.mjs` and `/tmp/neon-rt-shapes-{30,120}.json` during this session. Integrate regression coverage for ordering, subscription races, duplicate/replacement events, sealing, forks, slow readers, auth/epoch changes, and rerun the full unfiltered Neon suite.
