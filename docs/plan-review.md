# Plan review: readiness to start implementation

Reviewed **2026-11-22** against the [production library design](production-library.md), [protocol/platform investigation](investigation.md), pinned upstream source, and the recorded Neon documentation. This is a design review; there is no server implementation to test yet.

**Verdict: enough to start implementation.** The component split, library ownership, safety goals, and release scope are coherent. The plan still needs concrete transaction/state-machine contracts before the corresponding storage or public API is frozen. Some deployment behavior remains unqualified. Start with the [implementation sequence](implementation-plan.md), which moves the most consequential risks earlier.

Priority meanings: **P1** can undermine durability, correctness, or the intended production deployment; **P2** needs resolution before API/storage commitments or release qualification. These are gaps in a proposed design, not observed bugs in implemented code.

## Findings

### R1 — P1: pooled connections leave a hole in the durability contract

The [transaction requirements](production-library.md#storage-and-transaction-invariants) specify atomic commits, but did not specify synchronous commit settings. Neon [explicitly documents](https://neon.com/docs/connect/connection-pooling.md#common-issues-with-set-statements) that `synchronous_commit`, `statement_timeout`, and `search_path` can carry over on pooled backend connections. A caller-supplied pool or another pooler user can therefore change behavior outside this library.

**Impact:** acknowledging after COMMIT does not establish the intended durability if that transaction uses asynchronous commit. Uncontrolled transaction settings can also undermine resource bounds.

**Decision added:** every write transaction explicitly sets transaction-local synchronous commit on and bounded timeout settings, using a single checked-out client. All queries qualify schema objects. Writes use explicit READ COMMITTED plus the relevant row locks; multi-statement replay planning uses an explicit REPEATABLE READ snapshot. Do not leak session-level SET state. Verify this through the real Neon pooler as well as local Postgres. The library still relies on the provider's documented durability underneath synchronous commit.

### R2 — P1: GC requires an atomic acquisition/deletion protocol

The [replay plan](investigation.md#replay-without-seal-races) offered “a read lease or a longer grace period,” and sealing proposed lease checks plus orphan grace. Neither description settled when a new reader, fork, or publisher becomes unable to reference an object selected for deletion.

**Failure ordering:** a collector observes no references; another transaction pins or publishes the object; the collector sends DELETE using its earlier decision. A large grace period reduces the opportunity but does not close it.

**Required design:** use an object lifecycle with a transactional RETIRING barrier. Acquiring a read pin or publishing a new manifest reference must synchronize with that barrier. GC may retire only after checking current references, protected historical state, active pins, and publication intents under the same ownership protocol. Once retired, the object cannot acquire a new reference. Physical deletion is retryable and recorded. Read pins must cover a bounded verified page, with renewal/expiry rules; never rely on an unenforced maximum read duration.

**Gate:** destructive object GC remains disabled until concurrent pin/publish/delete and worker-loss tests pass. Safe retention during earlier development does not reduce the production release scope.

### R3 — P1: durable wake intent alone does not solve subscription registration races

The [append transaction](investigation.md#append-transaction) says to record wake intent, while the subscription design calls for at-tail backfill and later stream discovery. There is no concrete handoff between those operations yet.

**Failure ordering:** subscription creation snapshots a stream at offset A; append B commits; a dispatcher handles B before the new subscription is visible; subscription creation completes at A. If B's work was globally marked complete, the new subscription can remain asleep with pending data.

There is also no bound on subscription matching work per append. Scanning/mutating every subscription inside each append transaction would turn a hot log into a subscription-count-dependent operation.

**Decision added:** write a constant-size stream-change outbox record in the append transaction and perform matching/fan-out in durable maintenance. Identify records by installation/stream UUID and stream-local change version, with before/after offsets. Complete work by durable processing state; do not use a global sequence high-water mark as proof that all earlier transactions committed. Sequence allocation and commit order differ.

**Still required before subscription implementation:** specify registration/backfill and dispatch synchronization, ordered per-stream processing or an equivalent merge rule, durable fan-out progress, and outbox retention while registrations bootstrap. A short registration/dispatcher barrier is one candidate; it needs a worked concurrency proof and bounded backfill behavior. Add tests for the failure ordering above, delayed commits, stream recreation, and pattern changes/membership removal.

### R4 — P1: row locks do not cover missing paths or unspecified expiry transitions

The [fork plan](investigation.md#forks-and-lifecycle) says to lock source/target identities, but an absent target stream has no row to lock. Concurrent creates, fork/create, and delete/recreate need a stable path-ownership protocol, in addition to stream UUID locks.

Expiry is also only described behaviorally. A sweeper must not expire a stream from a stale TTL read after another request renewed it. A child read of inherited data must not accidentally revive a deleted parent through a generic access-touch helper.

**Decision added:** use persistent namespace rows keyed by installation/tenant/canonical path, backed by unique constraints and retries. The namespace owns the active stream mapping; stream UUIDs identify generations. Before migration 0001, specify the total lock order and generation revalidation for create/delete/fork/expiry. Expiry checks and renewal use database time and transactional predicates/locks. HEAD stays observational. Explicitly resolve live-reader TTL semantics already flagged in the investigation before implementing reader leases.

**Gate:** demonstrate simultaneous PUT, fork versus create/delete, TTL renewal versus sweep, and an old waiter/job acting after path recreation. A uniqueness violation alone is not the complete lifecycle algorithm.

### R5 — P2: persistent encoding is still a list of choices

The [offset/format section](investigation.md#offsets-and-chunk-format) says the reference scheme could be adopted; the schema and chunk structure are sketches. These become costly compatibility commitments as soon as consumers retain data.

**Decisions required before storage-format v1:** exact offset grammar/allocation/overflow policy; request/batch/message boundaries; JSON stored representation and numeric behavior; sub-offset fork representation; frame/index/checksum layout; and maximum supported sizes. Keep physical chunk boundaries independent of public offsets. Golden fixtures must remain readable across releases.

The byte-preservation claim also needs precise scope. Binary replay and binary SSE/base64 can be byte-exact; JSON mode intentionally transforms framing. Native text SSE has line-ending/UTF-8 boundary concerns. Test split multibyte sequences, CR/LF, empty text events, and canonical JSON behavior against the specification and clients. Do not silently normalize stored binary bytes or promise an equivalence that the text transport cannot represent.

**Starting constraints added:** use exact integer arithmetic, C/binary ordering, complete stored append-frame boundaries for ordinary paging, and a stored-frame cap that fits a bounded replay page. The final wire offset and persistent format remain an explicit early deliverable, rather than an accidental consequence of implementation.

### R6 — P2: authorization and mounting need typed contracts

The [consumer API](production-library.md#proposed-consumer-api) names an `authorize` hook without its input/output types. It does not yet establish where a trusted tenant comes from on user requests versus callback/claim tokens, or how operations requiring two resources are checked.

**Required before exporting the stable API:** define early identity resolution, operation-aware resource authorization, failure responses, credential expiry/revocation, and distinct validation for callbacks/claims and maintenance. Support a fixed tenant as the simple consumer case while keeping internal identity scoped. Test mount-relative stream paths, encoded paths, fork source resolution, public URL construction, and reserved control routes consistently.

Authentication must happen before receiving expensive bodies, with later resource checks after bounded parsing where required. Caller hooks that perform irreversible side effects are outside the retryable database transaction. A host middleware returning an identity must not bypass library checks on fork sources, subscription membership, or callback audiences.

### R7 — P1 for deployment qualification: the scheduler profile remains unresolved

The plan correctly identifies minute-level triggers versus short lease/retry deadlines, but defers the choice until release. This could change the deployment topology, so it belongs in the first integration milestone.

Lease expiry can be enforced lazily on the next operation using database time; it does not inherently require a timer at exactly one second. Issuing a new wake or retry while there are no requests does require a future invocation. Separate the safety rule (a stale owner cannot ack) from the liveness measurement (how late a replacement delivery runs).

**Gate:** kill all workers and leave the service idle with pending work; measure recovery, delivery lateness, and backlog under the documented trigger model. Test with live requests consuming account concurrency. Keep the requested Neon deployment as the initial target. Any proposal requiring an additional service is a visible architecture change, not a hidden release dependency. Do not infer a platform recovery-time guarantee from a successful test or from cron cadence.

### R8 — P2: the development loop and milestone ordering defer core evidence

The current release gates are comprehensive, but there is no executable local test strategy, CI selection policy, or first integrated deliverable. The prior sequence also implemented the full hot-only fork behavior before qualifying the sealed-data path.

**Decision added:** establish Node.js 24/ESM, a lockfile, migration fixtures, a test runner, controlled fault points, and a local Postgres 18 + S3-compatible test environment first. Polling/scripted notifications support local deterministic tests; real Neon CI/deployment tests qualify provider-specific semantics. Testcontainers or Compose are reasonable development mechanisms. A local S3 implementation cannot certify Neon behavior.

The first durable integration slice should cross append, rotation, upload, manifest publication, process restart, and replay, including one idempotent retry. This tests the requested tiered architecture early. Full live modes, forks, subscriptions, upgrades, recovery, and release qualification then build on that foundation. No partial stage is the production release.

## Further release work that need not block the first code

- Turn the restore outline into a quiesce/fence/restore/reconcile/resume runbook. A restored database can contain old ACTIVE state and old lease counters; the operator-controlled recovery incarnation needs a source and activation procedure outside that rollback. Test stale writers and callbacks during the drill.
- Choose provisional finite limits and maintenance budgets, publish a memory/connection budget, then measure and adjust. “As good as possible” provides a quality objective; it is not a measurable load profile or a reason to defer bounded defaults indefinitely.
- Turn every production release gate into an executable check or an auditable qualification artifact, including duration/configuration for soak tests. Keep an invariant-to-test mapping and an explicit dependency/version support policy.
- Verify the published package/toolchain compatibility and pin the actual conformance artifact. Package metadata and historical upstream README pass counts remain insufficient evidence.

The architecture does not need a wholesale redesign. It needs the transaction contracts, state-machine proofs, and executable milestones above before its strongest promises become implementation assumptions.
