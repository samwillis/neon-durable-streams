# Lifecycle ownership and transitions

The library uses explicit TypeScript states and transactional Postgres operations. It has no actor runtime or additional workflow journal. The Realtime payload query stays simple and unordered; contiguous byte delivery remains the responsibility of `AppendBuffer`.

## Live payload windows

A payload window is catching up, backing off, subscribing, reconciling, forwarding, or closed. The three subscription phases own a single session containing its watch and deadline timer. Leaving that session revokes its identity before closing resources. Callbacks from earlier sessions are ignored. Synchronous invalidation while obtaining a watch closes the returned handle immediately.

Closing is terminal. A late SQL replay, subscription readiness/failure, snapshot, or timer cannot allocate another watch or wake the reader. The live reader still retains its installation admission lease until in-flight replay settles. A successful subscription requires a subsequent durable reconciliation before forwarding; revision checks prevent an older SQL snapshot from clearing a newer invalidation. EOF still waits for all final contiguous bytes.

`AppendBuffer` retains its own revision and replay-required bit. These describe data validity, independently of the session lifecycle. They protect against replacement snapshots, removals during sealing, unordered batches, gaps, and capacity overflow.

## Durable subscription decisions

`subscription-state.ts` contains pure decisions for lease ownership, delivery scheduling, completion, and membership changes. SQL reads provide deadline-validity facts using the database clock while holding the existing locks. The coordinator applies each decision within that transaction. Retry scheduling and reset writes have single implementations.

A published pull wake waits for a claim without repeated delivery or wake appends. A delivered webhook waits for its scoped callback. An expired lease, replaced delivery claim, deleted subscription, or recovered wake fences late completion. Membership capacity is independent of existing members, generation identity, and explicit/glob membership.

Queue membership means an outbox change is pending: completion deletes it atomically with durable fanout progress. There is no `processed` column. Pull-wake creation and its append commit together, so there is no `pull_published` column or subsequent publication-flag update. Database checks reject ownerless leases and half-populated delivery claims.

## Replay, publication, and recovery

Nullable SQL replay joins are decoded once into either a complete Postgres frame or a complete sealed object range. Pins and materialization consume that union; they cannot treat a missing object manifest as a hot frame. Stored offsets, payload lengths, and selected frame indexes are validated before the plan is used.

Chunk maintenance obtains a typed frozen claim, prepares bytes, records immutable upload intent, uploads and verifies outside SQL, then publishes with the claim/epoch/retirement predicates. Publication and exact staging deletion remain atomic. No persisted `uploading` state is added. Claim checks require ownership token and deadline together.

Recovery rows distinguish active, verifying, and verified operations in their types. The existing persisted serving-state/completion fields remain compatible with the simple Realtime identity query. Database checks require a complete operation identity when recovering and nonnegative progress. Completed-operation tokens still make activation retryable; restored serving epochs still fence old runtimes and wakes.

Stream availability, append closure, retained fork ancestry, and collection eligibility remain separate facts. Named availability helpers preserve the existing path/generation status semantics. Collection and metadata phases are named round-robin tasks with their original persisted cursor order, preserving fairness across instances.

## Why no XState dependency

After separating lifecycle ownership from payload validity and durable coordination, the local lifecycle fits in one small tagged union. Pure subscription decisions are ordinary functions. An actor runtime would add another lifecycle to operate without replacing the database locks, leases, epochs, or atomic publication protocol.

XState remains an option if future local orchestration needs hierarchical or parallel actors. Any adoption should own only active in-process resources, preserve the current SQL authority, and demonstrate simpler shutdown and transition logic. Persisting opaque actor snapshots alongside domain rows is unnecessary here.

## Validation

Regression tests cover canceled SSE with append watches and a late replay, synchronous subscribe invalidation, readiness/replay ordering, and retry backoff. Generated event sequences exercise late callbacks, snapshot replacement, timers, failures, and closure while asserting bounded watch ownership and terminal cleanup. Subscription properties cover stale completion and capacity/generation rules. Actual-Neon generated stream sequences check replayed bytes across append, close, deletion, and recreation; database tests reject impossible recovery/claim/lease combinations. Clean runtime `03ad7b6` passed the complete direct qualification: 68 unit/property, 97 Neon regression, 21 transaction/process-loss, nine deployed Function/webhook, two hints-only, both recovery drills, and 338/338 conformance cases. The [qualification index](qualification/README.md) retains the initial US coordinator timeout, the complete unchanged-code Frankfurt recheck, and exact source/deployment evidence.

This is an unpublished baseline change. Fresh schemas omit the redundant columns and include the stronger constraints; existing baseline checksums are rejected rather than silently rewritten. Historical qualification reports retain their original source and schema scope.
