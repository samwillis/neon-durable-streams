# Reclamation contract — implementation draft

The main schema-0005 release candidate retains objects and frame indexes. This isolated implementation branch adds schema 0006: replay pins, bounded materialization, retirement and collection, scoped object inventory, upload intents, signed wake tokens, and dependency-ordered metadata cleanup. Collection is disabled initially. Only explicit quiesced administration can enable it. Production qualification is still underway; this branch is not yet a qualified release.

The collector must reclaim only unreachable stream generations after an explicit recovery horizon. A manifest's absence from a current read is insufficient: forks, materialized replay plans, paused readers, stale seal workers, retained backups, and restored databases can all explain apparent non-use.

## Compatibility boundary

Reclamation needs a new checksummed migration and a stopped-ingress upgrade from schema 0005. Migration 0006 changes the installation serving epoch atomically to fence runtimes already pinned to the earlier epoch. New pre-collector instances reject the new migration history. Stop all old runtimes and workers before this upgrade, including instances still initializing: an old binary can read its supported migration list before the DDL and pin its identity afterward. Epoch changes supplement the stopped upgrade; they cannot replace it or make a database rollback safe.

Current collector-aware binaries may explicitly support schema 0005 with collection disabled, preserving the qualified same-schema baseline/current overlap. That compatibility must be exercised on an actual five-migration installation. No binary without replay pins may participate in an installation where collection can run. A default migration to the new schema and a deliberately bounded old-schema compatibility fixture are separate procedures.

Reclamation policy is disabled initially. Enable or change it only during explicit quiesced administration, with an expected installation/epoch and an idempotent operation identity. The policy must declare the complete supported database recovery window and an additional safety margin. Operators must include retained external backups, not just Neon PITR, in this window. An installation promising indefinite historical restores must retain its objects indefinitely.

## Readers and acquisition

A replay page captures local bytes or immutable chunk manifests in one database snapshot. Local bytes already copied into the plan need no object pin. For sealed references, the same transaction must acquire shared chunk locks in a deterministic order, reject retired chunks, and persist expiring random replay pins before committing. A collector locks those same chunks exclusively before retirement. Acquiring an object pin after the snapshot closes is unsafe.

Pins cover partial-fork preparation as well as ordinary GET and live catch-up. A partial fork materializes its prefix outside SQL, then rechecks the source generation and fork eligibility under the existing namespace/stream locks before publishing its edge. Live payload forwarding does not need object pins; durable gap/reconnect replay does.

Generation replay currently uses read-only REPEATABLE READ. A pin-writing replay transaction must preserve that snapshot isolation while allowing the pin insert; changing it to ordinary READ COMMITTED would weaken the current snapshot contract. Initial reads retain their stream/path locks through pin acquisition. No database transaction stays open across object I/O.

Every materialization has a finite monotonic deadline that precedes its database-clock pin expiry. Check the deadline before starting another object request, after downloads, and before returning bytes. Always release pins after work settles. Expired work fails closed and cannot emit a late page. Consumer adapters and pools still need finite I/O/acquisition deadlines; abandoning a promise must not create unbounded detached work. Cancellation retains the existing admission/drain contract.

The foundation exposes `limits.replayTimeoutMs`, default 60 seconds and bounded from 1 through 300,000 ms. Schema-0006 replay pins last 315 seconds on the database clock; materialization checks both its earlier local deadline and persisted pin validity. Read-only generation snapshots become REPEATABLE READ read-write transactions only to persist their pins. The original schema-0005 compatibility installation is selected explicitly with `migrate({ target: '0005_operations', ... })` and has no collection/pin tables.

## Retirement and deletion

A candidate generation is deleted/expired, detached from its namespace, has zero active fork references, and has completed ancestor-edge cleanup. Fork acquisition and logical retirement continue to serialize on the existing stream locks. A retained parent with live descendants is never a candidate.

Under the installation guard and stream/chunk locks, retire a bounded chunk batch. Fence any FROZEN upload claim by changing its token/epoch so a stale worker cannot publish after retirement. New seal claims/publications skip retired chunks. A worker may leave an immutable orphan after fencing; orphan handling must use recorded upload intent and the same retention protection, not a bucket listing alone. Deleted generations with unchunked HOT data need a bounded retirement path as well.

Persist each immutable object's exact key, digest, size, retirement time, and collection claim. Do not remove the only retry identity before an ambiguous deletion is resolved. Object keys are never reused. The retained chunk format remains readable; collection metadata belongs in Postgres.

An object becomes eligible only after its retirement time plus the declared recovery window and safety margin, and when it has no unexpired replay pin. Claim batches with bounded work and random expiring ownership. Object I/O stays outside SQL. A lost successful delete response is retried idempotently for the same exact key. Metadata finalization requires the current claim, serving epoch, and retired identity; a stale worker cannot finalize another worker's collection.

Recovery verification must distinguish live sealed manifests from retired, unreachable manifests. A supported restore point after retirement can still contain a retired manifest whose object has since been collected; requiring that object would make an otherwise valid restore impossible. A restore point before retirement needs the object and is protected by the recovery horizon. Missing reachable objects keep the installation quiesced. Recovery activation fences collection claims and gives restored retirement work a conservative new hold before resuming collection.

Generation/path, producer, outbox, subscription, and collection-tombstone metadata need separate bounded retention rules. Deleting chunk payloads does not by itself bound those tables. Processed outbox records may be removed only after proving that registration/bootstrap and fan-out checkpoints no longer depend on them. Empty namespace coordination rows are reclaimed only after all generations disappear; creators verify that every namespace claim survived acquisition before publishing any generation.

## Required evidence

- Replay snapshot versus retirement in both commit orders, including pins acquired from an inherited fork range and partial-fork preparation.
- Reader suspension, cancellation, expired pins, late object completions, bounded queues, and no emission after materialization deadlines.
- Parent deletion with live descendants, recursive last-edge cleanup, expiry/recreation, and generation isolation.
- Upload before/after retirement, stale publication claims, successful deletes with lost replies, process loss around each collection transition, and idempotent recovery.
- New references rejected after retirement; active references and unexpired pins prevent deletion. Every physically deleted object has recorded retirement and a satisfied recovery hold.
- Actual Neon Object Storage deletion on an isolated branch, followed by supported-point branch and in-place restore drills. Verification fails for a deliberately missing reachable object.
- Stopped schema upgrade with producer/fork continuity, old-runtime epoch fencing, and qualified schema-0005 binary overlap with collection unavailable.
- Full unfiltered upstream conformance after the storage change. Sustained runs include repeated physical collections and demonstrate bounded payload, queue, connection, and metadata behavior for the supported profile.

Physical deletion is disabled by default. Enabling it requires explicit stopped administration and a declared recovery policy; the package remains an unpublished release candidate.

## Collector implementation notes

Retirement must persist a `collect_after` date for both the chunk and each object intent. A later shorter policy must not accelerate already-promised retention. Policy/recovery activation adds a conservative installation-wide hold; increasing a horizon cannot resurrect objects collected under an earlier policy.

Register exact object key/hash/size under the current FROZEN claim before calling `put`. Retirement fences that claim and copies every known intent into the collection path, including uploads not yet published. A process can resume or finish an external upload after retirement; repeated inventory must discover that late immutable object again. SQL epoch checks cannot revoke an already-issued object-store operation.

Inventory is confined to `v1/<installation UUID>/`. Persist bounded pages/cursors and discover candidates without deleting them. Missing current manifests alone are insufficient. Verify an unknown object's full envelope, checksum, identity, and bounded size outside SQL, recheck the manifest under the retirement barrier, then give that orphan a new retirement hold on the database clock. Do not derive eligibility from S3 LastModified: a rollback can discard a very recent manifest for an object created years earlier, while a retained backup of that discarded timeline still needs it. The FK-free object ledger retains retry identity after chunk metadata disappears.

Persist fair work-class rotation (retirement, object collection, retired metadata, inventory, orphan verification). A cold runtime with a one-round soft budget must not always restart at retirement and starve other work. An inventory page can register bounded candidates in SQL; each full orphan verification has its own finite object-I/O budget. No SQL transaction spans those downloads or deletes.

Metadata pruning needs separate bounded steps for completed wakes/snapshots, deleted subscriptions, old processed outbox records, producer sessions, retired chunk indexes, deleted generations, and inactive namespace rows. Preserve current/unexpired wakes and pending fan-out. Clear foreign-key dependents in bounded batches, working leaf-first through the retained fork DAG. A namespace row may disappear between INSERT ON CONFLICT and SELECT FOR UPDATE: creation/fork code must reject a missing claim and roll back, never acknowledge a generation whose namespace UPDATE matched no row. This stronger acquisition check is required before namespace reclamation is enabled.

Complete positive collection, stale upload/delete ambiguity, orphan inventory, metadata/namespace races, stopped schema upgrades, recovery, exact packaged-Function behavior, full conformance, and physical-GC load qualification before merging this implementation into the release candidate. The schema-0005 mixed-binary fixture remains an explicit no-collection compatibility profile.


## Administrative policy and maintenance

Stop ingress and every serving/maintenance process before a schema-0005 upgrade or a restore, including cold-starting instances and object operations already in flight. No SQL lock can revoke an external object request that has already started. Use the existing recovery procedure and a persistent operation UUID. While its token is active, configure policy once:

```ts
import { configureReclamation } from '@samwillis/durable-streams-neon/operations'

await configureReclamation(directPool, {
  schema, token, enabled: true,
  recoveryWindowMs: 8 * 24 * 60 * 60 * 1000,
  safetyMarginMs: 15 * 60 * 1000,
})
// Complete verifyRecovery/finishRecovery before deploying fresh instances.
```

Identical configuration retries under the same recovery token are idempotent; conflicting retries fail. To disable collection or change the horizon, begin another stopped recovery operation. The minimum safety margin is fifteen minutes; the recovery window can be zero only for an installation explicitly supporting no historical database recovery. Include all retained external backups in the declared window. A shorter configuration cannot shorten an already-recorded object retirement deadline or global hold. Raising a window cannot restore objects collected under a previous policy.

Existing `maintain()` calls execute collection when policy enables it; consumers need no second always-running server. A durable six-phase cursor rotates retirement, exact-key object deletion, retired chunk indexes, bounded inventory, orphan verification, and other metadata. A nested durable cursor rotates metadata classes. The server's existing work/time budgets still bound rounds. `reclaimedChunks`, `deletedObjects`, `prunedRows`, and `inventoriedObjects` are additional maintenance counters. Object I/O and queues retain finite adapter bounds. Consumers supplying their own adapter must implement idempotent `delete`, bounded `list`, and `ChunkNotFoundError` for a missing object before enabling collection. Readiness rejects missing collection capabilities.

Inventory scans only the installation's immutable prefix, in pages of at most 128. A completed scan waits five minutes before restarting; incomplete scans continue from their durable cursor. Failed object work backs off for five seconds so another candidate can progress. Corrupt objects are quarantined in `chunk_objects.quarantine_reason` and never automatically deleted. Alert on that field, old uncollected retirement work, orphan candidates, and failed maintenance. Invalid/noncanonical keys or oversized objects outside the chunk format remain operator-owned objects and are not deletion candidates.

Completed wake snapshots, deleted subscription rows, historical processed outbox records, dead producers, chunk frame indexes, leaf generations, empty namespaces, expired pins, and completed collection receipts have bounded cleanup steps. Current/unexpired wake work, pending fan-out, subscription references, active fork references, and valid replay pins prevent the relevant cleanup. A deployment with subscriptions must continue running its subscription worker; collection never discards undelivered fan-out to make progress. Without active subscriptions, old outbox records can be consumed under the same registry barrier as registration.

Schema-0006 wake tokens use a purpose-separated HKDF/HMAC-SHA256 signature over the installation, serving epoch, tenant, subscription UUID/name, wake UUID, generation, delivery type, and random nonce. Every scoped operation verifies the signature before state lookup and still checks the database-clock lease, hash, identity, and scope. There is no immutable token expiry claim: heartbeats extend the durable lease, and a stale signed identity remains verifiable after its wake/subscription rows are pruned. Retain verification keys while authentic stale-token fencing is supported, in addition to the database recovery window. Removing a key makes its tokens unauthenticatable (401). A single bounded Postgres sequence preserves increasing generations across subscription-name pruning; serving epochs fence restored sequence state.

Legacy schema-0005 tokens retain their original wire format only in the no-collection compatibility profile. After migration, they authenticate against their original hash and always return FENCED. Before pruning an old token's wake, the collector records its hash/name receipt. This finite legacy receipt set is retained so genuine stale tokens remain distinguishable from forged tokens. New schema-0006 wakes never add legacy receipts.

## Qualification and limits

The collector implementation at `b9b0de5` passed every required CI job, including 81 Neon cases, 21 transaction/process-loss cases, 53 unit/property cases, both recovery drills, nine public Function cases, two hints-only cases, and all 338 unfiltered protocol cases. The later local conformance fixture explicitly enabled collection before serving and passed 338/338 with real fifteen-minute holds. The packed consumer also passed the separate ninth stopped schema-0005 → 0006 upgrade case. These reports identify their exact source and do not qualify later code automatically.

A 45-minute Frankfurt deployment crossed real five-minute recovery plus fifteen-minute safety holds. It completed 264 sealed source/fork lifecycles and independently confirmed 149 collected keys missing. Ephemeral generation/chunk metadata stabilized around the retention window; receipt pruning began after the second interval. The lifecycle harness issued an explicit maintenance call to seal each source before deletion, in addition to cron and append hints. This is evidence for that combined workload, not proof that default scheduling alone can sustain that lifecycle rate.

The concurrent load replayed all 125,786 acknowledged records exactly, with all 16 live readers' ordered byte digests matching durable replay across 720 SSE rotations. The overall benchmark **failed** its independent head probe: 16,830 records remained uncovered after its twenty-second drain. Fresh direct Realtime connections subsequently returned HTTP 502 on that disposable branch while the existing smoke branch connected. Queries were simple and unordered. Root cause remains unresolved; neither the query shape nor a provider restart/restore has been established as the cause or repair. Preserve the failed report when assessing availability and latency.

Physical-collection recovery passed separately: a supported-point child restore verified 1,922 reachable chunks, and an independent stopped-Function in-place restore verified 4,103. Both retained an unreachable retired manifest whose object had actually been collected. Exact public binary/JSON/fork replay and producer deduplication passed, with owned branches cleaned up. The in-place test explicitly accepted only the independent head diagnostic failure while requiring all load data-safety checks; it did not change the failed benchmark. Its live-delivery-before-periodic-fallback observation alone cannot prove which path supplied the bytes, because initial catch-up also reads SQL. The required recovery fixture now additionally awaits a direct raw append subscription before issuing the new write and requires its committed payload independently.

The final hardening revision adds actual-Neon regressions and fixes for sealing deleted HOT parents retained by live forks, draining expired replay pins in batches of 128 before deleting a chunk, and enforcing replay deadlines after pin release and final serving validation. Final-source required CI remains a merge gate. See the [qualification index](qualification/README.md) for unchanged raw reports and scope; the package remains unpublished.

Read policy through `inspectReclamation(directPool, { schema })`; schema 0005 returns `{ supported: false }`. Setting `includeBacklog: true` also aggregates pending deletes, orphan candidates, quarantined objects, and the oldest collection deadline. Run those optional aggregates on an operator pool at a measured cadence; their cost grows with retained object metadata. The ordinary policy query does not scan the object ledger.

The qualification tooling is separate from production entrypoints. On a fresh recorded test branch, `scripts/neon-test-reclamation-policy.mjs` configures a five-minute recovery window plus fifteen-minute margin before any Function is deployed. It refuses existing serving Functions/triggers or an existing Function environment file. Run `qualify-load.mjs` and `qualify-reclamation.mjs` together for 2,700 seconds. No tool shortens actual collection deadlines. `qualify-reclamation-recovery.mjs` observes physical collection, creates a child at a checkpoint still inside the supported window, and reconciles reachable storage. `qualify-reclamation-inplace.mjs` performs the independently recorded stopped self-restore and cleans both target and preserved branches. By default it requires every input report to pass. `DS_GC_RESTORE_ALLOW_HEAD_PROBE_FAILURE=1` is an explicit diagnostic exception restricted to head-probe errors with exact durable/live-byte safety and drained payloads; the recovery report records that exception and the original load report remains failed.
