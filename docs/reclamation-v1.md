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

Until these checks pass, physical deletion remains disabled and the package remains a release candidate.

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

## Current development evidence

The signed-token/collector snapshot passed all **338/338** unfiltered upstream conformance cases inside a Neon Function, with subscriptions enabled and no failures or skips (123.09 seconds including Vitest setup). Its source was an in-progress dirty checkout; subsequent readiness/index/hold changes require independent qualification. Focused actual-Neon checks pass for retention/global holds, protected replay, deleted-parent/live-fork safety, lost delete replies, stale upload publication, orphan reappearance, metadata cleanup, signed/legacy token pruning, collection recovery verification, forced namespace races, expired delete claims, corrupt-object quarantine, and disappearing inventory objects. A stopped schema-0005 upgrade also preserves exact sealed/fork replay and producer deduplication while fencing the warmed old runtime.

Full final-tree Neon/packed-consumer qualification, stopped-Function physical-collection restore drills, exact committed CI, and a sustained deployed profile crossing real collection holds remain required before merging into the release candidate. Fixture-only deadline changes in focused tests do not qualify real elapsed retention.
