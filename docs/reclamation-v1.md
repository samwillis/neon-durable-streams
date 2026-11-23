# Stream collection and provider-owned recovery

The library collects chunks and metadata that current streams, forks, subscriptions, and readers no longer need. Historical database/object recovery belongs to Neon or the consumer's matching backup system. Schema 0007 removes the application's PITR window, safety margin, per-object recovery deadlines, and global restore holds. It keeps collection disabled by default and preserves the current-data safety barriers below.

## Stream offsets and database history

A fork at an old Durable Streams offset uses the source stream's existing history. Collection does not trim an existing stream's prefix. A deleted parent with surviving forks retains the generation and sealed bytes needed by those forks. Those references remain valid without a database-history grace period. Format v1 conservatively retains the parent's chunks while any descendant needs the generation.

A database point-in-time restore is different: it can bring a previously deleted generation back into the database. The storage provider or a matching backup must make that restored generation's original object keys/bytes available again, including objects deleted after the recovery point. An immutable key prevents overwrites; it does not recover a deleted object by itself.

Neon's [documented current behavior](https://neon.com/docs/storage/buckets#bucket-branching) coordinates buckets/objects with branching and reset-from-parent, but says point-in-time restore leaves them unchanged. Coordinated object PITR is an explicit platform dependency for historical recovery after collection, rather than functionality simulated by this library. Until that capability is verified for a deployment, keep collection disabled when database-only historical restores must remain possible, or supply matching object backups. Ordinary CI does not certify a future provider capability.

## Eligibility and active readers

Retirement requires a deleted or expired generation, no active namespace pointer, zero live fork references, and completed ancestor-edge cleanup. Fork acquisition and retirement serialize on the stream locks. A deleted HOT parent retained by a live descendant can still seal its bytes normally. No candidate can acquire a new reference after retirement.

A sealed replay plan acquires shared chunk locks in deterministic order under its SQL snapshot, rejects retired manifests, and persists expiring replay pins before committing. The collector takes the matching exclusive chunk locks. Pins also cover partial-fork preparation; direct Realtime payload delivery needs no object pin, but durable gap/catch-up replay does. Object downloads never hold SQL transactions open.

Materialization has a finite monotonic deadline, defaults to 60 seconds, and is bounded to 300 seconds. Database-clock replay pins last 315 seconds. Reads check the deadline after downloads, pin release, final serving validation, and body formatting. Expired work fails closed. A collector cannot delete an object while its pin is valid, and chunk cleanup drains expired pins in batches of at most 128 before deleting their parent row.

## Uploads, deletion, and inventory

Before each upload, the worker registers the exact immutable key, digest, and size under the current FROZEN claim. Retirement fences publication by changing that claim. An external upload already in flight can still complete; its object becomes a safe orphan, not a new reference to a dead generation. Every published object is downloaded and verified before local payloads are discarded.

Once a chunk is retired and no valid reader pin remains, object deletion is eligible on the next bounded collection pass. There is no application PITR delay. Random expiring delete claims preserve exact-key retry identity after an ambiguous response; repeated deletion is idempotent and stale claims cannot finalize replacement work.

Inventory scans only `v1/<installation UUID>/`, in bounded pages of at most 128. A listing alone never authorizes deletion: unknown objects are decoded and validated outside SQL, then checked against live manifests under the stream/chunk retirement locks. Invalid envelopes are quarantined. A late resumed upload is rediscovered even after its earlier deletion receipt has been pruned. Failed work backs off for five seconds; completed inventory scans restart after five minutes. These are work/retry bounds, not historical recovery windows.

## Metadata and tokens

Persisted cursors rotate six work classes and eight metadata classes so short/cold maintenance invocations cannot starve later work. Cleanup removes completed wake snapshots, deleted subscriptions, processed outbox records, dead producers, retired chunk indexes, leaf generations, empty namespace rows, expired pins, and completed deletion receipts in bounded steps. Current/unexpired wakes, pending fan-out, subscription references, active forks, and valid pins continue to block the corresponding cleanup. Stream generations are removed leaf-first through the fork graph. Namespace creators validate that every claim survived locking before publishing a generation.

Signed wake tokens retain their purpose-separated HMAC identity after wake/subscription rows disappear, so authentic stale work still returns FENCED. Keep verification keys while supporting that contract; deliberate key removal changes the result to 401. A finite legacy-token receipt table preserves authentic fencing for pre-0006 tokens. A single bounded sequence keeps generations increasing across subscription-name pruning; serving epochs fence restored state.

## Configuration and upgrades

Keep configuration on a trusted direct-pool operator path. Stop ingress, cold-starting runtimes, scheduled workers, and object work before a schema change or restore. Use `beginRecovery`, then configure collection with a single setting:

```ts
import { configureReclamation } from '@samwillis/durable-streams-neon/operations'

await configureReclamation(directPool, { schema, token, enabled: true })
// Complete verifyRecovery/finishRecovery before deploying fresh instances.
```

Identical retries under one operation token are idempotent; conflicting settings fail. Disable collection through another stopped operation. Removed `recoveryWindowMs` and `safetyMarginMs` arguments are rejected, including from JavaScript callers, rather than silently ignoring a prior retention requirement. Custom object adapters need bounded inventory, idempotent deletion, and `ChunkNotFoundError` for missing objects.

Migration 0007 is appended to the existing checksummed history; migrations 0001–0006 are unchanged. It fences cached runtimes, removes the old retention columns/index dependencies, and disables existing collection so an upgrade never silently opts into the changed recovery contract. New binaries accept the current seven-migration installation or the explicit schema-0005 compatibility profile, which has no collection. Schema 0006 requires a stopped upgrade. Rolling DDL is unsupported.

`inspectReclamation(pool, { schema })` reports support, enablement, and last completed inventory. Optional `includeBacklog: true` adds pending deletions, orphan candidates, quarantined objects, and `oldestRetiredAt`. There is no recovery-window/hold status. Run aggregate diagnostics on an operator pool at a measured cadence.

## Recovery and evidence

Recovery still stops old runtimes, verifies every reachable sealed manifest, drains old reader pins/leases, clears abandoned claims, activates a fresh epoch, and requires fresh deployments and representative protocol checks before ingress reopens. Retired unreachable manifests may remain after their objects are collected. Missing reachable objects keep the installation quiesced; no recovery API fabricates missing data.

The previous schema-0006 reports remain in the [qualification index](qualification/README.md) as historical evidence. Their real elapsed retention holds and restore proofs describe that implementation, not the schema-0007 provider contract. The old scripts that waited for application PITR deadlines have been removed. `scripts/neon-test-collection.mjs` enables collection only on a recorded fresh stopped test branch; `scripts/qualify-reclamation.mjs` tests current fork safety, actual object deletion, and bounded metadata drain without retention deadlines. It uses explicit sealing/drain calls and does not qualify cron-only capacity or historical restore semantics.

New actual-Neon regressions cover old-offset forks during collection, inherited history after parent deletion, immediate eligibility after retirement, and stopped schema-0006 upgrade with exact retained bytes. Full current-source required CI remains the merge gate. The unresolved independent Realtime diagnostic from the earlier 45-minute profile remains documented separately.

The first schema-0007 CI attempt passed all 88 Neon cases, 21 transaction/process-loss cases, and 338/338 conformance cases, but failed the in-place restore while waiting for post-restore raw Realtime rows. A fresh US-east-1 recovery recheck passed on the same clean runtime revision, including independent raw payload observation, without changing assertions/timeouts. The first failure is preserved in [its report](qualification/2026-11-23-provider-first-attempt.json); the independent [recheck](qualification/2026-11-23-provider-inplace-recheck.json) does not erase that failure or establish its cause. The retry encountered the same pre-append startup timeout; [both failed attempts](qualification/2026-11-23-provider-second-attempt.json) remain recorded. The recovery fixture now gives Realtime a separate bounded 180-second startup readiness gate with fresh-client retries for initial snapshot timeouts only. Identity/authorization/SQL errors still fail immediately. The independent payload must still arrive within 30 seconds after the append, and public SSE retains its 45-second bound. This measures startup separately from delivery without adding application data-retention delays. Current-source CI remains required.
