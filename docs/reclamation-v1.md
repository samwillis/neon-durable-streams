# Reclamation contract — implementation draft

The current release candidate retains objects and frame indexes. It has no destructive storage operation. This draft defines the next storage boundary; it does not authorize operators to delete objects from an existing installation.

The collector must reclaim only unreachable stream generations after an explicit recovery horizon. A manifest's absence from a current read is insufficient: forks, materialized replay plans, paused readers, stale seal workers, retained backups, and restored databases can all explain apparent non-use.

## Compatibility boundary

Reclamation needs a new checksummed migration and a stopped-ingress upgrade from schema 0005. Applying it must change the installation serving epoch atomically so an already-ready pre-collector runtime cannot continue using its cached migration check. New pre-collector instances reject the new migration history. Stop all old runtimes and workers before this upgrade; an epoch alone cannot make a database rollback safe.

Current collector-aware binaries may explicitly support schema 0005 with collection disabled, preserving the qualified same-schema baseline/current overlap. That compatibility must be exercised on an actual five-migration installation. No binary without replay pins may participate in an installation where collection can run. A default migration to the new schema and a deliberately bounded old-schema compatibility fixture are separate procedures.

Reclamation policy is disabled initially. Enable or change it only during explicit quiesced administration, with an expected installation/epoch and an idempotent operation identity. The policy must declare the complete supported database recovery window and an additional safety margin. Operators must include retained external backups, not just Neon PITR, in this window. An installation promising indefinite historical restores must retain its objects indefinitely.

## Readers and acquisition

A replay page captures local bytes or immutable chunk manifests in one database snapshot. Local bytes already copied into the plan need no object pin. For sealed references, the same transaction must acquire shared chunk locks in a deterministic order, reject retired chunks, and persist expiring random replay pins before committing. A collector locks those same chunks exclusively before retirement. Acquiring an object pin after the snapshot closes is unsafe.

Pins cover partial-fork preparation as well as ordinary GET and live catch-up. A partial fork materializes its prefix outside SQL, then rechecks the source generation and fork eligibility under the existing namespace/stream locks before publishing its edge. Live payload forwarding does not need object pins; durable gap/reconnect replay does.

Generation replay currently uses read-only REPEATABLE READ. A pin-writing replay transaction must preserve that snapshot isolation while allowing the pin insert; changing it to ordinary READ COMMITTED would weaken the current snapshot contract. Initial reads retain their stream/path locks through pin acquisition. No database transaction stays open across object I/O.

Every materialization has a finite monotonic deadline that precedes its database-clock pin expiry. Check the deadline before starting another object request, after downloads, and before returning bytes. Always release pins after work settles. Expired work fails closed and cannot emit a late page. Consumer adapters and pools still need finite I/O/acquisition deadlines; abandoning a promise must not create unbounded detached work. Cancellation retains the existing admission/drain contract.

## Retirement and deletion

A candidate generation is deleted/expired, detached from its namespace, has zero active fork references, and has completed ancestor-edge cleanup. Fork acquisition and logical retirement continue to serialize on the existing stream locks. A retained parent with live descendants is never a candidate.

Under the installation guard and stream/chunk locks, retire a bounded chunk batch. Fence any FROZEN upload claim by changing its token/epoch so a stale worker cannot publish after retirement. New seal claims/publications skip retired chunks. A worker may leave an immutable orphan after fencing; orphan handling must use recorded upload intent and the same retention protection, not a bucket listing alone. Deleted generations with unchunked HOT data need a bounded retirement path as well.

Persist each immutable object's exact key, digest, size, retirement time, and collection claim. Do not remove the only retry identity before an ambiguous deletion is resolved. Object keys are never reused. The retained chunk format remains readable; collection metadata belongs in Postgres.

An object becomes eligible only after its retirement time plus the declared recovery window and safety margin, and when it has no unexpired replay pin. Claim batches with bounded work and random expiring ownership. Object I/O stays outside SQL. A lost successful delete response is retried idempotently for the same exact key. Metadata finalization requires the current claim, serving epoch, and retired identity; a stale worker cannot finalize another worker's collection.

Recovery verification must distinguish live sealed manifests from retired, unreachable manifests. A supported restore point after retirement can still contain a retired manifest whose object has since been collected; requiring that object would make an otherwise valid restore impossible. A restore point before retirement needs the object and is protected by the recovery horizon. Missing reachable objects keep the installation quiesced. Recovery activation fences collection claims and gives restored retirement work a conservative new hold before resuming collection.

Generation/path, producer, outbox, subscription, and collection-tombstone metadata need separate bounded retention rules. Deleting chunk payloads does not by itself bound those tables. Processed outbox records may be removed only after proving that registration/bootstrap and fan-out checkpoints no longer depend on them. Persistent namespace coordination rows remain an explicit capacity dimension until their own ABA-safe reclamation exists.

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
