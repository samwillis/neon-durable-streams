# Sealed storage

The [storage layout](compact-storage-v2.md), [offset contract](offset-design.md) and [block encoding](block-storage-format.md) describe the current storage version 3 baseline.

The consumer supplies an immutable-key `ChunkStorage` adapter and explicitly binds its identity during migration. The `/s3` adapter derives that identity from the endpoint, region, and private bucket, excluding credentials so key rotation does not change the binding. A mismatched branch/bucket fails closed. Database restores and branch adoption still require an explicit recovery workflow; this version does not silently rebind an installation.

## Rotation and publication

Hot frames keep their bytes and JSON message indexes in Postgres. Under the stream lock, rotation records a contiguous bounded prefix as a new FROZEN manifest and advances the monotonic hot floor. It does not update individual append rows. It records the frame count, payload length, offsets, and UUID before any upload. Appends continue into the next hot chunk. Defaults rotate at 4 MiB or 131,072 frames; the triggering append can add up to one maximum frame beyond the byte target. Normal maintenance seals tails after one hour from their first append. Explicit `forceHot: true` drains earlier.

`server.maintain({ maxChunks })` obtains a persisted expiring claim with a random token and increasing epoch. It reads immutable frozen frames, encodes them, and uploads outside a database transaction. The object key contains the installation UUID, stream generation, chunk UUID, and SHA-256 digest. Duplicate uploads use the same key and bytes. After downloading and verifying the uploaded object, a short transaction publishes the manifest with its verified block directory and deletes those exact append rows together. Publication requires the current token, epoch, and unexpired lease. A stale worker cannot publish or release a replacement worker's claim.

Failures before publication leave the frozen local bytes readable. Failure after the commit leaves a sealed manifest and its verified object. A lost publication response is never automatically replayed as a database transaction. Another explicit maintenance call can recover through durable state. Claim expiration allows recovery after every worker disappears. Maintenance errors are returned to the host; they must be observed and retried by its scheduler. A bounded batch does not promise that an arbitrary backlog is drained.

The host owns scheduling and authentication of maintenance calls. No correctness step depends on an untracked task after an HTTP response. A cron Function or explicit worker calls `maintain`; opportunistic calls can supplement that durable recovery trigger. Scheduling cadence determines idle-chunk sealing latency. Subscription recovery shares this explicit maintenance API.

## Encoding and replay

The [block format](block-storage-format.md) specifies the single shipping codec and its independent fixture. Objects contain complete append frames grouped into independently hashed blocks. Postgres stores one packed block directory per chunk; frame lengths and JSON element boundaries live inside blocks. Dense ordinal/history intervals validate sealed checkpoints without per-append SQL metadata.

Replay planning captures authorized generation, capped ancestry and staging/object ownership in a coherent snapshot, acquiring temporary pins before it ends. It downloads selected verified ranges or an explicit whole object outside SQL. Adjacent missing blocks coalesce, concurrent readers share immutable fetches, and cached blocks own their allocations. Corruption in a later block fails the complete page before headers.

The S3 adapter requires exact HTTP 206 and Content-Range for ranges. It defaults to four concurrent operations, 128 queued requests with a five-second queue deadline, a 30-second operation deadline and three SDK attempts inside that deadline. Range fragments are consumed under backpressure. Per-server replay, response and cache memory have separate bounds described in the [storage layout](compact-storage-v2.md).

## Retention

Physical object deletion is disabled by default. The current implementation collects unreachable objects and metadata after an explicit enablement setting, with historical recovery owned by Neon or matching backups. It protects replay plans with pins, records upload intents, and synchronizes publication and retirement with references. The S3 adapter supplies idempotent exact-key deletion and bounded listing; custom adapters must provide those capabilities before collection can be enabled. Operators must size and monitor retained data. Listing unreferenced objects alone is insufficient evidence to delete them after a database restore.

Read the [collection contract](reclamation-v1.md) before enabling it during stopped administration. Explicit quiesced adoption and manifest reconciliation are described in the [operations contract](operations-v1.md). The [qualification index](qualification/README.md) records physical-collection child and in-place restore evidence, exact source revisions, and the unresolved independent Realtime diagnostic.

## Evidence

Tests run against real Neon Object Storage and Postgres, including hot/sealed replay from a fresh handler, exact large JSON numbers and UTF-8 indexes, producer retry after sealing, an upload whose response is lost, worker claim replacement, corrupt download rejection, storage-identity mismatch, and SIGKILL immediately after publication. Unit/property checks include the independent binary fixture, generated binary frames, truncation, and wrong-generation objects.

## Maintenance scheduling

`maintain` interleaves one bounded subscription pass with lifecycle/chunk work, so continuous outbox traffic cannot exclude delivery and sealing. `timeBudgetMs` stops new rounds after the deadline; the current round finishes its bounded SQL/object/webhook I/O. Count limits remain in force. Hints and cron share size/count/age policy. Explicit administrative drains use `forceHot: true`; ordinary cron does not flush every small tail. The example keeps at most two coalesced hint passes under one Neon `waitUntil` owner, plus a durable minute-level cron with 64 chunk/subscription work units and a 40-second soft budget.

Replay defaults to 8192 frames and 2 MiB per page. The frame-work ceiling is configurable up to 131072. Partial seeks use verified HTTP ranges; warm cached blocks require no object request. Ordinary bodies stream views over one owned verified page, and SSE emits bounded data fragments before their control checkpoint.
