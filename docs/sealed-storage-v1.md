# Sealed object format v1

Current SQL ownership and upgrade procedures are described in [compact Postgres storage](compact-storage-v2.md). Schema 0009 removes sealed append rows while retaining this immutable object format.

The consumer supplies an immutable-key `ChunkStorage` adapter and explicitly binds its identity during migration. The `/s3` adapter derives that identity from the endpoint, region, and private bucket, excluding credentials so key rotation does not change the binding. A mismatched branch/bucket fails closed. Database restores and branch adoption still require an explicit recovery workflow; this version does not silently rebind an installation.

## Rotation and publication

Hot frames keep their bytes and JSON message indexes in Postgres. Under the stream lock, rotation records a contiguous bounded prefix as a new FROZEN manifest and advances the monotonic hot floor. It does not update individual append rows. It records the frame count, payload length, offsets, and UUID before any upload. Appends continue into the next hot chunk. Defaults rotate at 4 MiB or 1,024 frames; the triggering append can add up to one maximum frame beyond the byte target. Normal maintenance seals tails after one hour from their first append. Explicit `forceHot: true` drains earlier.

`server.maintain({ maxChunks })` obtains a persisted expiring claim with a random token and increasing epoch. It reads immutable frozen frames, encodes them, and uploads outside a database transaction. The object key contains the installation UUID, stream generation, chunk UUID, and SHA-256 digest. Duplicate uploads use the same key and bytes. After downloading and verifying the uploaded object, a short transaction publishes the manifest with its boundary directory and deletes those exact append rows together. Publication requires the current token, epoch, and unexpired lease. A stale worker cannot publish or release a replacement worker's claim.

Failures before publication leave the frozen local bytes readable. Failure after the commit leaves a sealed manifest and its verified object. A lost publication response is never automatically replayed as a database transaction. Another explicit maintenance call can recover through durable state. Claim expiration allows recovery after every worker disappears. Maintenance errors are returned to the host; they must be observed and retried by its scheduler. A bounded batch does not promise that an arbitrary backlog is drained.

The host owns scheduling and authentication of maintenance calls. No correctness step depends on an untracked task after an HTTP response. A cron Function or explicit worker calls `maintain`; opportunistic calls can supplement that durable recovery trigger. Scheduling cadence determines idle-chunk sealing latency. Subscription recovery will extend this same explicit maintenance API.

## Binary format

The independently specified fixture is `test/fixtures/chunks/v1.hex`. All integer fields are unsigned big-endian.

| Field | Bytes |
| --- | ---: |
| ASCII magic `NDSTRM01` | 8 |
| Installation, stream, chunk UUIDs, raw bytes | 16 each |
| Frame count | 4 |
| Each frame's start and end offset, ASCII | 33 each |
| Frame payload length | 4 |
| JSON message count, or `0xffffffff` for binary/text | 4 |
| JSON message byte-end indexes, including trailing commas | 4 each |
| Exact frame payload | payload length |

Chunks have 1–1024 contiguous frames and an absolute encoded limit of 128 MiB. Decode verifies the complete checksum, identities, magic/version, frame lengths, offset arithmetic/continuity, indexes, JSON validity, and exact envelope exhaustion. It does not round-trip numeric values through JavaScript serialization.

## Replay and resource bounds

A snapshot transaction captures a bounded local/chunk plan and writes temporary replay pins when sealed objects are selected. PostgreSQL computes the page byte budget from retained frame lengths before returning payloads. A plan contains either local frame bytes or references to sealed immutable manifests. The SQL transaction ends before object downloads. Sealed manifests retain bounded relative end-offset arrays; exact payload and JSON message-index data live in objects. There is no retained per-frame SQL row or B-tree entry. Directory bytes still scale with append count.

Readers verify downloaded size, digest, identities, and frame boundaries before caching immutable objects and returning copied response frames. A per-server bounded verified cache coalesces concurrent downloads; hits still require the normal pins and fences. Response pages copy only selected frames, so a slow reader does not pin a whole chunk buffer. The S3 adapter defaults to four concurrent network operations, at most 128 queued operations with a five-second queue deadline, a 30-second operation deadline, and three SDK attempts inside that deadline. Stream read pages retain their existing size/frame caps. Memory/egress under mixed hot/sealed reads still needs sustained workload measurements before release.

## Retention

Physical object deletion is disabled by default. Schema 0007 collects unreachable objects and metadata after an explicit enablement setting, with historical recovery owned by Neon or matching backups. It protects replay plans with pins, records upload intents, and synchronizes publication and retirement with references. The S3 adapter supplies idempotent exact-key deletion and bounded listing; custom adapters must provide those capabilities before collection can be enabled. Operators must size and monitor retained data. Listing unreferenced objects alone is insufficient evidence to delete them after a database restore.

Read the [collection contract](reclamation-v1.md) before enabling it during stopped administration. Explicit quiesced adoption and manifest reconciliation are described in the [operations contract](operations-v1.md). The [qualification index](qualification/README.md) records physical-collection child and in-place restore evidence, exact source revisions, and the unresolved independent Realtime diagnostic.

## Evidence

Tests run against real Neon Object Storage and Postgres, including hot/sealed replay from a fresh handler, exact large JSON numbers and UTF-8 indexes, producer retry after sealing, an upload whose response is lost, worker claim replacement, corrupt download rejection, storage-identity mismatch, and SIGKILL immediately after publication. Unit/property checks include the independent binary fixture, generated binary frames, truncation, and wrong-generation objects.

## Maintenance scheduling

`maintain` interleaves one bounded subscription pass with lifecycle/chunk work, so continuous outbox traffic cannot exclude delivery and sealing. `timeBudgetMs` stops new rounds after the deadline; the current round finishes its bounded SQL/object/webhook I/O. Count limits remain in force. Hints and cron share size/count/age policy. Explicit administrative drains use `forceHot: true`; ordinary cron does not flush every small tail. The example keeps at most two coalesced hint passes under one Neon `waitUntil` owner, plus a durable minute-level cron with 64 chunk/subscription work units and a 40-second soft budget.

Replay defaults to 128 frames per page and a 2 MiB byte budget. `limits.maxPageFrames` supports up to 1,024. Larger chunks can serve several response pages from one verified cache entry; eviction or another Function instance can require another full-object read. See the [compact storage contract](compact-storage-v2.md) for cache/resource bounds and the stopped upgrade.
