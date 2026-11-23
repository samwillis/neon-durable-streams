# Sealed storage v1

The consumer supplies an immutable-key `ChunkStorage` adapter and explicitly binds its identity during migration. The `/s3` adapter derives that identity from the endpoint, region, and private bucket, excluding credentials so key rotation does not change the binding. A mismatched branch/bucket fails closed. Database restores and branch adoption still require an explicit recovery workflow; this version does not silently rebind an installation.

## Rotation and publication

Hot frames keep their bytes and JSON message indexes in Postgres. Under the stream lock, rotation assigns a contiguous bounded prefix to a new FROZEN chunk. It records the frame count, payload length, offsets, and UUID before any upload. Appends continue into the next hot chunk. Defaults rotate at 4 MiB or 128 frames; the triggering append can add up to one maximum frame beyond the byte target. Maintenance also freezes nonempty idle chunks.

`server.maintain({ maxChunks })` obtains a persisted expiring claim with a random token and increasing epoch. It reads immutable frozen frames, encodes them, and uploads outside a database transaction. The object key contains the installation UUID, stream generation, chunk UUID, and SHA-256 digest. Duplicate uploads use the same key and bytes. After downloading and verifying the uploaded object, a short transaction publishes the manifest and removes those exact local payloads together. Publication requires the current token, epoch, and unexpired lease. A stale worker cannot publish or release a replacement worker's claim.

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

A read-only REPEATABLE READ transaction captures a bounded frame plan. PostgreSQL computes the page byte budget from retained frame lengths before returning payloads. A plan contains either local frame bytes or references to sealed immutable manifests. The SQL transaction ends before object downloads. Sealed frames retain their offset/length index rows; bulky payload and message-index data move to objects. Retained per-frame metadata is a database capacity input.

Readers verify downloaded size, digest, identities, and frame boundaries before returning bytes. Response pages copy only selected frames, so a slow reader does not pin a whole chunk buffer. The S3 adapter defaults to four concurrent network operations, at most 128 queued operations with a five-second queue deadline, a 30-second operation deadline, and three SDK attempts inside that deadline. Stream read pages retain their existing size/frame caps. Memory/egress under mixed hot/sealed reads still needs sustained workload measurements before release.

## Retention

Physical object deletion is disabled by default. Schema 0007 collects unreachable objects and metadata after an explicit enablement setting, with historical recovery owned by Neon or matching backups. It protects replay plans with pins, records upload intents, and synchronizes publication and retirement with references. The S3 adapter supplies idempotent exact-key deletion and bounded listing; custom adapters must provide those capabilities before collection can be enabled. Operators must size and monitor retained data. Listing unreferenced objects alone is insufficient evidence to delete them after a database restore.

Read the [collection contract](reclamation-v1.md) before enabling it during stopped administration. Explicit quiesced adoption and manifest reconciliation are described in the [operations contract](operations-v1.md). The [qualification index](qualification/README.md) records physical-collection child and in-place restore evidence, exact source revisions, and the unresolved independent Realtime diagnostic.

## Evidence

Tests run against real Neon Object Storage and Postgres, including hot/sealed replay from a fresh handler, exact large JSON numbers and UTF-8 indexes, producer retry after sealing, an upload whose response is lost, worker claim replacement, corrupt download rejection, storage-identity mismatch, and SIGKILL immediately after publication. Unit/property checks include the independent binary fixture, generated binary frames, truncation, and wrong-generation objects.

## Maintenance scheduling

`maintain` interleaves one bounded subscription pass with lifecycle/chunk work, so continuous outbox traffic cannot exclude delivery and sealing. `timeBudgetMs` stops new rounds after the deadline; the current round finishes and may overrun by its bounded SQL/object/webhook I/O. It is a soft budget, not cancellation of an ambiguous publication. Count limits remain in force. Set `freezeHot: false` on append hints: full chunks frozen by the write path can upload, while a small hot tail stays in Postgres. An explicit cron/manual pass uses the default `freezeHot: true` to flush idle tails. The example uses at most two coalesced hint passes, tracked by one Neon `waitUntil` owner, plus a durable minute-level cron with 64 chunk and subscription work units and a 40-second soft budget.

The replay default is 128 frames per page, still subject to the 2 MiB byte budget. This aligns small-record catch-up pages with default chunk frame counts and reduces repeated full-object reads. `limits.maxPageFrames` is configurable up to 1024; object checksums and exact boundaries are still validated before bytes are returned.
