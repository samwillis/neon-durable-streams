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

Physical object deletion is unavailable in the adapter contract and disabled in the library. Objects, manifests, and generation indexes survive stream deletion. This preserves snapshot replay plans, fork references, and recoverable database history while reference pins, publication/retirement barriers, and recovery retention are unfinished. Operators must size and monitor retained data. Listing unreferenced objects is not permission to delete them after a database restore.

A future reclamation migration must establish the pin and retirement barriers before enabling deletion. Explicit quiesced adoption and manifest reconciliation are described in the [operations contract](operations-v1.md), with a real point-in-time branch drill. Destructive GC and in-place restore cutover remain unqualified.

## Evidence

Tests run against real Neon Object Storage and Postgres, including hot/sealed replay from a fresh handler, exact large JSON numbers and UTF-8 indexes, producer retry after sealing, an upload whose response is lost, worker claim replacement, corrupt download rejection, storage-identity mismatch, and SIGKILL immediately after publication. Unit/property checks include the independent binary fixture, generated binary frames, truncation, and wrong-generation objects.
