# Compact Postgres storage

The four-file [database baseline](migrations.md) installs the canonical compact layout directly. Immutable objects retain binary format v1. The [design review](postgres-efficiency-v2.md) records the alternatives and original synthetic measurement.

## Data ownership

Postgres retains exact payloads and JSON message indexes only for HOT and FROZEN append rows. After upload and full read-back verification, one publication transaction writes the sealed manifest's exact boundary array and deletes that chunk's append rows. A failed claim, checksum, or frame-count check leaves the old representation intact. Installation, worker, and retirement fences remain in force.

A frozen manifest owns its immutable `(stream_id, start_offset, end_offset]` staging range. A monotonic `hot_start_offset` advances under the stream lock when a prefix freezes. Freezing checks contiguity/count/bytes and updates the descriptor/head without updating any append row. Staging needs only its `(stream_id,end_offset)` primary index and stream foreign key; the redundant append `chunk_id`, assignment foreign key, and hot/chunk secondary indexes are removed. Sealing and bounded frozen cleanup address exact stream/range predicates. This avoids introducing another HOT descriptor state or per-append active-chunk updates.

A sealed manifest has one `integer[]` of cumulative end positions relative to its original start. With `D[0]=0`, frame `i` has payload length `D[i]-D[i-1]-5`. Directories have at most 1,024 entries and signed 32-bit relative deltas. The schema validates dimensions, lower bound, strict increments, frame count, epoch, final offset, and total payload size. Absolute wire offsets are unchanged.

There is no relational row or B-tree entry per sealed append. Remaining SQL history consists of O(chunks) manifests and approximately four raw boundary bytes per append, plus array/TOAST/page overhead. JSON element boundaries are stored in objects, not these SQL arrays. Producer state remains one latest epoch/sequence per producer identity; completed outbox records are consumed after fanout intent commits.

Boundary validation for reads, ordinary forks, and atomic acknowledgement batches uses the staged/sealed alternatives under one statement snapshot. A containing ancestor chunk can extend beyond a fork cap; only its contributing frames enter the read plan. Acknowledgements continue validating every supplied boundary before updating any cursor, including valid post-wake-snapshot appends. They require no object download.

Replay plans return one descriptor per selected sealed chunk and bounded local frames. Object I/O starts after SQL ends and after pins have been acquired. Each object is checked against its full checksum, identity, and directory. A response copies selected frames and retains the existing frame/byte/deadline bounds. Sealing produces Realtime row removals; the existing unordered payload query, contiguous-offset checks, and durable gap recovery handle this handover.

## Chunk and cache policy

```ts
storage: {
  objects,
  targetChunkBytes: 4 * 1024 * 1024,
  maxChunkFrames: 1024,
  maxChunkAgeMs: 3_600_000,
  cacheBytes: 32 * 1024 * 1024,
}
```

These are the defaults. Count and payload size rotate on the write path. A persisted `hot_chunks` due record is created on the first append of a hot chunk; later appends leave it unchanged. Ordinary appends update the stream head once. The due queue replaces the stream index whose predicate referenced the frequently updated frame counter, allowing HOT updates when PostgreSQL's other conditions permit them.

Normal `maintain()` seals age-due tails and uploads frozen chunks. Its frequency does not force one tiny object per cron tick. An explicit `maintain({ forceHot: true })` drains nonempty tails and is appropriate for administration or deterministic tests. The old `freezeHot` option is rejected. The one-hour age controls hot residency, not durability: acknowledged hot payloads are already committed to Postgres. Work must still be scheduled and monitored; a sustained worker or Object Storage outage accumulates staging/backlog.

The verified-object cache is per server instance, with a default 32 MiB accounting budget, at most 256 retained entries, and at most 128 coalesced pending fetches. Accounting includes encoded bytes and decoded frame/index overhead. `cacheBytes: 0` disables retention while preserving concurrent fetch coalescing. Cache identity includes storage/installation/stream/chunk identity, checksum, and encoded length. Pins, authorization, lifecycle, expiry, and serving epochs are validated independently of hits. Closing the server releases retained entries and prevents late loads from refilling it.

The budget covers retained cache state, not all concurrent replay/network memory. The S3 adapter and live admission retain their separate concurrency/queue bounds. Decoding a cold v1 chunk requires the whole object even when one small frame is requested. Eviction, a cold Function, or routing to a different instance can require another download. Range GETs require a future independently verifiable block/index format; a whole-object checksum does not authenticate a standalone range.

At 32 bytes per append, a full 1,024-frame chunk still has only 32 KiB of payload and v1 still spends 74 bytes on each frame header. Very sparse streams can create smaller objects at their age deadline. These are remaining costs to size explicitly; this change does not introduce cross-stream packing or compaction.

## Installation

Run the complete [database baseline](migrations.md) before serving. Older unreleased development histories require a fresh schema; their preparation, backfill, and conversion tooling has been removed from the package. The migration ledger and checksums remain for future schema changes. Immutable object format v1 is unchanged.

The native Function example exposes explicit forced draining only through its authenticated manual maintenance route. Cron and hints use normal due maintenance. `DS_MAX_CHUNK_AGE_MS` defaults to one hour; the cron integration fixture overrides it to one second so scheduled age-based sealing can be tested within a bounded test duration. The separate hints-only fixture keeps the production age default.

## Collection and qualification

Current-data object reachability and historical boundary metadata have distinct lifetimes. A directory can remain necessary for a pending subscription acknowledgement after object bytes become collectible. Metadata collection therefore retains ancestor directories and local HOT/FROZEN frame metadata while child-generation metadata remains, then prunes in dependency order. Regressions cover inherited acks after fork deletion with both unsealed parent frames and physically collected sealed objects, before outbox invalidation fences the wake.

Whole-parent retention for live forks, replay pins, upload intents, exact-key deletion retries, and serving-epoch fencing remain. Historical database/object recovery remains provider/backup-owned, and collection remains disabled by default. See the [collection contract](reclamation-v1.md).

Actual-Neon qualification covers 1,024 variable-size frames, eight replay pages with one object fetch, cache-disabled cold replay, interior seeks/forks, atomic sealed ack batches, post-snapshot appends, first-append age, explicit drains, one ordinary stream-head update, transactional baseline failure/retry, rejection of old development histories, and v1 object continuity. The complete pinned 338-case protocol suite must pass with subscriptions and collection enabled. Deployed Function/webhook, hints/cron recovery, process-loss, and restore checks remain required. Exact revisions and reports belong in the [qualification index](qualification/README.md); historical qualification remains scoped to its recorded source revision.
