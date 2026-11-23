# Postgres and object storage layout

The four-file [baseline](migrations.md) installs storage version 3, [ordinal/history offsets](offset-design.md), and [verified block objects](block-storage-format.md). Development installations use a fresh schema and empty object namespace. The package contains one codec and one schema layout.

## Data ownership

Postgres retains payloads and JSON element indexes for hot and frozen appends. A frozen manifest owns an immutable `(stream_id,start_offset,end_offset]` staging range. Rotation advances the stream's hot floor without updating individual append rows. Uploads run outside SQL transactions. Publication requires the current worker claim, serving epoch and unexpired lease, writes the verified manifest and block directory, and deletes that exact staging range atomically.

After sealing, Postgres retains one chunk manifest and one upload/deletion ledger entry per object, plus one packed directory entry per block. It retains no append rows, append indexes, offset arrays or byte-length arrays for sealed history. Each chunk's ordinal interval and history identifier validate interior checkpoints. Actual frame lengths and JSON element indexes live inside verified blocks.

An ordinary append updates its stream head once. A due record is inserted on the first hot append; later appends leave it unchanged. Producer state retains the latest epoch/sequence per producer identity. Subscription change records are an outbox, consumed after fanout commits. Without a subscription coordinator, maintenance independently prunes changes for tenants with no active subscriptions, under the same registry lock used by registration. That housekeeping runs even when object collection is disabled.

## Defaults and bounds

```ts
storage: {
  objects,
  targetChunkBytes: 4 * 1024 * 1024,
  maxChunkFrames: 131072,
  maxChunkAgeMs: 3_600_000,
  blockBytes: 65536,
  cacheBytes: 32 * 1024 * 1024,
  readAhead: 0, // Set to 1 for at most one following object read.
}
```

Size, frame count, history transition and first-append age bound chunks. The triggering append can exceed the payload target by one maximum frame. The encoded hard ceiling is 128 MiB and each block decodes at most 8192 frames. Normal maintenance preserves younger small tails. `maintain({ forceHot: true })` explicitly drains earlier. The host must schedule and monitor maintenance; storage outages accumulate staging.

Replay defaults to 2 MiB and 8192 frames per page, with a maximum configurable frame-work limit of 131072. Objects, blocks and response pages have independent sizes. A partial seek requests only its selected blocks, coalescing adjacent missing blocks into one HTTP range. Dense reads may verify a whole object. Cache hits still perform authorization, lifecycle, serving-epoch and pin checks.

The verified cache accounts for owned bytes and decoded frame/index overhead: 32 MiB by default, at most 256 retained entries, 128 pending fetch groups, 4096 pending blocks and 128 MiB of pending encoded reads. `cacheBytes: 0` disables retention while preserving concurrent sharing. One reader's cancellation releases its interest; the last reader aborts shared I/O and awaits settlement before releasing its pin. Closing is terminal and prevents late cache refill.

A separate replay/body admission budget defaults to 256 MiB and 128 reservations. Planning reserves the configured page's working set; a completed page transfers to an actual retained-body reservation. Ordinary response bodies serve 64 KiB views under downstream demand and hold that reservation until consumption, cancellation, authentication expiry, shutdown or the response deadline (five minutes by default). These budgets cover separate allocations and must be sized together with the Node heap, Realtime buffers and sealer working sets.

## Replay correctness

Reader checkpoints belong to clients. Request cursors remain in Function memory; temporary SQL pins and admission leases do not store reader offsets. Durable subscription acknowledgements are the separately requested persistent cursors.

Ordinary reads without sliding TTL use one coherent snapshot without locking the stream head. The plan captures generation, lifecycle, capped fork ancestry and staged bytes or immutable manifests, then acquires replay pins before releasing the snapshot. Sliding TTL renewal uses the locked writer path. Object I/O holds no SQL transaction.

The Function expands block directories rather than generating a SQL row per sealed append. Complete block totals bound page planning; boundary blocks may require decoding to set the final exact frame checkpoint. Every selected block is verified before HTTP headers. Formatting makes one owned response page. SSE encodes bounded fragments and emits a control checkpoint after its complete data event. An interruption before control remains resumable from the preceding completed checkpoint.

ETags include generation, serving epoch, encoding, page bounds, content type and closure. Where metadata determines the page boundary, matching conditionals skip object reads and pins. Other conditionals verify the exact page before comparing. `offset=now` ignores conditionals and has no ETag.

Forks inherit their parent's tokens only through the anchor. A partial prefix becomes a child frame. Restoration retains surviving tokens and lazily mints a fresh write history, preventing abandoned checkpoints from validating rewritten content at the same ordinal. The authorized Realtime query remains unordered and payload-carrying; the Function orders, deduplicates, checks contiguity, and replays durable data on gaps or invalidation.

## Collection and qualification

Current reachability and checkpoint metadata have distinct lifetimes. Pending inherited acknowledgements can need a manifest after its object bytes become collectible. Collection retains ancestry metadata until dependent generations and pending work can be pruned safely. Replay pins, upload intents, worker fencing and exact-key deletion protect current reads. Provider or matching backups own historical database/object recovery. Collection is explicitly configured; see the [collection contract](reclamation-v1.md).

The [replay plan](replay-performance-plan.md) records implementation and qualification status. Historical measurements retain their source hashes; they do not certify later builds. The final gate includes the full pinned conformance suite, actual Neon regression, process-loss, deployed consumers, hints/cron, independently packed instances and both provider recovery drills.
