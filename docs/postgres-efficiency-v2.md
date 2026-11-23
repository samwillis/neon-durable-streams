# Postgres efficiency and simplification review

Design review, 23 November 2026. The primary layout, policy, cache, head-update, and canonical-runtime changes are now implemented; see the [current contract](compact-storage-v2.md). The implementation also replaces per-frame freeze assignment with immutable manifest ranges and a monotonic hot floor; it needs no extra HOT descriptor state. The explicitly deferred ledger/outbox/object-format redesigns remain follow-up decisions. The measurements below describe the original baseline. This document reviews runtime revision `59c3b85` and documentation baseline `1ba871b`. The detailed proposal below records the design decision; qualification evidence distinguishes the old and new implementations. The library remains unpublished.

## Recommendation

Keep Postgres as the hot log and chunk directory, but stop retaining a relational row for every sealed append. Publish a small exact boundary array in each sealed manifest and delete that chunk's staging rows in the same transaction. Keep payloads and JSON message indexes in immutable objects.

This removes most retained Postgres storage and index work while preserving transaction-local boundary validation for ordinary forks and subscription acknowledgements. Putting every boundary exclusively in Object Storage would save more bytes, but adds object reads and a new validation protocol to those transactions. Start with the native Postgres array.

Combine this with a better sealing policy and fewer updates on the append path. Preserve the existing object format initially. Treat a compact object format, compaction, outbox redesign, and intent-ledger reduction as separate decisions after measurement.

## What is retained today

`ChunkManager.maintain()` in [storage.ts](../src/storage.ts) uploads and verifies a chunk, publishes its manifest, then sets each append's `payload` and `message_ends` to NULL. Its stream UUID, two 33-byte offsets, payload length, chunk UUID, primary index entry, and chunk index entry remain. They are removed during collection of an unreachable generation, rather than when the chunk is sealed.

Those rows support more than replay. [store.ts](../src/store.ts) uses them for exact boundary validation, bounded read planning, ordinary fork anchors, and inherited ranges. [subscription-store.ts](../src/subscription-store.ts) validates all acknowledgement boundaries inside the locked subscription transaction before updating any cursor. Deleting the rows without replacing these uses would break correctness.

Other retained state has different scaling:

| State | Growth and purpose |
| --- | --- |
| Stream/path/fork records | Per generation/path; preserve lifecycle and inherited history. |
| Chunk manifests | Per chunk, including object identity and integrity metadata. |
| `chunk_objects` | Upload/delete/orphan ledger; currently also mirrors published objects permanently. |
| Producers | Latest epoch/sequence per producer identity, not a row per append. Arbitrary expiry could admit duplicate retries. |
| Subscription membership, wakes, pending links | Durable consumer progress and delivery state. |
| Outbox | Pending fanout work. Completed records are already consumed; worker backlog still grows if processing stops. |
| Replay pins/admission leases | Temporary coordination. Ordinary reader offsets remain client-owned. |

Sealed append rows are the main avoidable history cost. The permanent object-ledger mirror and very small chunks are secondary costs.

## Actual-Neon size experiment

The [reproducible script](experiments/measure-postgres-metadata.mjs) populated two fresh disposable schemas on the recorded Neon Postgres 18 test branch. Each scenario has 65,536 sealed 32-byte appends. It measures actual heap, B-tree, TOAST, and relation overhead using `pg_total_relation_size`. The alternative copies the manifest's current columns and indexes and adds an `integer[]` directory. The existing object ledger is included unchanged in every comparison.

| Frames per chunk | Chunks | Current metadata | Manifests + object-only boundaries | Manifests + embedded boundary arrays |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 512 | 22.28 MiB | 0.86 MiB | 1.13 MiB |
| 1,024 | 64 | 21.97 MiB | 0.25 MiB | 0.62 MiB |

The embedded layout is approximately **20–36 times smaller**, a **95–97% reduction** for these tables in this synthetic steady state. The arrays' raw integer contents total 256 KiB. The [sanitized report](experiments/2026-11-23-postgres-metadata.json) records exact bytes, source, script hash, region, and successful cleanup. No objects were uploaded and no existing application rows or statistics were changed.

This is a physical-layout comparison, not an implemented read path, throughput result, or production bill prediction. It excludes hot payloads, the other tables above, old MVCC versions, vacuum churn, WAL, and provider history. Rows were inserted directly into the sealed state; production currently performs additional updates. Synthetic object descriptors use valid fixed-length placeholders, not real object checksums. Random UUID/index packing and array compression affect sizes. Deleting historical rows later makes space reusable after vacuum; it does not immediately shrink existing relation files or provider history.

## Proposed ownership and representation

| Storage | Contents |
| --- | --- |
| `appends` | Exact payload and JSON indexes for HOT/FROZEN frames only; staging until successful publication. |
| `chunks` | One manifest per chunk: range, count, total payload length, existing identity/digest/object fields, and a sealed boundary directory. |
| Object | All exact frames and JSON message boundaries, preserving the current v1 decoder and immutable keys. |

For a chunk with start position `p0`, store the cumulative end deltas `D[i] = position(frame[i].end) - p0` in a one-dimensional `integer[]`. Starts follow from the previous end. Current offset arithmetic adds five framing units per stored frame, so:

```text
payload_length[i]       = D[i] - D[i-1] - 5       (D[0] = 0)
payload_prefix_length[k] = D[k] - 5*k
```

Keep the original full start/end offsets on the manifest. Preserve their epoch component; never renumber offsets or conflate that component with the installation serving epoch. Require a single offset epoch within a chunk, positive payload lengths, exact frame count, and exact agreement between the final delta, manifest end, and total payload bytes. Use BigInt/SQL bigint for absolute position arithmetic before narrowing a checked relative delta. PostgreSQL `integer` is signed: require every delta to fit **2^31−1**, comfortably above the current 128 MiB encoded limit plus framing units.

At the existing 1,024-frame format limit, an array contains at most 4 KiB of integer contents. It is immutable after publication and has **no GIN index, row, or B-tree entry per frame**. Locate the chunk through its existing `(stream_id, end_offset)` index, then search that bounded array with native operations. Do not unnest every historical directory to find a chunk.

This retains **O(appends) bytes**, approximately four raw bytes per append, plus O(chunks) rows/indexes. It does not promise constant Postgres bytes per chunk regardless of frame count. One billion frames would still require roughly 3.73 GiB of raw integer contents before overhead. If that residual cost becomes material, revisit object-only directories with measured workloads.

## Operations and concurrency

### Sealing and reads

1. Freeze a bounded immutable prefix under the existing stream lock. Claim/upload/read-back verification keeps its existing durable token and epoch protocol.
2. Derive the directory from the exact verified frames. In one short transaction, validate the claim and manifest, publish SEALED plus the directory, and delete exactly those staging rows. Compare the deletion count to the manifest count; any mismatch rolls back publication. Preserve retirement fencing.
3. Before publication, reads use staging bytes. After publication, reads use the manifest and object. A read-planning SQL statement must see both alternatives under **one statement snapshot**. Separate READ COMMITTED queries of the two locations can miss a concurrent handover.
4. Return one descriptor per selected sealed chunk, rather than repeating its manifest for every frame. Capture a bounded local page under the same snapshot. Plan only the chunks that can contribute to the response's frame/byte budget; include the containing chunk when seeking inside it.
5. Acquire existing replay pins before ending the planning transaction. Perform object I/O outside SQL, verify the object and its agreement with the directory, select complete frames, and revalidate the lease/installation as today. Do not return bytes from a cache beyond lifecycle, authorization, or recovery fences.

Bound chunk descriptors, local bytes, downloaded object size, and selected response bytes independently. A large first object can exceed the response budget even when only one small frame is returned. Frame lengths from the directory permit exact page budgeting without retaining frame rows. v1 JSON message indexes remain in the object; a directory entry represents a stored frame, not each JSON element.

### Boundary checks, forks, and acknowledgements

Resolve immutable lineage ranges as today. For an exact boundary, check ZERO/tail, staged frame ends, or the containing sealed manifest's start/end/delta directory. Combine the staged/sealed alternatives in one SQL statement. A fork can cap a range **inside** an ancestor chunk: filter frames against that cap, not merely manifests whose end is at or below it.

Ordinary fork creation can still validate and acquire its reference in its existing transaction. A partial binary/JSON fork still needs its current prepare/materialize/final-revalidate sequence because it copies a payload prefix. Unifying these two paths through object I/O would add work to ordinary forks.

Acknowledgements keep their current locked transaction: authenticate the wake, check the database-time lease/generation, validate every supplied boundary and monotonic/tail constraint, then update all cursors. Manual acks may name a boundary after the original wake snapshot; snapshot-only proofs cannot replace arbitrary exact boundary checks. One invalid item must leave the entire ack batch unchanged.

Retain boundary directories as metadata for as long as pending subscription operations need them, even if an object's bytes can already be collected. Test inherited references explicitly: a subscription referring to a deleted fork can still lead through ancestor metadata. A direct subscription-to-owner-UUID check does not establish that an ancestor directory is unused. Prefer conservative metadata retention through surviving child-generation records, then prune in dependency order, over a new per-frame reference system. Establish the required terminal/fenced acknowledgement behavior with a regression before changing collection predicates. This is a design obligation, not a reproduced claim of a current bug.

### Realtime

Keep the current raw query shape over `appends`, with the authorized stream UUID and `end_offset > cursor`, no JOIN or ORDER BY. Successful sealing will produce row removals instead of payload-null updates. The Function must continue ordering/deduplicating committed payloads, delivering only contiguous offsets, and recovering gaps/replacement snapshots through durable replay. Renew the bounded lower-offset window; do not retain an ever-growing view because rows will eventually be sealed.

Keep the small head and installation watches, close/delete/expiry checks, final-bytes-before-EOF rule, and authorization/serving-epoch fences. Deleting staging rows must never let a queued notification attach to a recreated stream generation. The matched fast Realtime query experiments and earlier unexplained outliers remain separate evidence; this storage change does not establish a Realtime latency diagnosis.

## Chunk sizing and write amplification

The current 4 MiB target often cannot take effect. A 128-frame chunk holds about 128 KiB for 1 KiB frames, or 4 KiB for 32-byte frames. In addition, the production cron calls maintenance every minute, and the default maintenance pass force-freezes any nonempty tail. A stream appending once per minute can produce one object and manifest for every append.

Use byte/count thresholds plus a **maximum age from the first append in the active chunk**. Running cron should seal due chunks, rather than force every tail. Keep an explicit force/drain operation for administrative use and deterministic tests. A one-hour maximum age is a candidate initial setting to qualify: one append/minute would amortize roughly 60 frames instead of one. It is a hot-residency/cost choice, not a durability deadline; acknowledged bytes are already in Postgres. Measure many sparse streams before choosing the release default. Age measured from the latest append could postpone sealing forever under continuous trickle traffic.

First qualify 1,024 frames using the existing object format, together with bounded replay caching. A 32-byte workload still produces only 32 KiB payload per full chunk, and sparse streams can remain much smaller. The format's frame limit and 74-byte per-frame header are explicit remaining inefficiencies. A later compact format can use implicit starts/relative ends and larger bounded frame counts. Keep the encoded-byte ceiling independent of payload targets and JSON index overhead.

On the append path:

- Consolidate ordinary append tail/version, access time, and hot counters into **one stream-head UPDATE**. `insertAppend()` currently updates counters and the caller updates the same head again.
- As a separate refinement, assign an active chunk UUID when inserting staging rows, and freeze the chunk descriptor instead of updating every frame's `chunk_id`. Create that descriptor lazily at the first append. Store its identity on the stream; keep its counters on the stream head to avoid updating two hot rows on every append. Rotate under the same lock. This requires an explicit HOT descriptor state and corresponding ownership/index changes; it is not a prerequisite for removing sealed rows.
- Keep the staging primary key usable for known stream/range scans and deletion. Once historical rows are gone, evaluate whether the hot/chunk secondary indexes remain worthwhile. Preserve efficient foreign-key checks and stream/chunk ownership; do not remove an index merely because staging is temporary.
- Replace `streams_hot(seal_after,id) WHERE hot_frames > 0` with due-work state that changes on chunk transitions. The current partial predicate references a counter changed by every append, inhibiting PostgreSQL HOT updates even when the due time stays fixed. Check other indexes and page space before claiming HOT-update gains. A once-per-chunk due record is preferable to a new indexed counter.

Staging is transient only while maintenance makes progress. Worker loss can accumulate frozen frames and pending outbox work. Qualify recovery and backlog limits; the new schema is not a guarantee of bounded storage during an unlimited outage.

## Simplifications to make, preserve, and defer

| Area | Recommendation |
| --- | --- |
| Sealed per-frame SQL history | Replace with the bounded native array; remove duplicated frame-to-manifest joins and historical row-pruning loops once migration finishes. |
| Runtime compatibility | For the first public release, support one canonical serving schema after an explicit stopped upgrade. Retire the separate schema-0005 runtime profile and legacy wake-token receipt branches when their upgrade path has been exercised. Preserve existing migration checksums and old-object readers. |
| Chunk policy | One normal size/count/age policy shared by hints and cron, plus explicit force/drain. Avoid growing a collection of overlapping flush flags. |
| Object format | Keep v1 through the SQL layout change. A new binary format, range reader, compactor, and SQL redesign need not ship together. |
| Replay | A bounded verified immutable cache and in-flight fetch coalescing; use an ordinal/index lookup instead of repeated `.frames.find()` scans. Keep copied response pages bounded. |
| Work scheduling | Keep host-owned scheduled `maintain()` and durable claims. Remove obsolete cleanup phases after the new representation makes them empty. Avoid introducing another generic job framework. |
| Recovery | Historical object recovery remains provider/backup-owned. Preserve current-data publication, reference, retirement, pin, and serving-epoch fencing. |
| Fork retention | Keep conservative whole-parent retention while descendants need it. Per-range object GC or multi-stream object packing introduces ownership complexity before a demonstrated need. |
| Realtime | Keep the simple append/head/installation watches. Another bus, Redis, or an application-managed replication log is unnecessary. |

The permanent `chunk_objects` mirror is a worthwhile **later** simplification. The manifest can own successfully published objects, with the ledger retaining unresolved uploads, deletion attempts, and orphans. Removing a successful upload intent would have to commit atomically with publication; retirement would transfer exact delete identity back into the ledger. Late uploads, ambiguous deletes, inventory races, and restored branches must still be handled. Do not remove the ledger outright. The measurement above includes it, so the primary improvement does not depend on this redesign.

Similarly, a single dirty ticket per stream could reduce outbox churn. It needs a captured version/fanout cutoff, registration barrier, resumable pagination, and final head/version recheck under the existing lock order. `INSERT ... ON CONFLICT DO NOTHING` followed by a worker's unconditional DELETE can lose a wake when a concurrent append commits late. Keep the proven outbox initially; completed history is already pruned.

Keep producer deduplication, tenant/generation identity, atomic ack batches, key/token security, replay admission, and explicit recovery operations. Their guarantees remain necessary after the SQL representation gets smaller. Do not introduce server-side ordinary reader checkpoints to compensate for deleting history rows.

## Why not put all exact boundaries in objects now?

That option would make historical SQL storage O(chunks) without the four-byte-per-frame component. Reads already need object bytes, so their boundary validation can move with them. Ordinary forks and atomic acknowledgement batches are the harder cases.

They would need a shared immutable validation plan: resolve authorized generations/ranges and pin the relevant objects, end the SQL transaction, fetch/verify bounded index data, then reacquire locks and recheck all mutable identity, lease, lifecycle, and epoch conditions before committing. Pin release, cancellation, retry deadlines, and inherited metadata retention become part of more API paths. Holding subscription/writer locks during S3 I/O is unacceptable. Dropping exact validation or changing protocol offsets is also unacceptable.

This may be justified for an installation with billions of tiny frames. For the first release, saving another 0.27–0.37 MiB per 65,536 frames in this comparison does not justify that added protocol. Use the compact SQL directory as the common primitive for reads, forks, and acknowledgements.

## Upgrade and implementation order

1. **Boundary directory and sealed-row deletion.** Add new migrations after 0007, a single chunk/staging planner, and exact directory validation. Keep current chunk assignment, object format, outbox, and ledger. This isolates the largest storage saving.
2. **Chunk age, read efficiency, and head updates.** Add due-age semantics, bounded verified cache/fetch coalescing, and one ordinary head update. Qualify increasing the frame count to 1,024. Make the initial chunk policy explicit in the public contract.
3. **Remove redundant transitions and compatibility branches.** Add active chunk identity if its measured WAL savings justify the extra state; remove the freeze-time row update and redundant indexes together. Establish the canonical serving schema and retire the obsolete runtime mode. Simplify bounded cleanup accordingly.
4. **Measure again before further redesign.** Decide whether ledger reduction, compact object v2, outbox tickets, compaction, or object-only boundaries address a measured remaining cost.

Do not edit migrations 0001–0007, overwrite existing immutable objects, or reinterpret existing offsets. An incompatible schema upgrade requires stopped ingress/workers/instances and fencing of old warm runtimes. There is no rolling-compatibility claim across this layout change and no automatic binary rollback after staging deletion.

Backfill existing sealed manifests in bounded, resumable per-chunk transactions. For each, acquire existing replay protection, verify its v1 object against retained frame offsets/lengths, publish its directory, and remove exactly its retained frame rows atomically. Missing/corrupt objects stop that chunk's conversion. Retired manifests may already lack an object: derive their metadata directory from complete retained rows under the stopped upgrade, or prune them only after proving no subscription/lineage consumer remains. Never infer missing boundaries from just the endpoints/count.

The current migration runner performs DDL in one 60-second transaction. Do not embed an unbounded historical object-reading backfill in it. Use short schema preparation/finalization migrations and an explicit stopped resumable conversion command. Serving remains disabled until all required manifests pass validation and a final readiness check; migration-only tooling can read the old representation without preserving two permanent serving modes. Back up the matching database/objects before conversion. Keep v1 byte fixtures and stopped-upgrade fixtures even when retiring old runtime profiles.

## Acceptance gates

- Every sealed manifest has its exact bounded directory; no sealed append staging rows remain after conversion/maintenance. Retained row/index counts scale with chunks, producers, and subscriptions. Report the directory's remaining linear byte cost explicitly.
- Reproduce size measurements at 128 and 1,024 frames with variable payload sizes, larger histories, sparse streams, and actual seal/delete/vacuum churn. Measure WAL bytes per append/seal, head tuple updates, HOT eligibility, SQL calls, database egress, and recovery backlog on an otherwise isolated test workload. Preserve previous results for comparison.
- Exercise append/freeze/publication/read/GC races, failed verification, lost upload/publication responses, worker replacement, and process death before/after publication. A commit must expose staging or manifest data without a gap or duplicate.
- Exact seeks and full/partial forks across HOT/FROZEN/SEALED and inherited mid-chunk caps; binary and JSON lexemes/message indexes; old-offset producer retries; valid and invalid mixed ack batches, including acks beyond their wake snapshot and pending operations after fork deletion.
- Actual Neon Realtime removals, reordered/duplicate batches, initial snapshot races, replacement/reconnect, sealing before delivery, fixed-window renewal, slow readers, auth expiry, delete/recreate, restored epochs, and final contiguous bytes before EOF.
- Record warm and cold replay object requests/bytes, verification CPU, cache-hit rate, process memory, and first-byte/page latency. A 1,024-frame chunk must not be fetched once per 128-frame response page when it can safely be reused. Preserve bounded memory and cache-independent correctness after eviction/restart. Whole-object SHA-256 does not authenticate an isolated range; defer range GETs until there is an independently verifiable block/index design.
- Run the **full unfiltered 338-case pinned conformance suite on actual Neon**, with subscriptions/collection enabled, plus deployed Function/webhook, process-loss, migration, child/in-place restore, and cron-only/hints-only checks. Retain failures and the unresolved Realtime diagnostic separately from protocol correctness.

This review adds a measured design and implementation sequence. It does not close the remaining production capacity or provider recovery qualification gates.
