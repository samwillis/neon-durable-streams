# Committed Realtime payload delivery

Neon Realtime carries committed append bytes to the consumer's Function after authentication, authorization, UUID resolution, and initial durable replay. Postgres and sealed Object Storage remain the source for catch-up and recovery. Realtime delivery is not part of the append commit or acknowledgment.

The native adapter uses this raw subscription, with `materialize: false`:

```sql
SELECT stream_id, start_offset, end_offset, payload, message_ends
FROM <schema>.appends
WHERE stream_id = $1::uuid AND end_offset > $2
```

The UUID is authorized before subscribing. The query has no ordering, join, limit, or aggregation. A separate small tenant/UUID head view supplies the committed tail, change version, closure, and deletion. That existing head query also has no ORDER BY. A shared installation view binds Realtime to the installation UUID, serving epoch, active state, environment, and object-store identity pinned by the Postgres runtime. A cloned database can contain the same stream UUID; UUID matching alone cannot establish that binding.

## Contiguous delivery and recovery

The Function indexes unordered rows by durable start offset, validates offset arithmetic and JSON message indexes, discards acknowledged duplicates, and emits only a contiguous prefix through the known committed head. Conflicting, overlapping, foreign, or malformed rows invalidate the window. The head watermark also pairs a final append with its atomic close. EOF requires all bytes through the final tail, including pages supplied by durable replay.

Subscribe readiness always triggers an authoritative replay recheck. This covers appends between catch-up and subscription, and snapshots delivered before the raw callback attaches. Replacement snapshots, disconnects, identity changes, missing payloads, removed rows, buffer overflow, or gaps trigger durable replay before a fresh window is trusted. SDK move events do not change durable ordering. Fork history comes from replay; the child live view contains only the child's new appends.

Lifecycle and installation validation use a coalesced SQL check per watched generation, by default every second during payload delivery. The interval is configurable from 100 ms to 60 seconds. Lost head notifications also have a coalesced five-second SQL fallback. SQL validation and replay errors fail the live request closed. Database-clock expiry deadlines are mapped conservatively to monotonic elapsed time; Realtime activity never renews TTL. Authentication expiry, cancellation, shutdown, and bounded SSE rotation still end the request.

Recovery activation refuses unexpired live-reader leases from earlier serving epochs. A fenced reader can release its own random lease without validating the obsolete epoch. Operators must still stop ingress, workers, and old runtimes before an in-place rollback, which can restore an older epoch. Previously queued response bytes remain ordinary in-flight network data.

## Resource bounds

Each reader defaults to 2 MiB and 256 pending frames. Overflow unsubscribes and catches up from its durable cursor. Each raw view expires after ten seconds, then replays and rebases its fixed lower-offset predicate. These limits are configurable with `maxBufferedBytes`, `maxBufferedFrames`, and `payloadWindowMs`; the latter is bounded from 100 ms to 60 seconds. Raw subscriptions avoid the SDK retaining an ever-growing materialized append result. They do not cap the bytes in one transport message or establish a server-side throughput limit. SDK decoding and transient batches need additional memory beyond the retained buffer; the adapter rejects batches over 4,096 row changes after receipt. Capacity qualification must include this transient cost.

Backpressure permits one encoded replay page in the response queue. A paused reader does not retain an unbounded history of payload events; it catches up from Postgres/Object Storage after invalidation. The notifier and pool remain consumer-owned. Custom notifiers without `watchAppends` continue to use head notifications and durable replay.

## Qualification

Unit tests cover unordered/duplicate batches, gaps, conflicting frames, watermark gating, sealing updates, replacement snapshots, and stale reconciliation tokens. Real-Neon tests cover direct delivery without replay per append, subscription setup races, sealing, bounded-window rebasing, forks, JSON number preservation, final closure, deletion/recreation, authentication and stream expiry, serving-epoch fencing, and wrong-epoch Realtime rejection. The test profiles put both SQL fallback and periodic validation beyond the notification assertion deadline. Full upstream conformance and deployed Function qualification remain required for each implementation revision.

Matched query-shape probes supplied on 22 November ran four writers, four streams, eight SSE readers, 1 KiB appends, and separate direct SDK clients for head/materialized, head/raw, appends/materialized, and appends/raw. The 120-second probe observed 6,408 appends, no errors or missing rows, and p95 values of 91.88, 92.61, 101.18, and 101.76 ms respectively. This supports simple unordered payload delivery. The workload differs from the earlier 16-reader benchmark and does not explain its multi-second outliers or qualify capacity. The supplied JSON records bundle `930df346abcc730af30bcc3b3a847cb08c1b33c17ec56fd78b7119c54d6140f4`; the accompanying message named `6020396e…`. Preserve that provenance discrepancy when comparing results. See the [qualification evidence](qualification/README.md).
