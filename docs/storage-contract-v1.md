# Initial hot-log storage contract

Implemented during M0/M1. This is an unreleased storage version; object reclamation remains unimplemented. Durable subscriptions and fan-out are covered by the [subscription contract](subscriptions-v1.md). Recursive and partial forks are covered in the [fork/lifecycle contract](forks-v1.md). Migration 0003 adds fork edges, logical cascade cleanup, and indexed expiration deadlines. Migration 0001 establishes the hot log and durable change outbox; migration 0002 adds the recoverable chunk manifest and storage binding. See [sealed storage v1](sealed-storage-v1.md).

## Identity and transactions

Each dedicated SQL schema contains exactly one installation UUID and its migration history. Migration execution is explicit, transactionally serialized, and checks previously applied checksums. An existing installation's identity is never regenerated automatically. The runtime verifies the supported storage version/migration IDs before accessing streams; schema changes are not triggered by imports or requests.

Tenant/path namespace rows persist across stream deletion. Every mutation locks the namespace row first, then its active stream UUID. Stream UUIDs identify generations and are never reused. Composite foreign keys bind the active pointer to the same tenant/path. Stream data and change records are retained after logical deletion; physical reclamation is disabled.

Writers use READ COMMITTED, transaction-local synchronous commit on, and finite statement/lock/transaction timeouts. Application failures roll back. Failed/ambiguous commits discard the connection and are not retried automatically. Replay captures metadata and bounded frame batches in an explicit read-only REPEATABLE READ transaction; the preceding TTL touch revalidates the stream identity.

## Offsets and frames

Offsets use two 16-digit decimal fields separated by `_`. The initial value is `0000000000000000_0000000000000000`. Arithmetic uses bigint; each stored append advances the second component by its stored byte length plus five. The first component is preserved. Overflow beyond 16 digits rejects the write instead of wrapping. Internal ordering is binary/C order. Request sentinels `-1` and `now` are resolved by the read path and are never stored as offsets.

A stored append frame is one nonempty request batch: start offset, end offset, bytes, and optional JSON message-end indexes. Producer sequence numbers apply to that request batch, independently of logical JSON message count. Read pages end on stored frame boundaries and return the last emitted end offset. Non-sentinel read offsets are validated against retained returned boundaries.

Default limits are 1 MiB received body, 1 MiB stored frame, 2 MiB replay page, and 16 frames fetched per page. A normalized JSON frame may exceed its input length and hit the stored limit separately. Limits are validated at construction. The maximum stored frame must fit a replay page. These are finite initial defaults, not throughput or capacity certification.

## JSON representation

Validate complete UTF-8 JSON before storage. Preserve each value's source text, including number lexemes, rather than serializing JavaScript numeric values. Top-level arrays flatten exactly one level. Store comma-terminated UTF-8 value fragments and byte-end indexes including each comma. Whitespace around top-level values may be removed; whitespace inside a value is retained. Reads combine fragments inside a single array.

For example, `[9007199254740993,[1,2]]` becomes stored bytes `9007199254740993,[1,2],` and replays as `[9007199254740993,[1,2]]`. JSON numbers beyond JavaScript's precision remain intact on the wire. POST `[]` is rejected; PUT `[]` creates an empty stream. Binary streams never pass through text decoding.

## Producer and lifecycle semantics

The stream lock protects producer validation, append insertion, tail and sequence changes, optional closure, and the change outbox in one commit. Retrying an accepted producer tuple returns deduplicated success without another append. Producer state persists with its stream generation. Stream-Seq is scoped to the stream and compared bytewise; producer deduplication precedes its regression check. A closing tuple can be retried even with different body bytes.

Creation is serialized even for a previously absent path. A matching repeated PUT does not append its initial body again. Logical delete/expiry clears the active namespace pointer, retains the previous generation, and writes a durable lifecycle change. Recreating the URL creates a fresh UUID, producer namespace, and ETag identity.

Expiry uses the database clock under the path/stream locks. Successful GET on a TTL stream renews access time; successful nonduplicate appends renew it. HEAD does not renew. Absolute expiry never moves. A live request renews TTL once when it begins, as specified by the pinned protocol; notifications, heartbeats, and background replay do not extend it. Generation reads recheck expiry with the database clock. HEAD and absolute-expiry behavior are the same across read modes.

## Change outbox

Every successful creation, new append, closure, deletion, and expiry writes a bounded record keyed by stream UUID and stream-local change version. Records contain before/after offsets and lifecycle kind. Duplicate writes do not create extra records. Migration 0004 adds durable subscription matching and bounded fan-out, serialized against registration by tenant registry locks. A processed record means its affected subscription state has been persisted; it does not mean that an external receiver has acknowledged delivery. The subscription contract defines registration barriers, per-stream ordering, fan-out checkpoints, and retained wake/retry state.

## Evidence and remaining qualification

Tests cover exact arithmetic above JavaScript's safe integer limit, generated JSON/binary cases, concurrent creates/retries, rolled-back work, tenant isolation, expiry, ETags/closure, path recreation, fresh-process reads, and SIGKILL before versus after commit. A separate consumer test installs the packed npm artifact and verifies declarations, runtime exports, and SQL assets.

Required CI runs against real Neon Postgres 18 and Realtime. Actual Realtime tests verify append-and-close ordering, EOF, tenant filtering, cancellation, and generation isolation with fallback polling set beyond the test deadline. Deployed Function ingress, Object Storage, and the full pinned protocol suite now pass against Neon. Format upgrades and recovery/GC still require the qualification work in the implementation plan.


## Live delivery

Consumers enable live modes by supplying a notifier in `live.notifier`; the Neon adapter is exported from `/neon`. The notifier and database pool are consumer-owned. Disposing a server releases its watches without closing a shared notifier or pool.

Realtime observes UUID-scoped, tenant-filtered stream heads. Registrations are shared between local readers. Register-then-recheck and a revision counter cover notifications racing a durable replay. Each watched generation also has a coalesced SQL fallback (default every five seconds), so lost notifications delay delivery rather than losing data. A request resolves `now` once and pins its original UUID for its entire lifetime.

Long-poll defaults to 25 seconds. SSE emits bounded data/control pairs, base64 for binary types, text with every line framed, 15-second heartbeat comments, and a 60-second rotation. EOF comes from replaying the final durable offset, never directly from a close notification. Backpressure permits one encoded page in the response queue. Cancellation, authentication expiry, server shutdown, EOF, and rotation release registrations. Per-instance live admission defaults to 80 readers; the consuming deployment must budget aggregate Neon Function concurrency across instances and services. This default is a resource bound, not a certified capacity target.
