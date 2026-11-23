# Stream storage contract

The current fresh baseline is storage version 3, organized into [four logical migrations](migrations.md). Postgres owns stream generations, hot/frozen payloads and sealed block manifests. Object Storage owns immutable verified chunks. See [storage layout](compact-storage-v2.md), [offsets](offset-design.md), [subscriptions](subscriptions-v1.md), [forks](forks-v1.md) and [collection](reclamation-v1.md).

## Identity and transactions

Each dedicated SQL schema contains exactly one installation UUID and its migration history. Migration execution is explicit, transactionally serialized, and checks previously applied checksums. An existing installation's identity is never regenerated automatically. The runtime verifies the supported storage version/migration IDs before accessing streams; schema changes are not triggered by imports or requests.

Tenant/path namespace rows serialize creation and logical deletion. Every mutation locks the namespace row first, then its active stream UUID. Stream UUIDs identify generations and are never reused. Composite foreign keys bind the active pointer to the same tenant/path. Logical deletion preserves referenced generations. With collection enabled, bounded cleanup can remove unreachable generations and empty namespace rows; creation validates that its namespace claim still exists before publication. Collection is disabled by default.

Writers use READ COMMITTED, transaction-local synchronous commit and finite statement/lock/transaction timeouts. Failures roll back; failed or ambiguous commits discard the connection and are not retried automatically. Ordinary non-TTL reads capture a coherent snapshot without locking the stream head. Sliding TTL renewal uses the locked writer path. Generation replay uses REPEATABLE READ. Replay pins are acquired under the same snapshot before SQL ends. Object I/O holds no SQL transaction, enforces its deadline, validates/releases pins, and rechecks the serving epoch before returning bytes.

## Offsets and frames

Nonzero offsets contain a 16-digit append ordinal, `_`, and a 32-character hexadecimal history identifier. The protocol zero token is `0000000000000000_0000000000000000`. Each committed nonempty stored frame consumes one ordinal, independent of byte length. Fixed widths preserve lexical ordering; bigint prevents numeric precision loss. Forks retain inherited tokens through their anchor. A fresh serving epoch starts a new write history, preserving surviving prefix tokens while rejecting discarded history after a rewrite. Reads and acknowledgements validate both fields. See the [offset contract](offset-design.md). Request sentinels `-1` and `now` are resolved by reads and never stored.

A stored append frame is one nonempty request batch: start offset, end offset, bytes, and optional JSON message-end indexes. Producer sequence numbers apply to that request batch, independently of logical JSON message count. Read pages end on stored frame boundaries and return the last emitted end offset. Non-sentinel read offsets are validated against retained returned boundaries.

Default limits are 1 MiB received body, 1 MiB stored frame, 2 MiB replay page, and 8192 frames per page (configurable up to 131072). A normalized JSON frame may exceed its input length and hit the stored limit separately. Limits are validated at construction. The maximum stored frame must fit a replay page. These are finite initial defaults, not throughput or capacity certification.

## JSON representation

Validate complete UTF-8 JSON before storage. Preserve each value's source text, including number lexemes, rather than serializing JavaScript numeric values. Top-level arrays flatten exactly one level. Store comma-terminated UTF-8 value fragments and byte-end indexes including each comma. Whitespace around top-level values may be removed; whitespace inside a value is retained. Reads combine fragments inside a single array.

For example, `[9007199254740993,[1,2]]` becomes stored bytes `9007199254740993,[1,2],` and replays as `[9007199254740993,[1,2]]`. JSON numbers beyond JavaScript's precision remain intact on the wire. POST `[]` is rejected; PUT `[]` creates an empty stream. Binary streams never pass through text decoding.

## Producer and lifecycle semantics

The stream lock protects producer validation, append insertion, tail and sequence changes, optional closure, and the change outbox in one commit. Retrying an accepted producer tuple returns deduplicated success without another append. Producer state persists with its stream generation. Stream-Seq is scoped to the stream and compared bytewise; producer deduplication precedes its regression check. A closing tuple can be retried even with different body bytes.

Creation is serialized even for a previously absent path. A matching repeated PUT does not append its initial body again. Logical delete/expiry clears the active namespace pointer, retains the previous generation, and writes a durable lifecycle change. Recreating the URL creates a fresh UUID, producer namespace, and ETag identity.

Expiry uses the database clock under the path/stream locks. Successful GET on a TTL stream renews access time; successful nonduplicate appends renew it. HEAD does not renew. Absolute expiry never moves. A live request renews TTL once when it begins, as specified by the pinned protocol; notifications, heartbeats, and background replay do not extend it. Generation reads recheck expiry with the database clock. HEAD and absolute-expiry behavior are the same across read modes.

## Change outbox

Every successful creation, new append, closure, deletion, and expiry writes a bounded record keyed by stream UUID and stream-local change version. Records contain before/after offsets and lifecycle kind. Duplicate writes do not create extra records. The subscription schema provides durable subscription matching and bounded fan-out, serialized against registration by tenant registry locks. A processed record means its affected subscription state has been persisted; it does not mean that an external receiver has acknowledged delivery. The subscription contract defines registration barriers, per-stream ordering, fanout checkpoints and retained wake/retry state. Without a subscription coordinator, maintenance prunes changes for tenants without active subscriptions under the same registration lock; this is independent of object collection.

## Evidence and remaining qualification

Tests cover exact arithmetic above JavaScript's safe integer limit, generated JSON/binary cases, concurrent creates/retries, rolled-back work, tenant isolation, expiry, ETags/closure, path recreation, fresh-process reads, and SIGKILL before versus after commit. A separate consumer test installs the packed npm artifact and verifies declarations, runtime exports, and SQL assets.

Required CI runs against real Neon Postgres 18 and Realtime. Actual Realtime tests verify append-and-close ordering, EOF, tenant filtering, cancellation, and generation isolation with fallback polling set beyond the test deadline. The [qualification index](qualification/README.md) records exact tested source revisions. Final qualification of the current replay design is tracked in the [replay plan](replay-performance-plan.md); earlier successful results apply to their recorded builds.


## Live delivery

Consumers enable live modes by supplying a notifier in `live.notifier`; the Neon adapter is exported from `/neon`. The notifier and database pool are consumer-owned. Disposing a server releases its watches without closing a shared notifier or pool.

The Neon adapter relays committed append payloads through a simple unordered raw Realtime query after initial authorization and durable catch-up. The Function orders/deduplicates rows and checks contiguous offsets against the committed head. Bounded buffers and periodically replaced lower-offset windows constrain retained state. Gaps, sealing, snapshot replacement, and recovery fall back to durable replay. Shared tenant/UUID head watches and coalesced SQL fallback preserve lifecycle handling. An installation identity watch binds Realtime to the serving Postgres epoch/environment. A request resolves `now` once and pins its original UUID for its entire lifetime. See the [payload delivery contract](realtime-payload-v1.md) for the validation, expiry, and resource bounds.

Long-poll defaults to 25 seconds. SSE emits bounded data/control pairs, base64 for binary types, text with every line framed, 15-second heartbeat comments, and a 60-second rotation. EOF requires the final contiguous durable offset, whether bytes arrive through Realtime or replay. Backpressure permits one bounded encoded fragment in the response queue; its verified source page remains reserved until drained or canceled. Cancellation, authentication expiry, server shutdown, EOF, and rotation release registrations. Per-instance live admission defaults to 80 readers; the installation baseline adds a separately configured database admission limit, also defaulting to 80. The consuming deployment must budget aggregate Neon Function concurrency across installations and services. See the [operations contract](operations-v1.md). This default is a resource bound, not a certified capacity target.
