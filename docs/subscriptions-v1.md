# Durable subscription contract

Subscription state, memberships, wake snapshots, leases, callback-token hashes, encrypted token material, retry deadlines, signing keys, and fan-out progress live in PostgreSQL. The library has no subscription timers. The host calls bounded `maintain()` batches and supplies a durable recovery schedule. Realtime is used for ordinary stream delivery, including pull-wake stream appends.

## Registration and dispatch

Registration and explicit membership changes take an exclusive tenant registry advisory transaction lock. Dispatch takes its shared counterpart. Registration eagerly captures existing matching streams at their committed tails; explicit nonexistent paths start at the zero offset. It then commits the configuration and membership together.

An append commits its per-stream versioned outbox record independently of that registry barrier. If its data is visible during backfill, it is included in the initial cursor. If it commits later, dispatch cannot overtake the uncommitted backfill and will observe the older cursor. Newly discovered paths are linked before their first observed change. Per-stream pending versions are processed in order, with durable per-record subscriber pagination. Allocation order and a global sequence high-water mark are never used as commit order.

Stream mutations take `FOR NO KEY UPDATE` after the namespace lock. Stream identity columns are immutable. This serializes writers while allowing membership/snapshot foreign-key checks to proceed; a slow writer on one stream does not block wake snapshots for another stream. A real PostgreSQL trigger/lock barrier test exercises append commit between backfill and registration commit. Another test commits a previously hidden earlier change after a later change has already been delivered and acknowledged.

Default limits are 1000 linked streams per subscription and 10000 subscriptions per tenant. Initial backfill beyond capacity is rejected transactionally. Later glob discovery at capacity retains its earliest cursor in a durable pending-link queue and reports failed status. Raising capacity or freeing a slot allows recovery without dropping earlier appends. Explicit and glob membership flags are independent. Adding an already-linked path is idempotent; removing its explicit flag preserves a matching glob link.

## Wake and lease state

Each wake has a unique UUID, increasing subscription generation, immutable webhook snapshot, and a scope-specific random token. A pull-wake event and its generation are appended to the ordinary application-created JSON wake stream in the same database transaction. A claim captures current member tails, issues a claim token, and starts a lease. Competing workers receive `ALREADY_CLAIMED` with holder/generation metadata.

Callbacks, acknowledgements, and releases use their scoped token instead of application/service credentials. Validation locks the subscription, checks database-time lease validity after acquiring that lock, and compares token, generation, wake ID, route, and delivery type. Tokens cannot authorize ordinary stream reads. Deleted/replaced subscriptions and stale/expired generations reject callbacks with `FENCED`.

All requested acknowledgements are validated before a batch changes any cursor. Offsets must be nonregressing, within the correct stream generation's tail, and actual returned frame boundaries, including inherited sealed boundaries. Manual acknowledgements may advance beyond the original wake snapshot when the worker has processed newer appends. A heartbeat extends the lease. Completion and release clear the holder and queue another wake if pending data remains.

Synchronous webhook `{ done: true }` acknowledges only the stored wake snapshot. A newer append remains pending and causes another generation. A callback arriving before an HTTP delivery finishes cannot let that delivery resurrect a completed wake. Expiring leases fence old tokens even if a recovery worker has not run yet.

## Keys and webhook transport

Call `initializeSubscriptions` explicitly after migrations, using a persistent operator-owned `SubscriptionKeyring`. Signing keys are generated as Ed25519, encrypted using AES-256-GCM, and stored with public JWKs. Callback/claim token material needed for retry is also encrypted; authentication compares its SHA-256 digest. Envelopes bind the installation and purpose with authenticated associated data. Wrapping keys are cloned and validated at construction and must be retained for supported database recovery history.

`initializeSubscriptions({ rotateSigningKey: true, ... })` explicitly rotates the active signing key. Old public JWKs remain published so a receiver can verify an earlier in-flight delivery. Key IDs derive from public key material. Deploy a keyring containing old and new wrapping keys to all workers before changing the active wrapping/signing key; remove old wrapping keys only after active work and the supported recovery history no longer require them.

The signature covers exact `<timestamp>.<raw-body>` bytes and advertises its key ID. Receivers must verify the raw body, select the advertised JWK, enforce a replay window, and bind the delivery to the expected server, tenant, and subscription. Signed payloads include `tenant_id` in addition to protocol fields. The `subscribe` authorization hook receives normalized selectors and delivery destinations so applications can enforce tenant-specific integration/URL ownership. Pull-wake registration additionally requires append authorization for the wake channel. A subscription cannot select its own pull-wake channel.

Production destinations require HTTPS and globally routable addresses. Validation runs during registration and every delivery attempt. Resolved addresses are checked and pinned through connection setup while retaining TLS hostname verification. Redirects are not followed. The explicit development exception permits only localhost/127.0.0.x. DNS resolution, HTTP duration, response size, member snapshots, and maintenance batches have finite bounds.

Webhook failures persist exponential retry state (one to sixty seconds with jitter). A fresh handler honors the stored deadline. Lease expiration can create a new fenced generation but does not bypass an outstanding retry deadline. Failed wake publication, including an unavailable JSON wake stream, also retains a retry schedule. Delivery is at least once; receivers and workers must make application effects idempotent.

## Runtime and remaining release work

`GET __ds/jwks.json` intentionally exposes only public keys. Other subscription control operations use the consuming application's authentication/authorization policy. Callbacks/acks/releases accept only their scoped wake token. `authorize` distinguishes `subscribe` and `claim` from ordinary stream operations and includes normalized subscription configuration where applicable.

A cron-only host has its configured scheduling latency. Low-latency opportunistic work can supplement the durable schedule; correctness must survive all such workers disappearing. Account-wide admission, restore/adoption fencing, retained-history reclamation, sustained-load measurements, and remaining operational gates are tracked in the implementation plan.

The complete pinned 338-case protocol suite has passed unfiltered with subscriptions enabled and real Neon data services. Additional real-Neon tests cover registration and commit-order races, snapshot acknowledgements, persisted retry across handler replacement, heartbeat/expiry/fencing, atomic invalid-ack batches, quota recovery, Realtime pull-wake delivery, key rotation, inherited sealed acknowledgement boundaries, membership precedence, and destination authorization.

Single-transaction fan-out can coalesce up to 128 already committed changes for one stream. Its cutoff is captured before member updates while holding the shared registry lock. Later appends remain pending. A multi-transaction fan-out retains its original per-record cursor and cannot consume newer changes when it completes; newly registered lower UUIDs may require those changes. Fan-out batches are limited to 16 subscriptions to keep each transaction within its timeout even when the coordinator is distant from Postgres.

Completed outbox records are deleted in that same transaction after member/pending-wake updates. Registration's exclusive registry barrier establishes its own committed-tail baseline; it cannot require an already-consumed record. Per-stream versions remain on the stream row and never reset when outbox records disappear. Partial fan-out and later committed changes stay durable until their own handoff completes. This bounds newly completed outbox history without treating a global cursor or allocation order as commit order. The current baseline has no processed-history flag: retained outbox rows are pending work. Collection removes unreachable metadata and objects under the [reclamation contract](reclamation-v1.md).
