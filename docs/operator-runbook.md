# Consumer operations

The application owns admission, credentials, ingress protection, monitoring, and the Neon resources. Keep management/recovery operations on a trusted operator path with a separate direct Postgres pool. Library imports and public stream requests never provision infrastructure, migrate schemas, enable collection, or activate a restored installation.

## Supported and tested configuration

| Component | Current qualification |
| --- | --- |
| Node.js | Node 24; Linux ARM64 Neon Function runtime 24.22.1, Node 24.22.2 test coordinator. Package engines exclude other majors. |
| Postgres/client | Actual Neon Postgres 18; `pg` 8.23.1. The declared `pg` peer API range begins at 8.16.3; other versions in that range require consumer qualification. |
| Realtime | `@neon/realtime` 0.2.1, raw append windows, SDK-decoded bytea, bounded subscriptions and fallback. |
| Object Storage | Neon S3 endpoint with `@aws-sdk/client-s3` 3.1169.0; private bucket, immutable v1 chunk keys. |
| Functions | `@neon/functions` 0.11.0, Node 24, native HTTPS ingress and authenticated schedule triggers. |
| Protocol | Pinned upstream 0.3.7 / commit `461b40267aabd644558f9b19dbb9507dd5f691cf`; all 338 cases, subscriptions enabled. |
| Schema | Fresh schema 0006; explicit stopped upgrade from 0005. Schema-0005 compatibility has no physical collection. The separately packed rolling pair is qualified only on schema 0005. |
| Regions | Frankfurt local/deployed qualification; US-east-1 required CI. Other regions and combinations need their own deployment evidence. |

Sealing moves payload bytes to Object Storage. Format v1 retains per-append offset/frame indexes in Postgres while that history is reachable, including history retained by forks. Postgres metadata therefore grows with retained append count; this design does not offer constant database size for an indefinitely retained stream. Reclamation removes those indexes only after the generation becomes unreachable and its holds/references permit cleanup. Include that index cost, especially for tiny appends, in sizing.

The checked-in lockfile pins the complete dependency graph used by CI. Dependency updates rerun packed-consumer, actual Function, Neon, recovery, and full conformance gates. A peer range describes the accepted client API; it does not claim every historical driver/provider combination has passed the matrix. The package is unpublished and remains marked private.

Use the [qualification reports](qualification/README.md) for exact workload, source, deployment bundle, clocks, reader mappings, and test counts. The current long-running collection profile combines four closed-loop 1 KiB writers, four streams, 16 receiving SSE readers, one sealed 64 KiB source/fork lifecycle every ten seconds, cron recovery, and append hints. Default example pools have five connections per module. Routed health samples describe one observed instance/process, rather than total fleet memory or pool use. Measurements establish the stated profile; they do not establish an arrival-rate ceiling or an end-to-end availability SLA.

Application-specific request rates, per-tenant fairness, and storage budgets remain consumer policies. Enforce them using a trusted admission wrapper and the identity/authorization hooks, backed by shared state when several Function instances serve a tenant. The library supplies installation-wide live leases and finite per-request/adapter bounds; those do not implement a tenant billing or traffic policy. In particular, a prolonged Object Storage outage retains acknowledged payloads in Postgres. Set staging-backlog/disk alarms and throttle new appends before exhausting the database's provisioned budget. Durable storage backlogs are distinct from bounded Function memory and queues.

## First response by symptom

| Symptom | Action |
| --- | --- |
| Append response lost or connection breaks during commit | Preserve the original producer ID/epoch/sequence and exact payload. Retry that tuple. A completed transaction deduplicates it; a rolled-back transaction accepts it once. Without a producer identity, the application must resolve an unknown outcome before blindly repeating side effects. |
| Repeated SQL timeouts or `STORAGE_BUSY` | Inspect pool waiting/active counts, database locks/load, adapter queue bounds, and maintenance backlog. Reduce concurrent work or scale within the measured envelope. Keep finite connection, transaction, and object I/O deadlines; do not replace bounded queues with unbounded retries. |
| `429` live-reader admission | Respect `Retry-After`, close idle readers, and check installation-wide live leases. Cancellation releases slots; process-loss leases expire on the DB clock. Increase limits only with Function/account and database headroom. Limits from all deployments share platform constraints. |
| Realtime disconnect, silent lag, or repeated rotations | Expect durable gap replay and bounded fallback. Check notifier errors and observed live latency. Keep raw query windows simple and unordered. Order/deduplicate by contiguous durable offsets in the Function; do not advance cursors using notification arrival order. |
| Frozen chunks or pending outbox keep growing | Confirm the correct branch's schedule trigger is invoking its authenticated endpoint. Inspect failed maintenance and oldest-work age. Append hints improve normal progress; cron remains the durable recovery mechanism. Tune bounded work/time budgets and rerun sustained load before relying on a higher input rate. |
| Subscription delivery is late | Inspect durable retry time, lease expiry, pending links, quota, and wake state. A failed delivery backs off persistently. Do not reset acknowledgment cursors or mark pending fan-out complete to clear a queue. Cron cadence is not a one-second delivery guarantee. |
| Scoped token returns `409 FENCED` | Stop using that wake identity. Claim or process the current generation through the subscription workflow. Restored epochs, completed/expired wakes, deletion, and recreation intentionally fence old work. |
| Scoped token returns `401` | Check the exact route/name, token transport, key ID, and current verification keyring. Tokens authorize scoped operations only. Never place them in query strings or logs. A deliberately revoked key makes its tokens unauthenticatable. |
| `INSTALLATION_FENCED`, binding mismatch, or unsupported migration | Keep ingress stopped. Inspect installation/schema identity against the intended branch and configured storage. Follow explicit stopped upgrade or recovery; do not change database bindings to make a stale process pass readiness. |
| Missing/corrupt sealed object | Stop exposing affected data and keep recovery quiesced. Restore the exact key/bytes from a trusted retained source, validate the full envelope/hash, and resume verification. Never fabricate a shorter stream, skip a frame, or mark verification complete manually. |
| Object retirement is deferred | Inspect policy hold, per-object collection deadline, valid replay pins, fork references, pending subscription references, and maintenance progress. A recovery/policy change conservatively extends holds. Decreasing policy cannot accelerate an existing persisted promise. |
| Quarantined orphan | Inspect the immutable object/key and original upload provenance on an operator path. The collector never automatically deletes invalid envelopes. Repair an exact trusted object or resolve the foreign/corrupt object explicitly; do not clear quarantine merely to force deletion. |
| Slow reader or canceled request | Reconnect using the last durable checkpoint. The server bounds replay pages, live buffers, SQL/object queues, and live lifetimes. A late object download cannot emit after its replay deadline or expired pin. |

## Monitoring and thresholds

Export `onEvent` counters and durations, response status rates, `onError` exception classes, and the example's authenticated runtime snapshots. Redact infrastructure exceptions before logging. Hooks are observation paths and cannot change committed results. An SSE request's time to response headers does not measure its delivery lag or total lifetime.

Collect backlog and oldest-work age independently from request latency: local HOT/FROZEN bytes, frozen chunks, pending/total outbox, subscription next-work/retry times, expired claims, valid live leases/pins, retired object deadlines, and quarantined objects. `inspectReclamation(pool, { schema })` reads policy; optional `includeBacklog: true` scans retained ledger metadata and belongs on an operator pool at a measured cadence. Alert when backlog age/size grows across several normal maintenance periods rather than using a single routed health sample as a fleet total. Set numeric alarms from the exact deployment profile and application objectives.

Verify schedule recovery by stopping hints/workers in an isolated environment and leaving durable pending work. Required CI has separate hints-without-cron and cron-without-hints checks. Do not disable both mechanisms in production while expecting unattended progress. Detailed provider metrics complement application counters; neither a successful cron sample nor a platform quota establishes availability for the complete stack.

## Keys and credentials

Persist subscription wrapping/verification keys outside the database restore timeline and include them in backup policy. All instances must share the same keyring. Rotate wrapping keys in two stages: first deploy every instance with both old and new keys while keeping the old current key, then switch the current key. This prevents a new token/ciphertext from reaching a still-serving instance that lacks its key. Retain old keys for supported recovery and authentic stale-token fencing; explicit trust revocation changes that contract to 401. Rotate the Ed25519 webhook signing key through `initializeSubscriptions({ rotateSigningKey: true, ... })` and retain historical public keys for receiver verification.

Rotate Postgres, Realtime, S3, and application credentials through the consumer's secret manager. Deploy/warm the replacement configuration, verify readiness and representative protocol operations, then revoke the old credential. Never fall back to anonymous access or a different bucket after authentication failures. The library has no dependency on a Neon management API key at runtime.

## Upgrade, recovery, and collection

Follow [installation/recovery](operations-v1.md), [in-place restore](inplace-restore-v1.md), the [upgrade matrix](upgrade-compatibility-v1.md), and the [reclamation contract](reclamation-v1.md). Stop ingress, cold-starting instances, timers, scheduled workers, and object work before DDL or rollback. Epoch fencing supplements that stop; rolling an epoch backward cannot invalidate a process that still remembers it.

Declare the entire supported database recovery window, including external backups, before enabling collection. The extra margin is at least fifteen minutes. An indefinitely restorable installation must retain its objects indefinitely. SQL restore does not itself restore Object Storage. A supported point after a manifest's retirement may retain that unreachable manifest after its object is collected; verification skips retired manifests and requires every reachable sealed object. Missing reachable objects or keys keep the installation quiesced.

After verification, activate a new epoch, deploy fresh instances, require repeated authenticated readiness, replay representative binary/JSON/fork streams, verify producer retry and live final-byte/EOF behavior, then reopen ingress. Restores can intentionally discard externally observed writes and acknowledgments; downstream applications must reconcile their checkpoints and side effects.
