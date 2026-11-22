# Installation admission and recovery

Migration 0005 adds a serving epoch, explicit environment binding, resumable recovery, and installation-wide live-reader admission. Administrative operations live in the `/operations` entrypoint. They use a consumer-owned direct Postgres pool; no management API credential is required by the library.

## Deployment identity and admission

Bind production installations with `migrate(pool, { schema, storageIdentity: objects.identity, environmentId: neonBranchId, maxLiveReaders: 80 })`. Supply the same `postgres.environmentId` to every server/worker. A copied database retains the old identity and rejects configuration for the new branch until explicitly adopted. Imports and request handling cannot change that binding.

Every writer transaction first acquires an installation `FOR KEY SHARE` lock and validates its pinned UUID/epoch/environment/storage identity. Recovery takes `FOR UPDATE`, waiting for those transactions before changing the epoch. Short, read-only snapshots validate the same identity; materialized replay checks again after object downloads. A stale worker may finish an immutable upload, but cannot publish its manifest or advance durable subscription state.

Live requests acquire a slot under a separate serialized capacity row. All processes sharing the installation use the same persisted limit (default 80), in addition to local `live.maxReaders`. Slots expire on the database clock after a bounded request lifetime plus grace; cancellation and shutdown release them promptly, including after an epoch fence. A local hard deadline precedes lease expiry, and live delivery checks cancellation before emission. Process eviction therefore delays slot recovery rather than leaking capacity indefinitely. Realtime installation invalidation and periodic SQL validation fence payload sessions. Recovery activation refuses unexpired leases from earlier epochs; drain them or wait for expiry before activating. See the [payload delivery contract](realtime-payload-v1.md).

This limits logical readers within one installation. Allocate the account's Neon Function budget across **all** installations, Functions, write traffic, health, and maintenance. Admission itself runs inside an invocation; it cannot increase Neon's platform invocation quota or replace ingress rate limiting. Suspended processes can retain platform resources beyond an application deadline. The default 80 is a finite initial setting, not a measured capacity guarantee.

Transaction timeouts default to 10 seconds, with a five-second lock timeout. Consumers can set finite `postgres.transactionTimeoutMs` and `postgres.lockTimeoutMs` values from 1 through 300,000 ms. Those limits apply transaction-locally to runtime writes, replay, admission, and maintenance; session settings are restored afterward. Budget the caller's pool acquisition timeout separately. A remote coordinator running many contending transactions needs a larger budget than a Function colocated with Postgres. Timeouts never trigger an automatic commit retry.

## Quiesced restore or branch adoption

1. Block public ingress, pause scheduled/application workers, drain live reads, and stop all serving instances. Preserve the current database and external wrapping keys using your backup policy. This is mandatory before an in-place database rollback: restoring an old database can restore an old epoch that an old process still remembers. Database fencing alone cannot stop that process from becoming valid again after rollback.
2. Restore the database or create the isolated target branch. Retain Object Storage independently. Neon database restore does not roll back Object Storage. For a new bucket, copy every referenced immutable object using its exact key and bytes before verification. Do not garbage-collect objects based on the restored database's apparent references.
3. Run compatible migrations while serving remains stopped. Use `inspectInstallation` to record the restored installation UUID and serving epoch. Do not pass a different binding to `migrate` as a substitute for adoption.
4. Persist a random UUID operation ID securely, then call `beginRecovery` with the exact inspected UUID/epoch and target environment/storage identity. Reuse that operation ID and the identical original arguments after an ambiguous response. Beginning recovery is idempotent for those arguments and changes the serving epoch atomically. Save its returned token/cursor context; do not expose recovery operations on the public API.
5. Call `verifyRecovery` repeatedly with the token, target storage adapter, wrapping keyring, and a finite batch size. It validates every sealed object's full checksum, size, installation/stream/chunk identities, frame count, range, and payload size. It also verifies access to the active signing key. Object downloads hold no SQL transaction or installation lock. Progress commits under the recovery token and prior cursor; competing progress fails rather than overwriting a newer checkpoint. Missing/corrupt objects or keys leave the installation quiesced. Repair the external resource and resume the same operation.
6. After `complete: true`, call `finishRecovery`. It resets stale frozen-worker claims, fences copied wake tokens/leases, schedules pending subscription work, clears live-reader leases, applies the target bindings, and activates the installation in one transaction. A lost activation response can be retried with the same token. For a new server URL, supply `publicBaseUrl` to **begin** recovery so existing subscription callback URLs are rebound during activation.
7. Deploy fresh instances with the target configuration, verify authenticated health and exact representative binary/JSON/fork replays, and resume workers and ingress. Rotate the branch's webhook signing key explicitly with `initializeSubscriptions({ rotateSigningKey: true, ... })` when the environments require independent trust. Keep old public verification keys for the receiver's replay window.

```ts
import { randomUUID } from 'node:crypto'
import { inspectInstallation, beginRecovery, verifyRecovery, finishRecovery }
  from '@samwillis/durable-streams-neon/operations'

const before = await inspectInstallation(directPool, { schema })
const operationId = randomUUID() // Persist before calling beginRecovery.
const { token } = await beginRecovery(directPool, {
  schema, operationId, expectedInstallationId: before.id,
  expectedServingEpoch: before.serving_epoch,
  environmentId: targetBranchId, storageIdentity: objects.identity,
  publicBaseUrl: 'https://streams.example.com/v1/stream',
})
while (!(await verifyRecovery(directPool, { schema, token, objects, keyring, maxChunks: 16 })).complete) {}
await finishRecovery(directPool, { schema, token })
```

Administrative credentials must remain exclusive during recovery: no raw SQL changes to manifests, bindings, or stream state. External bucket writers must not mutate or delete referenced keys. Verification establishes that recorded objects resolve at verification time; it cannot protect against later out-of-band deletion. The final activation transaction is bounded by 60 seconds and rolls back on timeout. Large installations still need scale qualification of claim/wake reset before this cutover is certified for them.

Point-in-time rollback can discard appends and acknowledgments already observed externally. Producer state and offsets return to the recovery point; downstream consumers must reconcile their checkpoints and side effects. Random wake IDs/tokens are regenerated, so old callbacks cannot acknowledge replacement work. At-least-once external effects remain possible. Recovery does not promise to preserve writes intentionally rolled back by the operator.

## Upgrade and retention policy

This package is unreleased. Upgrade from the pre-0005 checkout using the same stopped-ingress procedure: old prerelease binaries do not contain the serving-epoch guard. Run explicit checksummed migrations, deploy fresh instances, and verify replay/producer continuity before reopening. Older binaries must not run against a newer unsupported schema. Mixed-version rolling upgrades and an in-place restore cutover remain separate qualification gates.

Physical object deletion remains unavailable. All objects and indexes are retained, including objects unreferenced after a rollback. This protects recovery history and in-flight replay while read/reference pins, retirement barriers, and bounded historical retention are unfinished. Monitor retained storage and provision capacity. These tests do not qualify destructive reclamation.

## Observability

`onEvent` emits request method/status/time-to-response-headers and maintenance duration/result counts. It includes no request paths, bodies, credentials, or tokens. Export these observations to the consumer's metrics/tracing system. `onError` receives infrastructure exceptions; redact those before exposing logs. Synchronous throws and asynchronous rejections from either hook cannot change committed outcomes or become unhandled rejections. An SSE request's header latency is not its total session duration.

Alert on sustained 429 admission failures, failed maintenance, oldest pending change/chunk age, subscription retry delay, storage growth, corrupted/missing chunks, and configuration fences. Detailed backlog metrics, dashboards, load/soak evidence, and supported upgrade matrices remain release work.

## Evidence

A stopped-server upgrade fixture preserves sealed v1 bytes, fork references, offsets, and producer retry state from migration 0004 through 0005. Six real-Neon operations tests cover cross-instance capacity, cancellation/expiry recovery, quiescence waiting on in-flight writers, retryable recovery/activation, failed object verification, stale worker publication, and environment adoption. A separate management-plane drill branches a real Neon database at a recorded WAL LSN after sealing, then verifies exact sealed/forked replay, producer deduplication, copied-token fencing, and isolated child writes. It uses an expiring child and restores the disposable parent's expiry after cleanup. A scheduled janitor covers runner loss during the temporary expiry removal. This is evidence for point-in-time **branch** recovery; an in-place restore, mixed-version upgrade, and sustained capacity still need their own drills.
