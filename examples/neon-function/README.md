# Neon Function consumer

This consumer embeds the library in a Node.js 24 Neon Function. It owns the pool, Realtime adapter, S3 adapter, authentication policy, shutdown handling, and maintenance routes. The library never provisions resources or runs migrations while handling a request.

The example demonstrates deployment wiring. The package remains unreleased while recovery/adoption, reclamation, and production qualification are incomplete.

## Environment

Neon injects `DATABASE_URL` and the `AWS_*` Object Storage variables. Create a private bucket on the serving branch and enable Realtime on Postgres 18. Supply these Function environment values through your secret/deployment system:

| Variable | Meaning |
| --- | --- |
| `DS_ENVIRONMENT_ID` | Serving Neon branch ID, bound explicitly during migration |
| `DS_SCHEMA` | Dedicated, explicitly migrated library schema |
| `DS_BUCKET` | Private sealed-chunk bucket |
| `DS_REALTIME_URL`, `DS_REALTIME_SECRET` | Branch Realtime endpoint and backend secret |
| `DS_KEYRING_JSON` | Persistent wrapping keyring (`currentKeyId`, base64 `keys`) for signing keys and wake tokens |
| `DS_AUTH_JSON` | JSON array of `{ key, tenantId, subject }`; keys have at least 32 characters |
| `DS_MAINTENANCE_KEY` | Separate random server-side credential for manual maintenance |
| `DS_MAINTENANCE_TRIGGER_NAME` | Exact name of the configured scheduled recovery trigger |
| `DS_BUILD_HASH` | Deployed bundle/build identifier returned by authenticated health |
| `DS_DEPLOYMENT_ID` | Optional unique rollout identifier; returned by health separately from the code hash; defaults to `DS_BUILD_HASH` |
| `DS_ALLOWED_ORIGINS_JSON` | Optional exact browser-origin allowlist; defaults to `[]` |
| `DS_PUBLIC_BASE_URL` | Optional canonical stream mount URL for `Location` headers |
| `DS_MAINTENANCE_HINTS` | `on` by default; coalesced bounded work after successful mutations; `off` isolates cron qualification |
| `DS_CRON_MAX_CHUNKS`, `DS_CRON_SUBSCRIPTION_WORK` | Bounded cron batches, each defaulting to 64 (maximum 128) |
| `DS_LIVE_FALLBACK_MS` | Optional coalesced fallback interval; defaults to 5000 ms |
| `DS_LIVE_VALIDATION_MS` | Optional coalesced lifecycle/epoch validation interval; defaults to 1000 ms |

Replace the example API-key policy with the consuming application's identity and permission checks as needed. The example gives each verified key all operations within its configured tenant. Request parameters cannot choose that tenant. Keep keys in server-side secret storage; browser clients should use the application's short-lived identity credentials.

Before deployment, use a **direct** connection to call `migrate(pool, { schema, storageIdentity: objects.identity, environmentId: branchId, maxLiveReaders: 80 })`, then explicitly call `initializeSubscriptions(pool, { schema, keyring })`. The S3 adapter must target the same branch endpoint, region, and bucket as the Function. Storage/environment binding rejects a mismatched branch. Use the [quiesced recovery procedure](../../docs/operations-v1.md) for adoption.

Bundle `index.ts` and its installed dependencies as Node.js 24 ESM. The repository's `scripts/neon-test-function.mjs` shows the concrete bundle/ZIP/deploy flow using the Neon SDK, passes secrets as a structured encrypted deployment environment, polls build readiness, and verifies authenticated runtime health against the bundle hash. It only accepts a recorded isolated test branch. Consumers should use their normal deployment pipeline and migration controls for production.

## Maintenance and recovery

Configure a Neon schedule trigger with `function_path: /_trigger`, the exact configured trigger name, and cron `* * * * *` for a one-minute recovery cadence. That route uses `parseTriggerDelivery` and the platform's stripping of client-supplied `x-neon-*` headers. It stays outside application API-key auth. It is intended for Neon-managed ingress; do not expose this trust model through a different runtime that preserves forged platform headers.

For manual or application-scheduler work, POST `/_maintenance` with `x-ds-maintenance-key`. This credential is distinct from application API keys. Manual invocations process at most eight chunks. Scheduled invocations default to 64 chunks and 64 subscription work rounds, with a 40-second soft budget. Large backlogs need repeated calls and capacity monitoring. The default write path starts bounded coalesced maintenance using Neon `waitUntil`. Only the invocation starting a pass retains it. Hints upload already-frozen chunks and dispatch subscriptions, preserving small hot tails. The cron trigger recovers work after process loss or when there are no further requests. A one-minute cron cadence does not provide subsecond job recovery.

`GET /health` requires an application bearer key and reports readiness, storage mode, implemented capabilities, the build hash, and deployment ID. It returns 503 while shutting down. Deployment verification requires three consecutive one-second probes with `ready: true` and both expected identifiers; sanitized probe traces are retained as qualification artifacts. Use a new deployment ID when deploying identical code after recovery, so a response from an earlier deployment cannot satisfy the rollout check. Shutdown releases owned clients after a bounded drain. Interrupted uploads remain recoverable through persisted frozen frames and expiring worker claims.

## Qualification

The real deployment tests verify unauthorized and cross-tenant requests, the exact bundle hash, initial SSE flushing, final-append-before-EOF delivery, sealed replay after local payload removal, producer deduplication after sealing, forged-trigger rejection, and actual scheduled sealing without an application maintenance request. The test profile sets both fallback and periodic validation to 60 seconds so its shorter SSE deadline requires Realtime delivery. The native adapter forwards committed payloads from bounded unordered raw subscriptions; see the [payload delivery contract](../../docs/realtime-payload-v1.md).

Function ingress has a default account-wide concurrency limit of 100. Installation-wide database admission bounds live readers across instances. Each installation still needs a share of the account budget and ingress protection. Size aggregate live readers, pools, and maintenance concurrency for the consuming deployment. Sustained capacity and availability claims remain unqualified.

The qualification tooling also deploys a separate HTTPS webhook receiver fixture. Function-to-Function tests verify signatures against JWKS, synchronous completion, scoped callbacks, and retry after HTTP 500 over real public TLS connections. The fixture uses a fixed trusted issuer/tenant and administrative key; it is not a production receiver implementation or part of the shipped consumer.

The hosted-service conformance/ingress fixture sets `DS_MAINTENANCE_HINTS=off` so the scheduled-recovery assertion actually requires cron. The load qualification workflow enables hints to measure the production profile. A time budget is soft: each begun round finishes its bounded work before the deadline prevents another round. Every round gives subscription dispatch, link recovery, wake preparation, lifecycle cleanup, and sealing a turn. Measure backlog and tune the counts for the workload.

A separate qualification branch runs the same Function with hints enabled and no cron trigger. It proves pull-wake delivery through Realtime and full-chunk sealing without manual maintenance; the ordinary profile disables hints to prove independent cron recovery. The load workflow uses the combined hints-plus-cron profile.
