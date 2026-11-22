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
| `DS_ALLOWED_ORIGINS_JSON` | Optional exact browser-origin allowlist; defaults to `[]` |
| `DS_PUBLIC_BASE_URL` | Optional canonical stream mount URL for `Location` headers |
| `DS_LIVE_FALLBACK_MS` | Optional coalesced fallback interval; defaults to 5000 ms |

Replace the example API-key policy with the consuming application's identity and permission checks as needed. The example gives each verified key all operations within its configured tenant. Request parameters cannot choose that tenant. Keep keys in server-side secret storage; browser clients should use the application's short-lived identity credentials.

Before deployment, use a **direct** connection to call `migrate(pool, { schema, storageIdentity: objects.identity, environmentId: branchId, maxLiveReaders: 80 })`, then explicitly call `initializeSubscriptions(pool, { schema, keyring })`. The S3 adapter must target the same branch endpoint, region, and bucket as the Function. Storage/environment binding rejects a mismatched branch. Use the [quiesced recovery procedure](../../docs/operations-v1.md) for adoption.

Bundle `index.ts` and its installed dependencies as Node.js 24 ESM. The repository's `scripts/neon-test-function.mjs` shows the concrete bundle/ZIP/deploy flow using the Neon SDK, passes secrets as a structured encrypted deployment environment, polls build readiness, and verifies authenticated runtime health against the bundle hash. It only accepts a recorded isolated test branch. Consumers should use their normal deployment pipeline and migration controls for production.

## Maintenance and recovery

Configure a Neon schedule trigger with `function_path: /_trigger`, the exact configured trigger name, and cron `* * * * *` for a one-minute recovery cadence. That route uses `parseTriggerDelivery` and the platform's stripping of client-supplied `x-neon-*` headers. It stays outside application API-key auth. It is intended for Neon-managed ingress; do not expose this trust model through a different runtime that preserves forged platform headers.

For manual or application-scheduler work, POST `/_maintenance` with `x-ds-maintenance-key`. This credential is distinct from application API keys. Each invocation processes at most eight chunks. Large backlogs need repeated calls and capacity monitoring. No timer or untracked task after an append response is required to recover sealing work. A one-minute cron cadence does not provide subsecond job recovery.

`GET /health` requires an application bearer key and reports storage mode, implemented capabilities, and the build hash. Shutdown releases owned clients after a bounded drain. Interrupted uploads remain recoverable through persisted frozen frames and expiring worker claims.

## Qualification

The real deployment tests verify unauthorized and cross-tenant requests, the exact bundle hash, initial SSE flushing, final-append-before-EOF delivery, sealed replay after local payload removal, producer deduplication after sealing, forged-trigger rejection, and actual scheduled sealing without an application maintenance request. The test profile sets fallback to 60 seconds so its shorter SSE deadline requires Realtime delivery.

Function ingress has a default account-wide concurrency limit of 100. Installation-wide database admission bounds live readers across instances. Each installation still needs a share of the account budget and ingress protection. Size aggregate live readers, pools, and maintenance concurrency for the consuming deployment. Sustained capacity and availability claims remain unqualified.
