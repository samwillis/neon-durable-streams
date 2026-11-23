# Durable Streams for Neon

A TypeScript library for building your own [Durable Streams](https://durablestreams.com/) server on Neon. Embed a Fetch handler in your Neon Function and use your own Postgres, Object Storage, and Realtime resources. Your application supplies authentication, permissions, and deployment configuration.

Supports append and replay, long-poll and SSE, JSON batching, idempotent producers, stream closure and expiry, recursive forks, and durable subscriptions with webhooks or pull wakes.

**Status:** unpublished; available from a source checkout or package tarball. The current implementation passed all **338 pinned upstream conformance cases**, including subscriptions. See the [qualification report](docs/qualification/2026-11-23-replay-final.md) for tested builds, workloads, and operating limits.

## How it works

- **Neon Function** serves the Durable Streams API through the library's Fetch handler.
- **Neon Postgres** stores stream identities, hot appends, chunk manifests, producer state, and subscriptions.
- **Neon Object Storage** holds sealed immutable chunks. Sealing removes their append rows from Postgres; historical replay uses verified HTTP range reads and a bounded block cache.
- **Neon Realtime** delivers committed append payloads to live readers after authorized durable catch-up. The Function orders and deduplicates updates, checks continuity, and replays durable data when needed.

Clients own their resume offsets. Ordinary readers do not create persistent per-reader cursors; explicitly created durable subscriptions store their acknowledgements.

## Get started

Use **Node.js 24**, `pg` 8, and a Neon branch with **Postgres 18**, Realtime enabled, and a private Object Storage bucket. Start with the [Neon Function example](examples/neon-function/README.md) for authentication, environment variables, maintenance triggers, health checks, and shutdown handling.

The package is not on npm yet. Build a tarball from this checkout:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm pack
```

Install that tarball in your application:

```sh
pnpm add /path/to/samwillis-durable-streams-neon-0.0.1.tgz pg @neon/functions
```

### Create the handler

The following configuration uses the same environment variable names as the Neon Function example. `authenticate` and `authorize` are application-owned functions from your `auth.ts` module.

```ts
import { Pool } from 'pg'
import { attachDatabasePool } from '@neon/functions'
import { createDurableStreams } from '@samwillis/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@samwillis/durable-streams-neon/neon'
import { createS3ChunkStorage } from '@samwillis/durable-streams-neon/s3'
import { authenticate, authorize } from './auth.js'

const pool = new Pool({ connectionString: process.env.DATABASE_URL!, max: 5 })
attachDatabasePool(pool)

const objects = createS3ChunkStorage({
  endpoint: process.env.AWS_ENDPOINT_URL_S3!,
  region: process.env.AWS_REGION!,
  bucket: process.env.DS_BUCKET!,
})
const notifier = createNeonRealtimeNotifier({
  url: process.env.DS_REALTIME_URL!,
  secret: process.env.DS_REALTIME_SECRET!,
  database: new URL(process.env.DATABASE_URL!).pathname.slice(1),
})

const streams = createDurableStreams({
  postgres: {
    pool,
    schema: 'durable_streams',
    environmentId: process.env.DS_ENVIRONMENT_ID!, // Serving Neon branch ID.
  },
  storage: { objects },
  live: { notifier },
  auth: { authenticate, authorize },
})

export default { fetch: streams.fetch }
```

`authenticate(request)` verifies the caller and returns `{ tenantId, subject }`, optionally with `expiresAt` in Unix milliseconds, or `null`. `authorize(context)` checks that principal's operation and stream path and returns a boolean. Both hooks may be asynchronous. Derive tenant identity from verified credentials. Keep Realtime and storage credentials on the server.

The default stream mount is `/v1/stream`. Set `basePath` to change it, `publicBaseUrl` to set the canonical mount URL, and `allowedOrigins` to allow specific browser origins. See the [complete example](examples/neon-function/index.ts) for a working API-key policy and the routes omitted from this minimal handler.

### Initialize the database

Before serving requests, run `migrate` as an explicit deployment step using a **direct** Postgres connection. Use the same schema, branch ID, and object-storage configuration as the handler:

```ts
import { migrate } from '@samwillis/durable-streams-neon/migrations'

await migrate(directPool, {
  schema: 'durable_streams',
  environmentId: process.env.DS_ENVIRONMENT_ID!,
  storageIdentity: objects.identity,
})
```

Here, `directPool` is a deployment-owned `pg.Pool` using your direct connection string, and `objects` uses the adapter configuration shown above. Close those deployment clients when initialization finishes. Imports and constructors do not run migrations or provision Neon resources. Use a pooled connection for request traffic.

The current prerelease requires a fresh schema and empty object namespace. Earlier development layouts require recreation. See [database initialization](docs/migrations.md) and [installation and recovery](docs/operations-v1.md).

### Schedule maintenance

Run bounded batches from an authenticated worker or Neon schedule trigger:

```ts
await streams.maintain({
  maxChunks: 64,
  maxSubscriptionWork: 64,
  timeBudgetMs: 40_000,
})
```

Maintenance seals chunks, dispatches subscription work, and cleans up metadata. Schedule it even when your handler starts opportunistic work after appends: scheduled passes recover work after process loss and during quiet periods. The [Function example](examples/neon-function/README.md#maintenance-and-recovery) wires a minute cron trigger and bounded append hints. For an administrative drain of younger hot tails, use `forceHot: true`.

### Read and write a stream

With the example's bearer-key authentication, set `STREAM_URL` to your Function's `/v1/stream` mount and `STREAM_TOKEN` to an application key:

```sh
curl -i -X PUT "$STREAM_URL/example" \
  -H "Authorization: Bearer $STREAM_TOKEN" \
  -H 'Content-Type: text/plain' --data-binary 'hello'

curl -i -X POST "$STREAM_URL/example" \
  -H "Authorization: Bearer $STREAM_TOKEN" \
  -H 'Content-Type: text/plain' --data-binary ' world'

curl -i "$STREAM_URL/example?offset=-1" \
  -H "Authorization: Bearer $STREAM_TOKEN"
```

Save `Stream-Next-Offset` and pass it as `offset` on the next read. Continue paging until `Stream-Up-To-Date: true`. Treat offsets as opaque strings. Add `live=long-poll` or `live=sse` to follow new appends; `offset=now` starts at the current tail. With SSE, advance the saved checkpoint only after its control event. See the [Durable Streams protocol](https://github.com/durable-streams/durable-streams) for client libraries and the complete HTTP interface.

## Configuration and operations

| Setting | Default |
| --- | --- |
| Received body / stored frame limit | 1 MiB each |
| Replay page | 2 MiB, up to 8192 frames and 64 immutable objects |
| Chunk sealing thresholds | 4 MiB, 8192 frames, or one hour from the first append |
| Verification block target | 64 KiB |
| Verified cache | 32 MiB per server instance |
| Cross-object read-ahead | Off; configurable to one following read |

Use the `limits` and `storage` options to tune page budgets and sealing policy; the 64-object page cap is fixed. A chunk's triggering frame can exceed its byte target. The [storage contract](docs/compact-storage-v2.md) describes hard bounds and separate pending-read and response-memory budgets; size these alongside Function concurrency and database capacity.

To enable durable subscriptions, initialize a persistent keyring with `initializeSubscriptions` from `/subscriptions`, then supply `subscriptions: { keyring }` to the handler. All instances share that keyring. Follow the [subscription guide](docs/subscriptions-v1.md) for registration, acknowledgements, delivery, and key rotation.

Your application owns its database pool, notifier, and storage adapter. On shutdown, await `streams.close()` before closing those clients. Monitor request errors, live-reader admission, staging bytes, maintenance backlog, and delivery lag. Physical object collection is disabled by default; historical recovery needs every referenced object's original bytes, including objects deleted after the restore point. Follow the [operator runbook](docs/operator-runbook.md) and [collection contract](docs/reclamation-v1.md) before enabling deletion or restoring a branch.

## Reference

| Guide | Covers |
| --- | --- |
| [Neon Function example](examples/neon-function/README.md) | Deployment configuration, authentication, cron, health, and shutdown |
| [Storage layout](docs/compact-storage-v2.md) | Hot data, sealed blocks, range reads, and resource bounds |
| [Subscriptions](docs/subscriptions-v1.md) | Webhooks, pull wakes, scoped tokens, and acknowledgements |
| [Operations](docs/operator-runbook.md) | Monitoring, keys, deployment, and recovery |
| [Qualification](docs/qualification/README.md) | Tested revisions, conformance, benchmarks, and limits |
| [Development](AGENTS.md) | Repository conventions, local checks, and real-Neon testing |

## License

[Apache-2.0](LICENSE). Copyright 2026 Databricks, Inc.
