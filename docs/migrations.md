# Database baseline

The unpublished library installs its current schema directly, organized by responsibility:

| File | Owns |
| --- | --- |
| `0001_installation.sql` | Installation/storage identity, recovery and serving epochs, shared live-reader capacity and leases |
| `0002_streams.sql` | Paths and generations, fork references, hot/frozen appends, producer sessions, expiration work |
| `0003_storage.sql` | Chunk manifests and verified block directories, hot-chunk scheduling, replay pins, object ledger, collection policy |
| `0004_subscriptions.sql` | Signing keys, subscriptions and members, wakes and generation sequence, pending links, change outbox |

Run the complete baseline explicitly through a direct connection before serving:

```ts
import { migrate } from '@samwillis/durable-streams-neon/migrations'

await migrate(directPool, {
  schema: 'durable_streams',
  environmentId: branchId,
  storageIdentity: objects.identity, // Omit for a hot-only installation.
})
```

Initialization is transactional and serialized with an advisory lock. The migration ledger retains IDs and source checksums; repeated calls validate the installed history and preserve identity. Future released schema changes can append migrations to this baseline. Constructors and imports perform no DDL. Initialize the external subscription keyring separately when using subscriptions.

The unpublished storage version 3 baseline requires a **fresh schema and empty object namespace**. Earlier development states are disposable; no backfill or conversion layer ships. The runner rejects incompatible migration checksums and storage versions. Imports and constructors do not reset state or perform DDL.

The current baseline uses ordinal/history offsets and block object format 3. Same-version restarts and independently packed instances share it. Recovery, branch adoption and current-data collection fences remain in place. Installing a fresh baseline is separate from a provider restore; see [recovery](operations-v1.md) and [storage](compact-storage-v2.md).
