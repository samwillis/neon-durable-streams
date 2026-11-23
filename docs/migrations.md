# Database baseline

The unpublished library installs its current schema directly, organized by responsibility:

| File | Owns |
| --- | --- |
| `0001_installation.sql` | Installation/storage identity, recovery and serving epochs, shared live-reader capacity and leases |
| `0002_streams.sql` | Paths and generations, fork references, hot/frozen appends, producer sessions, expiration work |
| `0003_storage.sql` | Chunk manifests and boundary directories, hot-chunk scheduling, replay pins, object ledger, collection policy |
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

This consolidation replaces the unreleased nine-step development history. Existing development installations require a **fresh schema**. The runner rejects their recorded history before changing it. It does not reset databases, rewrite migration receipts, adopt old data, or run a historical conversion. Preserve any development data you need and keep its matching Git revision and object store for reading it. The earlier migration SQL and conversion tooling remain available in Git history; they are not part of the current package. Partial migration targets are no longer supported.

Immutable object format v1 and the final logical schema are unchanged. Recovery, branch adoption, and current-data collection fences remain in place; a new baseline is not a recovery procedure. See [recovery](operations-v1.md) and [storage](compact-storage-v2.md).
