import { createHash, randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import type { Pool } from 'pg'
import { StorageCompatibilityError } from './errors.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'

export const MIGRATION_IDS = ['0001_installation', '0002_streams', '0003_storage', '0004_subscriptions'] as const
export const STORAGE_VERSION = 2

export interface Migration {
  readonly id: string
  readonly checksum: string
  readonly sql: string
}

export async function migrationPlan(schema = 'durable_streams'): Promise<readonly Migration[]> {
  const identifier = validateSchema(schema)
  return Promise.all(MIGRATION_IDS.map(async (id) => {
    const source = await readFile(new URL(`../migrations/${id}.sql`, import.meta.url), 'utf8')
    return { id, checksum: createHash('sha256').update(source).digest('hex'), sql: source.replaceAll('__SCHEMA__', identifier) }
  }))
}

/** Explicit, transactional DDL. Call through the deployment's direct connection. */
export async function migrate(pool: Pool, options: { readonly schema?: string; readonly storageIdentity?: string; readonly environmentId?: string; readonly maxLiveReaders?: number } = {}): Promise<readonly string[]> {
  const schema = options.schema ?? 'durable_streams'
  const identifier = validateSchema(schema)
  const plan = await migrationPlan(schema)
  for (const key of Object.keys(options)) {
    if (!['schema', 'storageIdentity', 'environmentId', 'maxLiveReaders'].includes(key)) throw new TypeError(`Unsupported migration option: ${key}`)
  }
  return transaction(pool, { timeoutMs: 60_000 }, async (client) => {
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`durable-streams:migrate:${schema}`])
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${identifier}`)
    await client.query(`CREATE TABLE IF NOT EXISTS ${identifier}.migrations (
      id text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`)
    const applied = await client.query<{ id: string; checksum: string }>(`SELECT id, checksum FROM ${identifier}.migrations ORDER BY id`)
    for (const [index, row] of applied.rows.entries()) {
      const expected = plan[index]
      if (expected?.id !== row.id || expected.checksum !== row.checksum) {
        throw new StorageCompatibilityError(`Unknown or modified migration: ${row.id}; this development baseline requires a fresh schema`)
      }
    }
    const completed: string[] = []
    for (const migration of plan) {
      if (applied.rows.some((m) => m.id === migration.id)) continue
      await client.query(migration.sql)
      await client.query(`INSERT INTO ${identifier}.migrations (id, checksum) VALUES ($1, $2)`, [migration.id, migration.checksum])
      completed.push(migration.id)
    }
    if (completed.includes('0001_installation')) {
      await client.query(`INSERT INTO ${identifier}.installation (id, storage_version) VALUES ($1, $2)`, [randomUUID(), STORAGE_VERSION])
    } else {
      const installation = await client.query<{ storage_version: number }>(`SELECT storage_version FROM ${identifier}.installation`)
      if (installation.rows.length !== 1 || installation.rows[0]!.storage_version !== STORAGE_VERSION) {
        throw new StorageCompatibilityError('Missing or incompatible installation identity; explicit recovery required')
      }
    }
    const beforeBinding = (await client.query<{ storage_identity: string | null }>(`SELECT storage_identity FROM ${identifier}.installation FOR UPDATE`)).rows[0]!
    if (options.storageIdentity !== undefined) {
      if (!options.storageIdentity || options.storageIdentity.length > 2048) throw new TypeError('Invalid storage identity')
      const binding = await client.query(`UPDATE ${identifier}.installation SET storage_identity = $1
        WHERE storage_identity IS NULL OR storage_identity = $1 RETURNING id`, [options.storageIdentity])
      if (binding.rowCount !== 1) throw new StorageCompatibilityError('Storage identity differs; explicit branch adoption or recovery is required')
      if (beforeBinding.storage_identity === null) {
        // Enabling tiered storage on an existing hot-only installation also
        // enrolls its old tails. This one-time administrative scan keeps the
        // ordinary append path/indexes free of extra due-counter updates.
        await client.query(`INSERT INTO ${identifier}.hot_chunks SELECT id,clock_timestamp()+interval '1 hour'
          FROM ${identifier}.streams WHERE hot_frames>0 ON CONFLICT DO NOTHING`)
      }
    }
    if (options.environmentId !== undefined) {
      if (!options.environmentId || Buffer.byteLength(options.environmentId) > 2048) throw new TypeError('Invalid environment identity')
      const bound = await client.query(`UPDATE ${identifier}.installation SET environment_id = $1
        WHERE environment_id IS NULL OR environment_id = $1 RETURNING id`, [options.environmentId])
      if (bound.rowCount !== 1) throw new StorageCompatibilityError('Environment identity differs; explicit branch adoption is required')
    }
    if (options.maxLiveReaders !== undefined) {
      if (!Number.isSafeInteger(options.maxLiveReaders) || options.maxLiveReaders < 1 || options.maxLiveReaders > 100_000) throw new RangeError('Invalid installation live reader capacity')
      await client.query(`UPDATE ${identifier}.live_capacity SET max_readers = $1`, [options.maxLiveReaders])
    }
    return completed
  })
}
