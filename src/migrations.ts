import { createHash, randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import type { Pool } from 'pg'
import { StorageCompatibilityError } from './errors.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'

export const MIGRATION_IDS = ['0001_core', '0002_chunks', '0003_forks'] as const

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
export async function migrate(pool: Pool, options: { readonly schema?: string; readonly storageIdentity?: string } = {}): Promise<readonly string[]> {
  const schema = options.schema ?? 'durable_streams'
  const identifier = validateSchema(schema)
  const plan = await migrationPlan(schema)
  return transaction(pool, { timeoutMs: 60_000 }, async (client) => {
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`durable-streams:migrate:${schema}`])
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${identifier}`)
    await client.query(`CREATE TABLE IF NOT EXISTS ${identifier}.migrations (
      id text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`)
    const applied = await client.query<{ id: string; checksum: string }>(`SELECT id, checksum FROM ${identifier}.migrations ORDER BY id`)
    for (const row of applied.rows) {
      if (!plan.some((m) => m.id === row.id && m.checksum === row.checksum)) {
        throw new StorageCompatibilityError(`Unknown or modified migration: ${row.id}`)
      }
    }
    const completed: string[] = []
    for (const migration of plan) {
      if (applied.rows.some((m) => m.id === migration.id)) continue
      await client.query(migration.sql)
      await client.query(`INSERT INTO ${identifier}.migrations (id, checksum) VALUES ($1, $2)`, [migration.id, migration.checksum])
      completed.push(migration.id)
    }
    if (completed.includes('0001_core')) {
      await client.query(`INSERT INTO ${identifier}.installation (id, storage_version) VALUES ($1, 1)`, [randomUUID()])
    } else {
      const installation = await client.query<{ storage_version: number }>(`SELECT storage_version FROM ${identifier}.installation`)
      if (installation.rows.length !== 1 || installation.rows[0]!.storage_version !== 1) {
        throw new StorageCompatibilityError('Missing or incompatible installation identity; explicit recovery required')
      }
    }
    if (options.storageIdentity !== undefined) {
      if (!options.storageIdentity || options.storageIdentity.length > 2048) throw new TypeError('Invalid storage identity')
      const binding = await client.query(`UPDATE ${identifier}.installation SET storage_identity = $1
        WHERE storage_identity IS NULL OR storage_identity = $1 RETURNING id`, [options.storageIdentity])
      if (binding.rowCount !== 1) throw new StorageCompatibilityError('Storage identity differs; explicit branch adoption or recovery is required')
    }
    return completed
  })
}
