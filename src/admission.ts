import { randomUUID } from 'node:crypto'
import type { InstallationGuard } from './installation.js'
import { StreamError } from './errors.js'
import { validateSchema } from './path.js'

/** Serialized installation-wide admission; expiry recovers slots after eviction. */
export async function admitLive(guard: InstallationGuard, schema: string, lifetimeMs: number): Promise<() => Promise<void>> {
  const identifier = validateSchema(schema)
  const id = randomUUID()
  await guard.run({}, async (client) => {
    const capacity = await client.query<{ max_readers: number }>(`SELECT max_readers FROM ${identifier}.live_capacity FOR UPDATE`)
    if (!capacity.rows[0]) throw new Error('Missing installation live capacity')
    // Take the count in a new statement after acquiring the capacity lock, so
    // it sees the previous admission's commit even if lock acquisition waited.
    const admitted = await client.query(`WITH expired AS (
      DELETE FROM ${identifier}.live_leases WHERE expires_at <= clock_timestamp()
    ) INSERT INTO ${identifier}.live_leases (id, serving_epoch, expires_at)
      SELECT $1, serving_epoch, clock_timestamp() + $2 * interval '1 millisecond' FROM ${identifier}.installation
      WHERE (SELECT count(*) FROM ${identifier}.live_leases WHERE expires_at > clock_timestamp()) < $3 RETURNING id`, [id, lifetimeMs, capacity.rows[0].max_readers])
    if (!admitted.rowCount) {
      throw new StreamError(429, 'INSTALLATION_LIVE_LIMIT', 'Installation live reader capacity reached', { 'Retry-After': '1' })
    }
  })
  return () => guard.releaseLiveLease(id)
}
