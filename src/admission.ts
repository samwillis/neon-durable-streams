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
    await client.query(`DELETE FROM ${identifier}.live_leases WHERE expires_at <= clock_timestamp()`)
    const used = await client.query<{ count: string }>(`SELECT count(*) FROM ${identifier}.live_leases`)
    if (!capacity.rows[0] || Number(used.rows[0]!.count) >= capacity.rows[0].max_readers) {
      throw new StreamError(429, 'INSTALLATION_LIVE_LIMIT', 'Installation live reader capacity reached', { 'Retry-After': '1' })
    }
    await client.query(`INSERT INTO ${identifier}.live_leases (id, serving_epoch, expires_at)
      SELECT $1, serving_epoch, clock_timestamp() + $2 * interval '1 millisecond' FROM ${identifier}.installation`, [id, lifetimeMs])
  })
  return () => guard.run({}, async (client) => { await client.query(`DELETE FROM ${identifier}.live_leases WHERE id = $1`, [id]) })
}
