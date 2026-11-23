import { randomUUID } from 'node:crypto'
import type { PoolClient } from 'pg'
import type { StoredFrame } from './replay-plan.js'
import { StreamError } from './errors.js'
import type { InstallationGuard } from './installation.js'

export const REPLAY_PIN_MS = 315_000
export interface ReplayLease { readonly id: string; readonly chunks: number }
export function assertReplayDeadline(deadline: number): void {
  if (performance.now() >= deadline) throw new StreamError(503, 'REPLAY_DEADLINE', 'Replay deadline exceeded; retry from the durable checkpoint', { 'Retry-After': '1' })
}

/** Acquire under the replay snapshot; object I/O never holds its SQL locks. */
export class ReplayPins {
  constructor(private readonly schema: string, private readonly guard: InstallationGuard) {}
  async acquire(client: PoolClient, frames: readonly StoredFrame[]): Promise<ReplayLease | undefined> {
    const chunks = [...new Set(frames.flatMap((frame) => frame.storage === 'object' ? [frame.manifest.id] : []))].sort()
    if (!chunks.length) return undefined
    const locked = await client.query<{ id: string; retired_at: Date | null }>(`SELECT id, retired_at FROM ${this.schema}.chunks
      WHERE id = ANY($1::uuid[]) ORDER BY id FOR SHARE`, [chunks])
    if (locked.rows.length !== chunks.length || locked.rows.some((chunk) => chunk.retired_at !== null)) {
      throw new StreamError(503, 'REPLAY_RETIRED', 'Replay snapshot retired; retry from the durable checkpoint', { 'Retry-After': '1' })
    }
    const lease = { id: randomUUID(), chunks: chunks.length }, installation = await this.guard.snapshot()
    await client.query(`INSERT INTO ${this.schema}.replay_pins (id, chunk_id, serving_epoch, expires_at)
      SELECT $1, id, $2, clock_timestamp() + $3 * interval '1 millisecond' FROM ${this.schema}.chunks WHERE id=ANY($4::uuid[])`,
    [lease.id, installation.serving_epoch, REPLAY_PIN_MS, chunks])
    return lease
  }
  async validate(lease: ReplayLease | undefined): Promise<void> {
    if (!lease) return
    const valid = await this.guard.run({ readOnly: true }, (client) => client.query(`SELECT id FROM ${this.schema}.replay_pins WHERE id=$1
      GROUP BY id HAVING count(*)=$2 AND min(expires_at)>clock_timestamp()`, [lease.id, lease.chunks]))
    if (!valid.rowCount) throw new StreamError(503, 'REPLAY_EXPIRED', 'Replay lease expired; retry from the durable checkpoint', { 'Retry-After': '1' })
  }
  async release(lease: ReplayLease | undefined): Promise<void> {
    if (lease) await this.guard.releaseReplayPins(lease.id)
  }
}
