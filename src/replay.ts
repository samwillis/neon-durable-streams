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
    const lease = { id: randomUUID(), chunks: chunks.length }, installation = await this.guard.snapshot()
    const pinned = await client.query(`WITH locked AS MATERIALIZED (
      SELECT id,retired_at FROM ${this.schema}.chunks WHERE id=ANY($4::uuid[]) ORDER BY id FOR SHARE
    ) INSERT INTO ${this.schema}.replay_pins (id, chunk_id, serving_epoch, expires_at)
      SELECT $1, id, $2, clock_timestamp() + $3 * interval '1 millisecond' FROM locked
      WHERE retired_at IS NULL AND (SELECT count(*) FROM locked WHERE retired_at IS NULL)=$5`,
    [lease.id, installation.serving_epoch, REPLAY_PIN_MS, chunks, chunks.length])
    if (pinned.rowCount !== chunks.length) {
      throw new StreamError(503, 'REPLAY_RETIRED', 'Replay snapshot retired; retry from the durable checkpoint', { 'Retry-After': '1' })
    }
    return lease
  }
  /** Validate and release in one transaction after the bytes are owned locally. */
  async complete(lease: ReplayLease | undefined): Promise<void> {
    if (!lease) return
    await this.guard.run({}, async (client) => {
      const released = await client.query<{ valid: boolean }>(`DELETE FROM ${this.schema}.replay_pins WHERE id=$1
        RETURNING expires_at>clock_timestamp() AS valid`, [lease.id])
      if (released.rows.length !== lease.chunks || released.rows.some((row) => !row.valid)) {
        throw new StreamError(503, 'REPLAY_EXPIRED', 'Replay lease expired; retry from the durable checkpoint', { 'Retry-After': '1' })
      }
    })
  }
  async release(lease: ReplayLease | undefined): Promise<void> {
    if (lease) await this.guard.releaseReplayPins(lease.id)
  }
}
