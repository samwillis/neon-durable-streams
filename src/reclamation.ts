import { randomUUID } from 'node:crypto'
import type { PoolClient } from 'pg'
import { ChunkIntegrityError, decodeChunk, MAX_CHUNK_BYTES } from './chunk-codec.js'
import { StorageCompatibilityError } from './errors.js'
import type { InstallationGuard } from './installation.js'
import type { ChunkStorage } from './storage.js'
import { ChunkNotFoundError } from './storage.js'

export interface ReclamationResult { reclaimedChunks: number; deletedObjects: number; prunedRows: number; inventoriedObjects: number }
export const emptyReclamation = (): ReclamationResult => ({ reclaimedChunks: 0, deletedObjects: 0, prunedRows: 0, inventoriedObjects: 0 })
interface ObjectRow {
  object_key: string; stream_id: string; chunk_id: string; sha256: string; encoded_bytes: number
  claim_token: string; claim_epoch: string
}
const UUID = '[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
const KEY = new RegExp(`^v1/(${UUID})/(${UUID})/(${UUID})/([0-9a-f]{64})\\.ndsc$`)
const BATCH = 128
const CLAIM_MS = 120_000

/** Durable phase rotation is shared across instances, including one-round hints.
 * Every object operation runs outside SQL. Only explicit operator policy enables it. */
export class ReclamationManager {
  constructor(private readonly schema: string, private readonly guard: InstallationGuard, private readonly objects?: ChunkStorage) {}
  private capabilities(enabled: boolean): void {
    if (enabled && this.objects && (!this.objects.delete || !this.objects.list)) throw new StorageCompatibilityError('Enabled reclamation requires bounded object inventory and idempotent deletion')
  }
  async assertReady(): Promise<void> {
    const enabled = await this.guard.run({ readOnly: true }, async (client) => (await client.query<{ enabled: boolean }>(`SELECT enabled FROM ${this.schema}.reclamation_policy`)).rows[0]!.enabled)
    this.capabilities(enabled)
  }
  private horizon(): string { return `(SELECT (recovery_window_ms+safety_margin_ms)*interval '1 millisecond' FROM ${this.schema}.reclamation_policy)` }
  private unreferenced(alias = 's'): string {
    return `${alias}.deleted_at IS NOT NULL AND ${alias}.ref_count=0 AND NOT ${alias}.edge_active AND NOT ${alias}.cleanup_pending
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.paths p WHERE p.active_stream_id=${alias}.id)`
  }
  private noSubscriptionRefs(id: string): string {
    return `NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_members m WHERE m.stream_id=${id})
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_wake_streams w WHERE w.stream_id=${id})
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_pending_links l WHERE l.stream_id=${id})`
  }
  async maintain(): Promise<ReclamationResult & { worked: boolean }> {
    const result = emptyReclamation()
    const phase = await this.guard.run({}, async (client) => {
      const policy = (await client.query<{ enabled: boolean; work_phase: number }>(`SELECT enabled,work_phase FROM ${this.schema}.reclamation_policy FOR UPDATE`)).rows[0]!
      if (!policy.enabled) return undefined
      this.capabilities(policy.enabled)
      await client.query(`UPDATE ${this.schema}.reclamation_policy SET work_phase=(work_phase+1)%6`)
      return policy.work_phase
    })
    if (phase === undefined) return { ...result, worked: false }
    switch (phase) {
      case 0: result.reclaimedChunks = await this.retire(); break
      case 1: result.deletedObjects = await this.collect(); break
      case 2: result.prunedRows = await this.pruneChunk(); break
      case 3: result.inventoriedObjects = await this.inventory(); break
      case 4: await this.verifyOrphan(); break
      case 5: result.prunedRows = await this.pruneMetadata(); break
    }
    // Even an empty phase must not end an unbudgeted round before other phases.
    return { ...result, worked: true }
  }
  private async retire(): Promise<number> {
    return this.guard.run({}, async (client) => {
      const stream = (await client.query<{ id: string }>(`SELECT s.id FROM ${this.schema}.streams s WHERE ${this.unreferenced()}
        AND (s.reclaim_after IS NULL OR EXISTS (SELECT 1 FROM ${this.schema}.chunks c WHERE c.stream_id=s.id AND c.retired_at IS NULL))
        ORDER BY s.deleted_at,s.id LIMIT 1 FOR UPDATE OF s SKIP LOCKED`)).rows[0]
      if (!stream) return 0
      await client.query(`UPDATE ${this.schema}.streams SET reclaim_after=COALESCE(reclaim_after,clock_timestamp()+${this.horizon()}) WHERE id=$1`, [stream.id])
      const chunks = await client.query<{ id: string; object_key: string | null; sha256: string | null; encoded_bytes: number | null }>(`SELECT id,object_key,sha256,encoded_bytes
        FROM ${this.schema}.chunks WHERE stream_id=$1 AND retired_at IS NULL ORDER BY id LIMIT 16 FOR UPDATE`, [stream.id])
      for (const chunk of chunks.rows) {
        // Backfill pre-0006 SEALED manifests, which have no upload-intent row.
        if (chunk.object_key) await client.query(`INSERT INTO ${this.schema}.chunk_objects (object_key,stream_id,chunk_id,sha256,encoded_bytes)
          VALUES ($1,$2,$3,$4,$5) ON CONFLICT (object_key) DO NOTHING`, [chunk.object_key, stream.id, chunk.id, chunk.sha256, chunk.encoded_bytes])
        await client.query(`UPDATE ${this.schema}.chunks SET retired_at=clock_timestamp(),collect_after=clock_timestamp()+${this.horizon()},
          claim_token=NULL,claim_until=NULL,claim_epoch=claim_epoch+1 WHERE id=$1`, [chunk.id])
        await client.query(`UPDATE ${this.schema}.chunk_objects SET retired_at=COALESCE(retired_at,clock_timestamp()),
          collect_after=GREATEST(collect_after,clock_timestamp()+${this.horizon()}),orphan_candidate=false
          WHERE chunk_id=$1`, [chunk.id])
      }
      return chunks.rows.length
    })
  }
  private async claim(orphan: boolean): Promise<ObjectRow | undefined> {
    return this.guard.run({}, async (client) => {
      const candidate = (await client.query<{ object_key: string }>(`SELECT o.object_key FROM ${this.schema}.chunk_objects o
        WHERE o.quarantine_reason IS NULL AND o.next_attempt_at<=clock_timestamp() AND (o.claim_until IS NULL OR o.claim_until<=clock_timestamp())
        AND ${orphan ? `o.orphan_candidate AND o.retired_at IS NULL` : `NOT o.orphan_candidate AND o.retired_at IS NOT NULL AND o.collected_at IS NULL
          AND o.collect_after<=clock_timestamp() AND (SELECT hold_until<=clock_timestamp() FROM ${this.schema}.reclamation_policy)
          AND NOT EXISTS (SELECT 1 FROM ${this.schema}.chunks c WHERE c.id=o.chunk_id AND c.retired_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM ${this.schema}.replay_pins p WHERE p.chunk_id=o.chunk_id AND p.expires_at>clock_timestamp())`}
        ORDER BY ${orphan ? 'o.next_attempt_at,o.created_at' : 'o.collect_after'},o.object_key LIMIT 1 FOR UPDATE OF o SKIP LOCKED`)).rows[0]
      if (!candidate) return undefined
      return (await client.query<ObjectRow>(`UPDATE ${this.schema}.chunk_objects SET claim_token=$2,claim_epoch=claim_epoch+1,
        claim_until=clock_timestamp()+$3*interval '1 millisecond' WHERE object_key=$1 RETURNING *`, [candidate.object_key, randomUUID(), CLAIM_MS])).rows[0]!
    })
  }
  private async release(claim: ObjectRow): Promise<void> {
    await this.guard.run({}, async (client) => {
      await client.query(`UPDATE ${this.schema}.chunk_objects SET claim_token=NULL,claim_until=NULL,next_attempt_at=clock_timestamp()+interval '5 seconds'
        WHERE object_key=$1 AND claim_token=$2 AND claim_epoch=$3`, [claim.object_key, claim.claim_token, claim.claim_epoch])
    }).catch(() => {})
  }
  private async collect(): Promise<number> {
    if (!this.objects) return 0
    const claim = await this.claim(false)
    if (!claim) return 0
    try {
      await this.guard.check()
      await this.objects.delete!(claim.object_key)
      return await this.guard.run({}, async (client) => {
        const removed = await client.query(`UPDATE ${this.schema}.chunk_objects SET collected_at=clock_timestamp(),claim_token=NULL,claim_until=NULL
          WHERE object_key=$1 AND claim_token=$2 AND claim_epoch=$3 AND claim_until>clock_timestamp() AND retired_at IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM ${this.schema}.replay_pins p WHERE p.chunk_id=chunk_objects.chunk_id AND p.expires_at>clock_timestamp())`,
        [claim.object_key, claim.claim_token, claim.claim_epoch])
        return removed.rowCount ?? 0
      })
    } catch (error) { await this.release(claim); throw error }
  }
  private async inventory(): Promise<number> {
    if (!this.objects) return 0
    const before = await this.guard.run({ readOnly: true }, async (client) => (await client.query<{ inventory_cursor: string | null }>(`SELECT inventory_cursor FROM ${this.schema}.reclamation_policy
      WHERE inventory_cursor IS NOT NULL OR inventory_completed_at IS NULL OR inventory_completed_at<clock_timestamp()-interval '5 minutes'`)).rows[0])
    if (!before) return 0
    const identity = await this.guard.snapshot(), prefix = `v1/${identity.id}/`
    const page = await this.objects.list!(prefix, before.inventory_cursor, BATCH)
    if (page.objects.length > BATCH || (page.cursor !== null && (typeof page.cursor !== 'string' || !page.cursor || page.cursor.length > 8192 || page.cursor === before.inventory_cursor))) {
      throw new StorageCompatibilityError('Object inventory exceeded its bounded page/cursor contract')
    }
    return this.guard.run({}, async (client) => {
      const policy = (await client.query<{ inventory_cursor: string | null }>(`SELECT inventory_cursor FROM ${this.schema}.reclamation_policy FOR UPDATE`)).rows[0]!
      if (policy.inventory_cursor !== before.inventory_cursor) return 0
      for (const item of page.objects) {
        const match = KEY.exec(item.key)
        if (!match || match[1] !== identity.id || !Number.isSafeInteger(item.size) || item.size < 1 || item.size > MAX_CHUNK_BYTES) continue
        await client.query(`INSERT INTO ${this.schema}.chunk_objects (object_key,stream_id,chunk_id,sha256,encoded_bytes,orphan_candidate)
          VALUES ($1,$2,$3,$4,$5,true) ON CONFLICT (object_key) DO UPDATE SET orphan_candidate=true,retired_at=NULL,collect_after=NULL,collected_at=NULL,
          claim_token=NULL,claim_until=NULL,claim_epoch=chunk_objects.claim_epoch+1 WHERE chunk_objects.collected_at IS NOT NULL`, [item.key, match[2], match[3], match[4], item.size])
      }
      await client.query(`UPDATE ${this.schema}.reclamation_policy SET inventory_cursor=$1,
        inventory_completed_at=CASE WHEN $1::text IS NULL THEN clock_timestamp() ELSE inventory_completed_at END`, [page.cursor])
      return page.objects.length
    })
  }
  private async verifyOrphan(): Promise<void> {
    if (!this.objects) return
    const claim = await this.claim(true)
    if (!claim) return
    try {
      const identity = await this.guard.snapshot()
      let valid = true, missing = false
      try {
        const match = KEY.exec(claim.object_key)
        if (!match || match[1] !== identity.id || match[2] !== claim.stream_id || match[3] !== claim.chunk_id || match[4] !== claim.sha256) throw new ChunkIntegrityError('Invalid inventory identity')
        const bytes = await this.objects.get(claim.object_key, claim.encoded_bytes)
        if (bytes.length !== claim.encoded_bytes) throw new ChunkIntegrityError('Invalid inventory length')
        decodeChunk(bytes, { installationId: identity.id, streamId: claim.stream_id, chunkId: claim.chunk_id }, claim.sha256)
      } catch (error) {
        if (error instanceof ChunkNotFoundError) missing = true
        else if (error instanceof ChunkIntegrityError) valid = false
        else throw error
      }
      await this.guard.run({}, async (client) => {
        // Match retirement's stream -> chunk -> object order. An active chunk
        // (including a FROZEN upload) retains every validated immutable intent.
        await client.query(`SELECT id FROM ${this.schema}.streams WHERE id=$1 FOR UPDATE`, [claim.stream_id])
        const chunk = (await client.query<{ retired_at: Date | null }>(`SELECT retired_at FROM ${this.schema}.chunks WHERE id=$1 FOR UPDATE`, [claim.chunk_id])).rows[0]
        await client.query(`UPDATE ${this.schema}.chunk_objects SET orphan_candidate=false,quarantine_reason=$4,
          retired_at=CASE WHEN $5 THEN clock_timestamp() ELSE NULL END,
          collect_after=CASE WHEN $5 THEN clock_timestamp()+${this.horizon()} ELSE NULL END,
          collected_at=CASE WHEN $5 AND $6 THEN clock_timestamp() ELSE NULL END,claim_token=NULL,claim_until=NULL
          WHERE object_key=$1 AND claim_token=$2 AND claim_epoch=$3 AND claim_until>clock_timestamp()`,
        [claim.object_key, claim.claim_token, claim.claim_epoch, valid ? null : 'Invalid immutable chunk envelope', valid && (!chunk || chunk.retired_at !== null), missing])
      })
    } catch (error) { await this.release(claim); throw error }
  }
  private async pruneChunk(): Promise<number> {
    return this.guard.run({}, async (client) => {
      const chunk = (await client.query<{ id: string }>(`SELECT c.id FROM ${this.schema}.chunks c JOIN ${this.schema}.streams s ON s.id=c.stream_id
        WHERE c.retired_at IS NOT NULL AND c.collect_after<=clock_timestamp() AND ${this.unreferenced()} AND ${this.noSubscriptionRefs('s.id')}
        AND (SELECT hold_until<=clock_timestamp() FROM ${this.schema}.reclamation_policy)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.stream_changes j WHERE j.stream_id=s.id AND NOT j.processed)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.replay_pins p WHERE p.chunk_id=c.id AND p.expires_at>clock_timestamp())
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.chunk_objects o WHERE o.chunk_id=c.id AND o.collected_at IS NULL)
        ORDER BY c.retired_at,c.id LIMIT 1 FOR UPDATE OF c SKIP LOCKED`)).rows[0]
      if (!chunk) return 0
      // Keep ON DELETE CASCADE from turning one chunk into an unbounded pin
      // deletion after a period of failed reader cleanup. Valid pins already
      // exclude the candidate; locked/remaining expired pins defer its removal.
      const pins = await client.query(`WITH batch AS (SELECT id,chunk_id FROM ${this.schema}.replay_pins WHERE chunk_id=$1 AND expires_at<=clock_timestamp()
        ORDER BY expires_at,id LIMIT ${BATCH} FOR UPDATE SKIP LOCKED) DELETE FROM ${this.schema}.replay_pins p USING batch b WHERE p.id=b.id AND p.chunk_id=b.chunk_id`, [chunk.id])
      const frames = await client.query(`WITH batch AS (SELECT stream_id,end_offset FROM ${this.schema}.appends WHERE chunk_id=$1 ORDER BY end_offset LIMIT ${BATCH})
        DELETE FROM ${this.schema}.appends a USING batch b WHERE a.stream_id=b.stream_id AND a.end_offset=b.end_offset`, [chunk.id])
      const removed = await client.query(`DELETE FROM ${this.schema}.chunks c WHERE id=$1 AND NOT EXISTS (SELECT 1 FROM ${this.schema}.appends a WHERE a.chunk_id=c.id)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.replay_pins p WHERE p.chunk_id=c.id)`, [chunk.id])
      return (pins.rowCount ?? 0) + (frames.rowCount ?? 0) + (removed.rowCount ?? 0)
    })
  }
  private async pruneMetadata(): Promise<number> {
    return this.guard.run({}, async (client) => {
      const phase = (await client.query<{ metadata_phase: number }>(`SELECT metadata_phase FROM ${this.schema}.reclamation_policy FOR UPDATE`)).rows[0]!.metadata_phase
      await client.query(`UPDATE ${this.schema}.reclamation_policy SET metadata_phase=(metadata_phase+1)%8`)
      if (phase === 0) return this.pruneWake(client)
      if (phase === 1) return (await client.query(`WITH batch AS (SELECT id FROM ${this.schema}.subscriptions s WHERE deleted_at<clock_timestamp()-${this.horizon()}
        AND current_wake IS NULL AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_wakes w WHERE w.subscription_id=s.id)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_members m WHERE m.subscription_id=s.id)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_pending_links l WHERE l.subscription_id=s.id)
        ORDER BY deleted_at,id LIMIT ${BATCH} FOR UPDATE SKIP LOCKED) DELETE FROM ${this.schema}.subscriptions s USING batch b WHERE s.id=b.id`)).rowCount ?? 0
      if (phase === 2) return this.pruneOutbox(client)
      if (phase === 3 || phase === 4) return this.pruneGeneration(client, phase === 4)
      if (phase === 5) return (await client.query(`WITH batch AS (SELECT p.tenant_id,p.path FROM ${this.schema}.paths p WHERE active_stream_id IS NULL
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.streams s WHERE s.tenant_id=p.tenant_id AND s.path=p.path)
        ORDER BY p.tenant_id,p.path LIMIT ${BATCH} FOR UPDATE OF p SKIP LOCKED)
        DELETE FROM ${this.schema}.paths p USING batch b WHERE p.tenant_id=b.tenant_id AND p.path=b.path`)).rowCount ?? 0
      if (phase === 6) return (await client.query(`WITH batch AS (SELECT object_key FROM ${this.schema}.chunk_objects WHERE collected_at<clock_timestamp()-${this.horizon()}
        AND claim_token IS NULL ORDER BY collected_at,object_key LIMIT ${BATCH} FOR UPDATE SKIP LOCKED)
        DELETE FROM ${this.schema}.chunk_objects o USING batch b WHERE o.object_key=b.object_key`)).rowCount ?? 0
      return (await client.query(`WITH batch AS (SELECT id,chunk_id FROM ${this.schema}.replay_pins WHERE expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT ${BATCH} FOR UPDATE SKIP LOCKED)
        DELETE FROM ${this.schema}.replay_pins p USING batch b WHERE p.id=b.id AND p.chunk_id=b.chunk_id`)).rowCount ?? 0
    })
  }
  private async pruneWake(client: PoolClient): Promise<number> {
    const candidate = (await client.query<{ id: string; subscription_id: string }>(`SELECT w.id,w.subscription_id FROM ${this.schema}.subscription_wakes w
      JOIN ${this.schema}.subscriptions s ON s.id=w.subscription_id WHERE w.state IN ('completed','expired','fenced')
      AND s.current_wake IS DISTINCT FROM w.id AND (w.delivery_until IS NULL OR w.delivery_until<=clock_timestamp())
      AND w.created_at<clock_timestamp()-${this.horizon()} ORDER BY w.created_at,w.id LIMIT 1`)).rows[0]
    if (!candidate) return 0
    const sub = await client.query(`SELECT id FROM ${this.schema}.subscriptions WHERE id=$1 AND current_wake IS DISTINCT FROM $2 FOR UPDATE SKIP LOCKED`, [candidate.subscription_id, candidate.id])
    if (!sub.rowCount) return 0
    const snapshots = await client.query(`WITH batch AS (SELECT wake_id,path FROM ${this.schema}.subscription_wake_streams WHERE wake_id=$1 ORDER BY path LIMIT ${BATCH})
      DELETE FROM ${this.schema}.subscription_wake_streams w USING batch b WHERE w.wake_id=b.wake_id AND w.path=b.path`, [candidate.id])
    await client.query(`INSERT INTO ${this.schema}.legacy_wake_tokens (wake_id,subscription_id,subscription_name,token_hash)
      SELECT w.id,w.subscription_id,s.name,w.token_hash FROM ${this.schema}.subscription_wakes w JOIN ${this.schema}.subscriptions s ON s.id=w.subscription_id
      WHERE w.id=$1 AND w.token_version=1 AND w.token_hash IS NOT NULL ON CONFLICT DO NOTHING`, [candidate.id])
    const wakes = await client.query(`DELETE FROM ${this.schema}.subscription_wakes w WHERE id=$1 AND state IN ('completed','expired','fenced')
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.subscription_wake_streams x WHERE x.wake_id=w.id)`, [candidate.id])
    return (snapshots.rowCount ?? 0) + (wakes.rowCount ?? 0)
  }
  private async pruneOutbox(client: PoolClient): Promise<number> {
    const candidate = (await client.query<{ tenant_id: string }>(`SELECT s.tenant_id FROM ${this.schema}.stream_changes c JOIN ${this.schema}.streams s ON s.id=c.stream_id
      WHERE c.created_at<clock_timestamp()-${this.horizon()} AND (c.processed OR NOT EXISTS (SELECT 1 FROM ${this.schema}.subscriptions x WHERE x.tenant_id=s.tenant_id AND x.deleted_at IS NULL))
      ORDER BY c.created_at,c.stream_id,c.version LIMIT 1`)).rows[0]
    if (!candidate) return 0
    await client.query(`SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, [`durable-streams:subscription-registry:${this.schema}:${candidate.tenant_id}`])
    return (await client.query(`WITH batch AS (SELECT c.stream_id,c.version FROM ${this.schema}.stream_changes c JOIN ${this.schema}.streams s ON s.id=c.stream_id
      WHERE s.tenant_id=$1 AND c.created_at<clock_timestamp()-${this.horizon()} AND (c.processed OR NOT EXISTS (SELECT 1 FROM ${this.schema}.subscriptions x WHERE x.tenant_id=s.tenant_id AND x.deleted_at IS NULL))
      ORDER BY c.created_at,c.stream_id,c.version LIMIT ${BATCH} FOR UPDATE OF c SKIP LOCKED)
      DELETE FROM ${this.schema}.stream_changes c USING batch b WHERE c.stream_id=b.stream_id AND c.version=b.version`, [candidate.tenant_id])).rowCount ?? 0
  }
  private async pruneGeneration(client: PoolClient, final: boolean): Promise<number> {
    const stream = (await client.query<{ id: string }>(`SELECT s.id FROM ${this.schema}.streams s WHERE ${this.unreferenced()} AND ${this.noSubscriptionRefs('s.id')}
      AND s.reclaim_after<=clock_timestamp() AND (SELECT hold_until<=clock_timestamp() FROM ${this.schema}.reclamation_policy)
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.chunks c WHERE c.stream_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM ${this.schema}.stream_changes j WHERE j.stream_id=s.id)
      ${final ? `AND NOT EXISTS (SELECT 1 FROM ${this.schema}.appends a WHERE a.stream_id=s.id)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.producers p WHERE p.stream_id=s.id)
        AND NOT EXISTS (SELECT 1 FROM ${this.schema}.streams child WHERE child.source_id=s.id)` : `AND (EXISTS (SELECT 1 FROM ${this.schema}.appends a WHERE a.stream_id=s.id) OR EXISTS (SELECT 1 FROM ${this.schema}.producers p WHERE p.stream_id=s.id))`}
      ORDER BY s.deleted_at,s.id LIMIT 1 FOR UPDATE OF s SKIP LOCKED`)).rows[0]
    if (!stream) return 0
    if (final) {
      await client.query(`DELETE FROM ${this.schema}.stream_expirations WHERE stream_id=$1`, [stream.id])
      return (await client.query(`DELETE FROM ${this.schema}.streams WHERE id=$1`, [stream.id])).rowCount ?? 0
    }
    const frames = await client.query(`WITH batch AS (SELECT end_offset FROM ${this.schema}.appends WHERE stream_id=$1 ORDER BY end_offset LIMIT ${BATCH})
      DELETE FROM ${this.schema}.appends a USING batch b WHERE a.stream_id=$1 AND a.end_offset=b.end_offset`, [stream.id])
    const producers = await client.query(`WITH batch AS (SELECT producer_id FROM ${this.schema}.producers WHERE stream_id=$1 ORDER BY producer_id LIMIT ${BATCH})
      DELETE FROM ${this.schema}.producers p USING batch b WHERE p.stream_id=$1 AND p.producer_id=b.producer_id`, [stream.id])
    return (frames.rowCount ?? 0) + (producers.rowCount ?? 0)
  }
}
