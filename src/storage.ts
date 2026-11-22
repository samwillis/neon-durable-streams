import { randomUUID } from 'node:crypto'
import type { Pool, PoolClient } from 'pg'
import { chunkDigest, decodeChunk, encodeChunk } from './chunk-codec.js'
import type { ChunkFrame } from './chunk-codec.js'
import { StorageCompatibilityError } from './errors.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'

/** Immutable-key object storage. Destructive deletion is deliberately unavailable. */
export interface ChunkStorage {
  /** Stable identity of this branch/bucket/namespace, excluding credentials. */
  readonly identity: string
  readonly put: (key: string, bytes: Uint8Array) => Promise<void>
  readonly get: (key: string, maxBytes: number) => Promise<Buffer>
}
export interface StorageOptions {
  readonly objects: ChunkStorage
  readonly targetChunkBytes?: number
  readonly maxChunkFrames?: number
  readonly workerLeaseMs?: number
}
interface Manifest {
  id: string
  stream_id: string
  start_offset: string
  end_offset: string
  state: 'frozen' | 'sealed'
  frame_count: number
  payload_bytes: string
  claim_token: string
  claim_epoch: string
  object_key: string | null
  sha256: string | null
  encoded_bytes: number | null
}
export interface StoredFrame {
  readonly stream_id: string
  readonly start_offset: string
  readonly end_offset: string
  readonly payload: Buffer | null
  readonly payload_bytes: number
  readonly message_ends: number[] | null
  readonly chunk_id: string | null
  readonly object_key: string | null
  readonly sha256: string | null
  readonly encoded_bytes: number | null
}

/** Durable claims and immutable FROZEN frames make upload retries deterministic. */
export class ChunkManager {
  private readonly schema: string
  private readonly target: number
  private readonly maximumFrames: number
  private readonly lease: number
  private installationId?: string
  constructor(private readonly pool: Pool, schema: string, private readonly options: StorageOptions) {
    this.schema = validateSchema(schema)
    this.target = options.targetChunkBytes ?? 4 * 1024 * 1024
    this.maximumFrames = options.maxChunkFrames ?? 128
    this.lease = options.workerLeaseMs ?? 120_000
    if (!Number.isSafeInteger(this.target) || this.target < 1 || this.target > 16 * 1024 * 1024
      || !Number.isSafeInteger(this.maximumFrames) || this.maximumFrames < 1 || this.maximumFrames > 1024
      || !Number.isSafeInteger(this.lease) || this.lease < 1000 || this.lease > 300_000
      || !options.objects.identity || options.objects.identity.length > 2048) throw new RangeError('Invalid storage options')
  }
  async assertBinding(): Promise<void> {
    const result = await this.pool.query<{ id: string; storage_identity: string | null }>(`SELECT id, storage_identity FROM ${this.schema}.installation`)
    if (result.rows.length !== 1 || result.rows[0]!.storage_identity !== this.options.objects.identity) throw new StorageCompatibilityError('Run explicit migrations with the matching storageIdentity before enabling sealed storage')
    this.installationId = result.rows[0]!.id
  }
  /** Caller holds the stream lock; rotation never performs network I/O. */
  async freeze(client: PoolClient, streamId: string, force = false): Promise<boolean> {
    if (!force) {
      const head = await client.query<{ hot_bytes: string; hot_frames: number }>(`SELECT hot_bytes, hot_frames FROM ${this.schema}.streams WHERE id = $1`, [streamId])
      if (!head.rows.length || (BigInt(head.rows[0]!.hot_bytes) < BigInt(this.target) && head.rows[0]!.hot_frames < this.maximumFrames)) return false
    }
    const frames = await client.query<{ start_offset: string; end_offset: string; payload_bytes: number }>(`SELECT start_offset, end_offset, payload_bytes FROM ${this.schema}.appends
      WHERE stream_id = $1 AND chunk_id IS NULL ORDER BY end_offset LIMIT $2`, [streamId, this.maximumFrames])
    if (!frames.rows.length) return false
    // Bound an old installation's backlog as well as fresh append-time rotations.
    const selected = []
    let bytes = 0
    for (const frame of frames.rows) { selected.push(frame); bytes += frame.payload_bytes; if (bytes >= this.target) break }
    const first = selected[0]!, last = selected[selected.length - 1]!
    const id = randomUUID()
    await client.query(`INSERT INTO ${this.schema}.chunks (id, stream_id, start_offset, end_offset, state, frame_count, payload_bytes)
      VALUES ($1, $2, $3, $4, 'frozen', $5, $6)`, [id, streamId, first.start_offset, last.end_offset, selected.length, bytes])
    const assigned = await client.query(`UPDATE ${this.schema}.appends SET chunk_id = $2 WHERE stream_id = $1 AND chunk_id IS NULL AND end_offset <= $3`, [streamId, id, last.end_offset])
    if (assigned.rowCount !== selected.length) throw new Error('Hot chunk rotation invariant failed')
    await client.query(`UPDATE ${this.schema}.streams SET hot_bytes = hot_bytes - $2, hot_frames = hot_frames - $3, seal_after = clock_timestamp() WHERE id = $1`, [streamId, bytes, selected.length])
    return true
  }

  async maintain(input: { readonly maxChunks?: number } = {}): Promise<{ frozen: number; sealed: number }> {
    const maximum = input.maxChunks ?? 8
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid maintenance batch')
    await this.assertBinding()
    let frozen = 0, sealed = 0
    for (let i = 0; i < maximum; i++) {
      const rotated = await transaction(this.pool, {}, async (client) => {
        const stream = await client.query<{ id: string }>(`SELECT id FROM ${this.schema}.streams WHERE hot_frames > 0 ORDER BY seal_after, id LIMIT 1 FOR UPDATE SKIP LOCKED`)
        return stream.rows[0] ? this.freeze(client, stream.rows[0].id, true) : false
      })
      if (rotated) frozen++
      const claim = await transaction(this.pool, {}, async (client) => {
        const candidate = await client.query<{ id: string }>(`SELECT id FROM ${this.schema}.chunks WHERE state = 'frozen'
          AND (claim_until IS NULL OR claim_until <= clock_timestamp()) ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`)
        if (!candidate.rows[0]) return undefined
        const result = await client.query<Manifest>(`UPDATE ${this.schema}.chunks SET claim_token = $2,
          claim_epoch = claim_epoch + 1, claim_until = clock_timestamp() + $3 * interval '1 millisecond', attempts = attempts + 1
          WHERE id = $1 RETURNING *`, [candidate.rows[0].id, randomUUID(), this.lease])
        return result.rows[0]!
      })
      if (!claim) { if (!rotated) break; continue }
      try {
        const source = await this.pool.query<{ start_offset: string; end_offset: string; payload: Buffer; message_ends: number[] | null }>(`SELECT start_offset, end_offset, payload, message_ends FROM ${this.schema}.appends WHERE chunk_id = $1 ORDER BY end_offset`, [claim.id])
        const identity = { installationId: this.installationId!, streamId: claim.stream_id, chunkId: claim.id }
        const frames = source.rows.map((frame) => ({ start: frame.start_offset, end: frame.end_offset, payload: frame.payload, messageEnds: frame.message_ends }))
        if (frames.length !== claim.frame_count || frames[0]?.start !== claim.start_offset || frames.at(-1)?.end !== claim.end_offset
          || BigInt(frames.reduce((sum, frame) => sum + (frame.payload?.length ?? 0), 0)) !== BigInt(claim.payload_bytes)) throw new Error('Frozen manifest mismatch')
        const bytes = encodeChunk({ ...identity, frames })
        const digest = chunkDigest(bytes)
        const key = `v1/${identity.installationId}/${identity.streamId}/${identity.chunkId}/${digest}.ndsc`
        await this.options.objects.put(key, bytes)
        // Verify the exact uploaded object before discarding any local payload.
        decodeChunk(await this.options.objects.get(key, bytes.length), identity, digest)
        const published = await transaction(this.pool, {}, async (client) => {
          const result = await client.query(`UPDATE ${this.schema}.chunks SET state = 'sealed', object_key = $4, sha256 = $5,
            encoded_bytes = $6, sealed_at = clock_timestamp(), claim_until = NULL, claim_token = NULL
            WHERE id = $1 AND claim_token = $2 AND claim_epoch = $3 AND state = 'frozen' AND claim_until > clock_timestamp() RETURNING id`,
          [claim.id, claim.claim_token, claim.claim_epoch, key, digest, bytes.length])
          if (!result.rowCount) return false
          const cleared = await client.query(`UPDATE ${this.schema}.appends SET payload = NULL, message_ends = NULL WHERE chunk_id = $1 AND payload IS NOT NULL`, [claim.id])
          if (cleared.rowCount !== claim.frame_count) throw new Error('Chunk publication frame mismatch')
          return true
        })
        if (published) sealed++
      } catch (error) {
        // A failed/ambiguous publication can be retried only by obtaining a new
        // durable claim. This conditional release cannot unlock another worker.
        await transaction(this.pool, {}, async (client) => {
          await client.query(`UPDATE ${this.schema}.chunks SET claim_until = NULL, claim_token = NULL
            WHERE id = $1 AND claim_token = $2 AND claim_epoch = $3 AND state = 'frozen'`, [claim.id, claim.claim_token, claim.claim_epoch])
        }).catch(() => {})
        throw error
      }
    }
    return { frozen, sealed }
  }

  /** The SQL snapshot is already closed; immutable objects cannot be reclaimed. */
  async materialize(frames: readonly StoredFrame[]): Promise<readonly ChunkFrame[]> {
    let currentChunkId: string | undefined
    let decoded: readonly ChunkFrame[] | undefined
    const result: ChunkFrame[] = []
    for (const frame of frames) {
      if (frame.payload !== null) { result.push({ start: frame.start_offset, end: frame.end_offset, payload: frame.payload, messageEnds: frame.message_ends }); continue }
      if (!frame.chunk_id || !frame.object_key || !frame.sha256 || !frame.encoded_bytes) throw new Error('Sealed frame is missing its manifest')
      if (currentChunkId !== frame.chunk_id || !decoded) {
        // A copied response page must not retain whole chunk buffers while
        // another object is fetched or a slow reader holds the page.
        decoded = undefined
        const identity = { installationId: this.installationId!, streamId: frame.stream_id, chunkId: frame.chunk_id }
        const bytes = await this.options.objects.get(frame.object_key, frame.encoded_bytes)
        if (bytes.length !== frame.encoded_bytes) throw new Error('Sealed object length differs from manifest')
        decoded = decodeChunk(bytes, identity, frame.sha256).frames
        currentChunkId = frame.chunk_id
      }
      const found = decoded.find((value) => value.start === frame.start_offset && value.end === frame.end_offset)
      if (!found || found.payload.length !== frame.payload_bytes) throw new Error('Sealed object differs from frame index')
      result.push({ ...found, payload: Buffer.from(found.payload) })
    }
    return result
  }
}
