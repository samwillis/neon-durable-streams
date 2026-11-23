import { randomUUID } from 'node:crypto'
import type { Pool, PoolClient } from 'pg'
import { chunkDigest, decodeChunk, ChunkIntegrityError } from './chunk-codec.js'
import type { ChunkFrame } from './chunk-codec.js'
import { encodeBlockObject, decodeBlock, unpackBlockDirectory } from './block-codec.js'
import { StorageCompatibilityError } from './errors.js'
import { validateSchema } from './path.js'
import { InstallationGuard } from './installation.js'
import { assertReplayDeadline, replayPipeline } from './replay.js'
import { VerifiedChunkCache } from './chunk-cache.js'
import { isSuccessor, parseOffset } from './offset.js'
import type { StoredFrame, ObjectRange } from './replay-plan.js'

export interface ChunkInventoryPage {
  readonly objects: readonly { readonly key: string; readonly size: number }[]
  readonly cursor: string | null
}
export class ChunkNotFoundError extends Error { override readonly name = 'ChunkNotFoundError' }
/** Half-open physical interval in one immutable object of the stated size. */
export interface ChunkByteRange {
  readonly start: number
  readonly end: number
  readonly totalBytes: number
}

/** Immutable-key storage. Collection capabilities are used only by enabled policy. */
export interface ChunkStorage {
  /** Stable identity of this branch/bucket/namespace, excluding credentials. */
  readonly identity: string
  readonly put: (key: string, bytes: Uint8Array) => Promise<void>
  /** Transfers ownership of a buffer that the adapter will never mutate or reuse. */
  readonly get: (key: string, maxBytes: number, signal?: AbortSignal) => Promise<Buffer>
  readonly getRange: (key: string, range: ChunkByteRange, signal?: AbortSignal) => Promise<Buffer>
  /** Delivers ordered fragments under backpressure without buffering the range. */
  readonly readRange?: ((key: string, range: ChunkByteRange, consume: (bytes: Uint8Array) => void | Promise<void>, signal?: AbortSignal) => Promise<void>) | undefined
  readonly delete?: (key: string) => Promise<void>
  readonly list?: (prefix: string, cursor: string | null, limit: number) => Promise<ChunkInventoryPage>
}
export interface StorageOptions {
  readonly objects: ChunkStorage
  readonly targetChunkBytes?: number
  readonly maxChunkFrames?: number
  readonly workerLeaseMs?: number
  /** Maximum hot residency from the first append, independent of cron cadence. */
  readonly maxChunkAgeMs?: number
  /** Per-server verified-object cache budget; zero disables retention. */
  readonly cacheBytes?: number
  /** Encoded byte target for independently verified object ranges. */
  readonly blockBytes?: number
  /** At most one following object/range may overlap the current read. */
  readonly readAhead?: 0 | 1
}
interface FrozenClaim {
  readonly id: string
  readonly stream_id: string
  readonly start_offset: string
  readonly end_offset: string
  readonly state: 'frozen'
  readonly frame_count: number
  readonly payload_bytes: string
  readonly claim_token: string
  readonly claim_epoch: string
}
interface PreparedChunk {
  readonly claim: FrozenClaim
  readonly identity: { readonly installationId: string; readonly streamId: string; readonly chunkId: string }
  readonly bytes: Buffer
  readonly blockDirectory: Buffer
  readonly digest: string
  readonly key: string
}
type VerifiedChunk = PreparedChunk

/** Durable claims and immutable FROZEN frames make upload retries deterministic. */
export class ChunkManager {
  private readonly schema: string
  private readonly target: number
  private readonly maximumFrames: number
  private readonly blockBytes: number
  private readonly readAhead: 0 | 1
  private readonly lease: number
  readonly maxAgeMs: number
  private readonly cache: VerifiedChunkCache
  private installationId?: string
  close(): void { this.cache.close() }
  constructor(private readonly pool: Pool, schema: string, private readonly options: StorageOptions, private readonly guard = new InstallationGuard(pool, schema)) {
    this.schema = validateSchema(schema)
    this.target = options.targetChunkBytes ?? 4 * 1024 * 1024
    this.maximumFrames = options.maxChunkFrames ?? 131072
    this.blockBytes = options.blockBytes ?? 65536
    this.readAhead = options.readAhead ?? 0
    this.lease = options.workerLeaseMs ?? 120_000
    this.maxAgeMs = options.maxChunkAgeMs ?? 3_600_000
    this.cache = new VerifiedChunkCache(options.cacheBytes ?? 32 * 1024 * 1024)
    if (!Number.isSafeInteger(this.target) || this.target < 1 || this.target > 16 * 1024 * 1024
      || !Number.isSafeInteger(this.maximumFrames) || this.maximumFrames < 1 || this.maximumFrames > 131072
      || !Number.isSafeInteger(this.blockBytes) || this.blockBytes < 4096 || this.blockBytes > 16 * 1024 * 1024
      || (this.readAhead !== 0 && this.readAhead !== 1)
      || !Number.isSafeInteger(this.lease) || this.lease < 1000 || this.lease > 300_000
      || !Number.isSafeInteger(this.maxAgeMs) || this.maxAgeMs < 1 || this.maxAgeMs > 604_800_000
      || !options.objects.identity || options.objects.identity.length > 2048) throw new RangeError('Invalid storage options')
  }
  async assertBinding(): Promise<void> {
    await this.guard.check()
    const result = await this.pool.query<{ id: string; storage_identity: string | null }>(`SELECT id, storage_identity FROM ${this.schema}.installation`)
    if (result.rows.length !== 1 || result.rows[0]!.storage_identity !== this.options.objects.identity) throw new StorageCompatibilityError('Run explicit migrations with the matching storageIdentity before enabling sealed storage')
    this.installationId = result.rows[0]!.id
  }
  /** Caller holds the stream lock; rotation never performs network I/O. */
  async freeze(client: PoolClient, streamId: string, force = false): Promise<boolean> {
    const head = (await client.query<{ hot_bytes: string; hot_frames: number; hot_start_offset: string }>(`SELECT hot_bytes,hot_frames,hot_start_offset FROM ${this.schema}.streams WHERE id=$1`, [streamId])).rows[0]
    if (!head || !head.hot_frames || (!force && BigInt(head.hot_bytes)<BigInt(this.target) && head.hot_frames<this.maximumFrames)) return false
    const frames = await client.query<{ start_offset: string; end_offset: string; payload_bytes: number }>(`SELECT start_offset, end_offset, payload_bytes FROM ${this.schema}.appends
      WHERE stream_id=$1 AND end_offset>$3 ORDER BY end_offset LIMIT $2`, [streamId, this.maximumFrames,head.hot_start_offset])
    if (!frames.rows.length) throw new Error('Hot chunk counters have no staging range')
    // Bound an old installation's backlog as well as fresh append-time rotations.
    const selected = []
    let bytes = 0
    for (const frame of frames.rows) {
      if (selected.length && parseOffset(frame.end_offset).history !== parseOffset(selected[0]!.end_offset).history) break
      selected.push(frame); bytes += frame.payload_bytes
      if (bytes >= this.target) break
    }
    const first = selected[0]!, last = selected[selected.length - 1]!
    let previous=head.hot_start_offset
    for (const frame of selected) {
      if (frame.start_offset!==previous || !isSuccessor(previous,frame.end_offset)) throw new Error('Hot staging range is not contiguous')
      previous=frame.end_offset
    }
    if (selected.length>head.hot_frames || BigInt(bytes)>BigInt(head.hot_bytes)) throw new Error('Hot staging range differs from counters')
    const id = randomUUID()
    await client.query(`INSERT INTO ${this.schema}.chunks (id, stream_id, start_offset, end_offset, state, frame_count, payload_bytes)
      VALUES ($1, $2, $3, $4, 'frozen', $5, $6)`, [id, streamId, first.start_offset, last.end_offset, selected.length, bytes])
    await client.query(`UPDATE ${this.schema}.streams SET hot_bytes=hot_bytes-$2,hot_frames=hot_frames-$3,hot_start_offset=$4 WHERE id=$1`, [streamId,bytes,selected.length,last.end_offset])
    await client.query(`DELETE FROM ${this.schema}.hot_chunks h USING ${this.schema}.streams s WHERE h.stream_id=$1 AND s.id=h.stream_id AND s.hot_frames=0`, [streamId])
    return true
  }

  async maintain(input: { readonly maxChunks?: number; readonly forceHot?: boolean } = {}): Promise<{ frozen: number; sealed: number }> {
    const maximum = input.maxChunks ?? 8
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid maintenance batch')
    await this.assertBinding()
    let frozen = 0, sealed = 0
    for (let i = 0; i < maximum; i++) {
      const rotated = await this.guard.run({}, async (client) => {
        const stream = await client.query<{ id: string }>(`SELECT s.id FROM ${this.schema}.hot_chunks h JOIN ${this.schema}.streams s ON s.id=h.stream_id
          WHERE ${input.forceHot ? 'true' : 'h.due_at<=clock_timestamp()'} AND (s.deleted_at IS NULL OR s.ref_count > 0)
          ORDER BY h.due_at,h.stream_id LIMIT 1 FOR NO KEY UPDATE OF s SKIP LOCKED`)
        return stream.rows[0] ? this.freeze(client, stream.rows[0].id, true) : false
      })
      if (rotated) frozen++
      const claim = await this.claimFrozen()
      if (!claim) { if (!rotated) break; continue }
      try {
        const prepared = await this.prepare(claim)
        if (!await this.recordIntent(prepared)) continue
        const verified = await this.uploadAndVerify(prepared)
        if (await this.publish(verified)) sealed++
      } catch (error) {
        await this.releaseClaim(claim)
        throw error
      }
    }
    return { frozen, sealed }
  }

  private async claimFrozen(): Promise<FrozenClaim | undefined> {
    return this.guard.run({}, async (client) => {
      const candidate = await client.query<{ id: string }>(`SELECT id FROM ${this.schema}.chunks WHERE state = 'frozen'
        AND retired_at IS NULL
        AND (claim_until IS NULL OR claim_until <= clock_timestamp()) ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`)
      if (!candidate.rows[0]) return undefined
      const result = await client.query<FrozenClaim>(`UPDATE ${this.schema}.chunks SET claim_token = $2,
        claim_epoch = claim_epoch + 1, claim_until = clock_timestamp() + $3 * interval '1 millisecond', attempts = attempts + 1
        WHERE id = $1 RETURNING *`, [candidate.rows[0].id, randomUUID(), this.lease])
      return result.rows[0]!
    })
  }
  private async prepare(claim: FrozenClaim): Promise<PreparedChunk> {
    const source = await this.pool.query<{ start_offset: string; end_offset: string; payload: Buffer; message_ends: number[] | null }>(`SELECT start_offset,end_offset,payload,message_ends FROM ${this.schema}.appends
      WHERE stream_id=$1 AND end_offset>$2 AND end_offset<=$3 ORDER BY end_offset`, [claim.stream_id,claim.start_offset,claim.end_offset])
    const identity = { installationId: this.installationId!, streamId: claim.stream_id, chunkId: claim.id }
    const frames = source.rows.map((frame) => ({ start: frame.start_offset, end: frame.end_offset, payload: frame.payload, messageEnds: frame.message_ends }))
    if (frames.length !== claim.frame_count || frames[0]?.start !== claim.start_offset || frames.at(-1)?.end !== claim.end_offset
      || BigInt(frames.reduce((sum, frame) => sum + (frame.payload?.length ?? 0), 0)) !== BigInt(claim.payload_bytes)) throw new Error('Frozen manifest mismatch')
    const encoded = encodeBlockObject({ ...identity, frames }, this.blockBytes)
    const bytes = encoded.bytes
    const digest = chunkDigest(bytes)
    const key = `v1/${identity.installationId}/${identity.streamId}/${identity.chunkId}/${digest}.ndsc`
    return { claim, identity, bytes, blockDirectory: encoded.packedDirectory, digest, key }
  }
  private async recordIntent({ claim, key, digest, bytes }: PreparedChunk): Promise<boolean> {
    return this.guard.run({}, async (client) => {
      const valid = await client.query(`SELECT id FROM ${this.schema}.chunks WHERE id=$1 AND claim_token=$2 AND claim_epoch=$3
        AND state='frozen' AND retired_at IS NULL AND claim_until>clock_timestamp() FOR UPDATE`, [claim.id, claim.claim_token, claim.claim_epoch])
      if (!valid.rowCount) return false
      await client.query(`INSERT INTO ${this.schema}.chunk_objects (object_key,stream_id,chunk_id,sha256,encoded_bytes)
        VALUES ($1,$2,$3,$4,$5) ON CONFLICT (object_key) DO NOTHING`, [key, claim.stream_id, claim.id, digest, bytes.length])
      const intent = await client.query(`SELECT 1 FROM ${this.schema}.chunk_objects WHERE object_key=$1 AND retired_at IS NULL
        AND NOT orphan_candidate AND quarantine_reason IS NULL AND sha256=$2 AND encoded_bytes=$3`, [key, digest, bytes.length])
      return !!intent.rowCount
    })
  }
  /** Network I/O holds no SQL transaction. Only verified bytes may publish. */
  private async uploadAndVerify(prepared: PreparedChunk): Promise<VerifiedChunk> {
    const { claim, key, bytes, identity, digest } = prepared
    await this.options.objects.put(key, bytes)
    // Verify the exact uploaded object before discarding any local payload.
    const verified = await this.options.objects.get(key, bytes.length)
    if (verified.length !== bytes.length) throw new Error('Published object length differs')
    const frames = decodeChunk(verified, identity, digest).frames
    if (frames.length !== claim.frame_count || frames[0]?.start !== claim.start_offset || frames.at(-1)?.end !== claim.end_offset) throw new Error('Uploaded object differs from frozen range')
    return prepared
  }
  private async publish({ claim, key, digest, bytes, blockDirectory }: VerifiedChunk): Promise<boolean> {
    return this.guard.run({}, async (client) => {
      const result = await client.query(`UPDATE ${this.schema}.chunks SET state = 'sealed', object_key = $4, sha256 = $5,
        encoded_bytes = $6, block_directory=$7, sealed_at = clock_timestamp(), claim_until = NULL, claim_token = NULL
        WHERE id = $1 AND claim_token = $2 AND claim_epoch = $3 AND state = 'frozen' AND claim_until > clock_timestamp()
        AND retired_at IS NULL RETURNING id`,
      [claim.id, claim.claim_token, claim.claim_epoch, key, digest, bytes.length, blockDirectory])
      if (!result.rowCount) return false
      const cleared = await client.query(`DELETE FROM ${this.schema}.appends WHERE stream_id=$1 AND end_offset>$2 AND end_offset<=$3`, [claim.stream_id,claim.start_offset,claim.end_offset])
      if (cleared.rowCount !== claim.frame_count) throw new Error('Chunk publication frame mismatch')
      return true
    })
  }
  private async releaseClaim(claim: FrozenClaim): Promise<void> {
    // Conditional release cannot unlock a replacement worker after a lost reply.
    await this.guard.run({}, async (client) => {
      await client.query(`UPDATE ${this.schema}.chunks SET claim_until = NULL, claim_token = NULL
        WHERE id = $1 AND claim_token = $2 AND claim_epoch = $3 AND state = 'frozen'`, [claim.id, claim.claim_token, claim.claim_epoch])
    }).catch(() => {})
  }

  /** The replay owner holds its pins; object I/O holds no SQL transaction. */
  async materialize(frames: readonly StoredFrame[], deadline: number, signal?: AbortSignal): Promise<readonly ChunkFrame[]> {
    const pages = await replayPipeline(frames, async (frame, readSignal) => {
      assertReplayDeadline(deadline)
      if (frame.storage === 'postgres') return [{ start: frame.start_offset, end: frame.end_offset, payload: frame.payload, messageEnds: frame.message_ends }]
      const selected = await this.readObjectRange(frame, readSignal)
      assertReplayDeadline(deadline)
      if (selected.length !== frame.lastFrame - frame.firstFrame + 1 || selected[0]?.start !== frame.start_offset || selected.at(-1)?.end !== frame.end_offset
        || (frame.exactBytes ? selected.reduce((sum, value) => sum + value.payload.length, 0) !== frame.payload_bytes
          : selected.reduce((sum, value) => sum + value.payload.length, 0) > frame.payload_bytes)) throw new Error('Sealed object differs from planned range')
      // The store copies these private views into its response or fork prefix.
      // Avoid a temporary payload copy between verification and that boundary.
      return selected
    }, this.readAhead, signal)
    return pages.flat()
  }

  private async readObjectRange(frame: ObjectRange, signal: AbortSignal): Promise<readonly ChunkFrame[]> {
    const { manifest } = frame
    const identity = { installationId: this.installationId!, streamId: frame.stream_id, chunkId: manifest.id }
    const key = JSON.stringify([this.options.objects.identity, ...Object.values(identity), manifest.sha256, manifest.encodedBytes, chunkDigest(manifest.blockDirectory)])
    const directory = unpackBlockDirectory(manifest.blockDirectory, manifest.encodedBytes, manifest.frameCount)
    if (directory[0]!.start !== manifest.start || directory.at(-1)!.end !== manifest.end) throw new Error('Block directory differs from durable range')
    const blocks = directory.filter((block) => block.firstFrame < frame.lastFrame && block.firstFrame + block.frameCount >= frame.firstFrame)
    const cached = this.cache.peek(key)
    if (cached) return cached.frames.slice(frame.firstFrame - 1, frame.lastFrame)
    if (blocks.length === directory.length) {
      const whole = await this.cache.get(key, async (sharedSignal) => {
        const bytes = await this.options.objects.get(manifest.key, manifest.encodedBytes, sharedSignal)
        if (bytes.length !== manifest.encodedBytes) throw new Error('Sealed object length differs from manifest')
        const decoded = decodeChunk(bytes, identity, manifest.sha256)
        const packedLength = bytes.readUInt32BE(64)
        if (!bytes.subarray(68, 68 + packedLength).equals(manifest.blockDirectory)) throw new Error('Sealed object differs from its manifest')
        return decoded
      }, manifest.encodedBytes, signal)
      return whole.frames.slice(frame.firstFrame - 1, frame.lastFrame)
    }
    if (!blocks.length) throw new Error('Replay selected no object blocks')
    const requests = blocks.map((block) => ({ key: `${key}:${block.byteStart}:${block.sha256}`, encodedBytes: block.byteLength, block }))
    const byKey = new Map(requests.map((request) => [request.key, request.block]))
    const decoded = await this.cache.getMany(requests, async (missing, sharedSignal) => {
      const groups: (typeof blocks)[] = []
      for (const request of missing) {
        const block = byKey.get(request.key)!
        const current = groups.at(-1)
        const previous = current?.at(-1)
        if (previous && previous.byteStart + previous.byteLength === block.byteStart) current!.push(block)
        else groups.push([block])
      }
      const loaded = await replayPipeline(groups, async (group, groupSignal) => {
        const start = group[0]!.byteStart, end = group.at(-1)!.byteStart + group.at(-1)!.byteLength
        if (this.options.objects.readRange) {
          const chunks: { installationId: string; streamId: string; chunkId: string; frames: readonly ChunkFrame[] }[] = []
          let index = 0, position = 0
          let owned: Buffer | undefined
          await this.options.objects.readRange(manifest.key, { start, end, totalBytes: manifest.encodedBytes }, (part) => {
            let consumed = 0
            while (consumed < part.byteLength) {
              const block = group[index]
              if (!block) throw new ChunkIntegrityError('Range returned bytes beyond its last block')
              owned ??= Buffer.allocUnsafe(block.byteLength)
              const length = Math.min(part.byteLength - consumed, owned.length - position)
              owned.set(part.subarray(consumed, consumed + length), position)
              consumed += length
              position += length
              if (position === owned.length) {
                chunks.push({ ...identity, frames: decodeBlock(owned, block) })
                owned = undefined
                position = 0
                index++
              }
            }
          }, groupSignal)
          if (index !== group.length || position !== 0) throw new ChunkIntegrityError('Range ended inside a block')
          return chunks
        }
        const bytes = await this.options.objects.getRange(manifest.key, { start, end, totalBytes: manifest.encodedBytes }, groupSignal)
        if (bytes.length !== end - start) throw new ChunkIntegrityError('Range length differs from requested blocks')
        return group.map((block) => {
          const localStart = block.byteStart - start
          // A retained block must own its allocation independently of the group.
          const owned = group.length === 1 ? bytes : Buffer.from(bytes.subarray(localStart, localStart + block.byteLength))
          return { ...identity, frames: decodeBlock(owned, block) }
        })
      }, 0, sharedSignal)
      return loaded.flat()
    }, signal)
    const selected: ChunkFrame[] = []
    for (const [index, chunk] of decoded.entries()) {
      const block = blocks[index]!
      selected.push(...chunk.frames.slice(Math.max(0, frame.firstFrame - 1 - block.firstFrame), frame.lastFrame - block.firstFrame))
    }
    return selected
  }
}
