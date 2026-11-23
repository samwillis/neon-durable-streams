import type { Pool } from 'pg'
import { decodeChunk } from './chunk-codec.js'
import { boundaryDirectory, directoryFromLengths, equalDirectory } from './boundary-directory.js'
import { StorageCompatibilityError } from './errors.js'
import { validateSchema } from './path.js'
import type { ChunkStorage } from './storage.js'
import { transaction } from './transaction.js'

export interface StorageConversionOptions {
  readonly schema?: string
  readonly objects?: ChunkStorage
  readonly maxChunks?: number
}

/** Stopped, bounded and resumable. Preparation fences old runtimes; canonical
 * runtimes reject this intermediate schema. Never perform object I/O in SQL. */
export async function convertSealedStorage(pool: Pool, options: StorageConversionOptions = {}): Promise<{ complete: boolean; converted: number }> {
  const schema = validateSchema(options.schema ?? 'durable_streams'),
    maximum = options.maxChunks ?? 16
  if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid conversion batch size')
  if (!(await pool.query(`SELECT 1 FROM ${schema}.migrations WHERE id='0008_compact_prepare'`)).rowCount) {
    throw new StorageCompatibilityError('Stop all runtimes and migrate to 0008_compact_prepare before conversion')
  }
  const before = (
    await pool.query<{ id: string; serving_epoch: string; storage_identity: string | null }>(
      `SELECT id,serving_epoch,storage_identity FROM ${schema}.installation`,
    )
  ).rows[0]
  if (!before || (options.objects?.identity ?? null) !== before.storage_identity)
    throw new StorageCompatibilityError('Conversion requires the matching installation object storage')
  const chunks = (
    await pool.query<{
      id: string
      stream_id: string
      start_offset: string
      end_offset: string
      frame_count: number
      payload_bytes: string
      object_key: string
      sha256: string
      encoded_bytes: number
      retired_at: Date | null
    }>(
      `SELECT * FROM ${schema}.chunks
      WHERE state='sealed' AND end_deltas IS NULL ORDER BY id LIMIT $1`,
      [maximum],
    )
  ).rows
  let converted = 0
  for (const chunk of chunks) {
    const source = (
      await pool.query<{ start_offset: string; end_offset: string; payload_bytes: number }>(
        `SELECT start_offset,end_offset,payload_bytes FROM ${schema}.appends
      WHERE stream_id=$1 AND chunk_id=$2 ORDER BY end_offset`,
        [chunk.stream_id, chunk.id],
      )
    ).rows
    if (
      source.length !== chunk.frame_count ||
      source[0]?.start_offset !== chunk.start_offset ||
      source.at(-1)?.end_offset !== chunk.end_offset ||
      BigInt(source.reduce((sum, frame) => sum + frame.payload_bytes, 0)) !== BigInt(chunk.payload_bytes)
    )
      throw new StorageCompatibilityError('Conversion frame index differs from manifest')
    const directory = directoryFromLengths(
      chunk.start_offset,
      source.map((frame) => ({ start: frame.start_offset, end: frame.end_offset, bytes: frame.payload_bytes })),
    )
    // Retired objects may already be collected. Their exact retained index is
    // sufficient for pending boundary checks; live data must verify its object.
    if (chunk.retired_at === null) {
      if (!options.objects) throw new StorageCompatibilityError('Sealed conversion requires object storage')
      const bytes = await options.objects.get(chunk.object_key, chunk.encoded_bytes)
      if (bytes.length !== chunk.encoded_bytes) throw new StorageCompatibilityError('Conversion object length differs from manifest')
      const frames = decodeChunk(bytes, { installationId: before.id, streamId: chunk.stream_id, chunkId: chunk.id }, chunk.sha256).frames
      if (
        frames[0]?.start !== chunk.start_offset ||
        frames.at(-1)?.end !== chunk.end_offset ||
        !equalDirectory(directory, boundaryDirectory(chunk.start_offset, frames))
      )
        throw new StorageCompatibilityError('Conversion object differs from retained frame index')
    }
    converted += await transaction(pool, {}, async (client) => {
      const current = (
        await client.query<{ id: string; serving_epoch: string; storage_identity: string | null }>(
          `SELECT id,serving_epoch,storage_identity FROM ${schema}.installation FOR KEY SHARE`,
        )
      ).rows[0]
      if (
        !current ||
        current.id !== before.id ||
        current.serving_epoch !== before.serving_epoch ||
        current.storage_identity !== before.storage_identity
      )
        throw new StorageCompatibilityError('Installation changed during conversion')
      const published = await client.query(
        `UPDATE ${schema}.chunks SET end_deltas=$2 WHERE id=$1 AND end_deltas IS NULL AND state='sealed'
        AND sha256=$3 AND object_key=$4 RETURNING id`,
        [chunk.id, directory, chunk.sha256, chunk.object_key],
      )
      if (!published.rowCount) return 0 // Another conversion batch already won.
      const removed = await client.query(`DELETE FROM ${schema}.appends WHERE stream_id=$1 AND chunk_id=$2`, [chunk.stream_id, chunk.id])
      if (removed.rowCount !== chunk.frame_count) throw new StorageCompatibilityError('Conversion staging count changed')
      return 1
    })
  }
  const pending = await pool.query(`SELECT 1 FROM ${schema}.chunks WHERE state='sealed' AND end_deltas IS NULL LIMIT 1`)
  return { converted, complete: !pending.rowCount }
}
