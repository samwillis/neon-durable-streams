import { advanceOffset, parseOffset } from './offset.js'

interface Range {
  readonly stream_id: string
  readonly start_offset: string
  readonly end_offset: string
  readonly payload_bytes: number
}
export interface PostgresFrame extends Range {
  readonly storage: 'postgres'
  readonly payload: Buffer
  readonly message_ends: number[] | null
}
export interface ObjectRange extends Range {
  readonly storage: 'object'
  readonly manifest: {
    readonly id: string
    readonly key: string
    readonly sha256: string
    readonly encodedBytes: number
    readonly endDeltas: number[]
    readonly blockDirectory: Buffer
    readonly start: string
  }
  readonly firstFrame: number
  readonly lastFrame: number
}
export type StoredFrame = PostgresFrame | ObjectRange

/** Nullable joins exist only at the SQL boundary, never in a replay plan. */
export interface ReplayRow extends Range {
  readonly payload: Buffer | null
  readonly message_ends: number[] | null
  readonly chunk_id: string | null
  readonly object_key: string | null
  readonly sha256: string | null
  readonly encoded_bytes: number | null
  readonly end_deltas: number[] | null
  readonly block_directory: Buffer | null
  readonly chunk_start: string | null
  readonly first_frame: number | null
  readonly last_frame: number | null
}
export function decodeReplayRow(row: ReplayRow): StoredFrame {
  const range = { stream_id: row.stream_id, start_offset: row.start_offset, end_offset: row.end_offset, payload_bytes: row.payload_bytes }
  parseOffset(row.start_offset); parseOffset(row.end_offset)
  if (!Number.isSafeInteger(row.payload_bytes) || row.payload_bytes < 1 || row.end_offset <= row.start_offset) throw new Error('Invalid replay range')
  if (row.chunk_id === null) {
    if (!Buffer.isBuffer(row.payload) || row.payload.length !== row.payload_bytes || advanceOffset(row.start_offset, row.payload.length) !== row.end_offset) throw new Error('Postgres replay frame is incomplete')
    return { ...range, storage: 'postgres', payload: row.payload, message_ends: row.message_ends }
  }
  if (row.payload !== null || !row.object_key || !row.sha256 || !row.encoded_bytes || row.encoded_bytes < 1
    || !row.end_deltas?.length || !Buffer.isBuffer(row.block_directory) || !row.block_directory.length || !row.chunk_start || row.first_frame === null || row.last_frame === null
    || !Number.isSafeInteger(row.first_frame) || !Number.isSafeInteger(row.last_frame)
    || row.first_frame < 1 || row.last_frame < row.first_frame || row.last_frame > row.end_deltas.length) throw new Error('Sealed range is missing its manifest')
  return { ...range, storage: 'object', manifest: { id: row.chunk_id, key: row.object_key, sha256: row.sha256,
    encodedBytes: row.encoded_bytes, endDeltas: row.end_deltas, blockDirectory: row.block_directory, start: row.chunk_start }, firstFrame: row.first_frame, lastFrame: row.last_frame }
}
