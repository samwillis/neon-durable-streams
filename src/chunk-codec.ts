import { createHash } from 'node:crypto'
import { advanceOffset, parseOffset } from './offset.js'

const MAGIC = Buffer.from('NDSTRM01')
const HEADER_BYTES = 60
const FRAME_HEADER_BYTES = 74
export const MAX_CHUNK_BYTES = 128 * 1024 * 1024
export interface ChunkIdentity { readonly installationId: string; readonly streamId: string; readonly chunkId: string }
export interface ChunkFrame {
  readonly start: string
  readonly end: string
  readonly payload: Buffer
  readonly messageEnds: readonly number[] | null
}
export interface Chunk extends ChunkIdentity { readonly frames: readonly ChunkFrame[] }
export class ChunkIntegrityError extends Error { override readonly name = 'ChunkIntegrityError' }
export function chunkDigest(bytes: Uint8Array): string { return createHash('sha256').update(bytes).digest('hex') }
const uuidBytes = (uuid: string) => {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(uuid)) throw new ChunkIntegrityError('Invalid chunk identity')
  return Buffer.from(uuid.replaceAll('-', ''), 'hex')
}
const uuidString = (bytes: Buffer) => {
  const hex = bytes.toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
function validateFrames(frames: readonly ChunkFrame[]): void {
  if (!frames.length || frames.length > 1024) throw new ChunkIntegrityError('Invalid frame count')
  let previous: string | undefined
  for (const frame of frames) {
    try { parseOffset(frame.start); parseOffset(frame.end) } catch { throw new ChunkIntegrityError('Invalid frame offset') }
    if (!frame.payload.length || frame.end !== advanceOffset(frame.start, frame.payload.length) || (previous && previous !== frame.start)) throw new ChunkIntegrityError('Noncontiguous chunk frames')
    previous = frame.end
    if (frame.messageEnds !== null) {
      let prior = 0
      if (!frame.messageEnds.length) throw new ChunkIntegrityError('Empty JSON index')
      for (const end of frame.messageEnds) {
        if (!Number.isSafeInteger(end) || end <= prior || end > frame.payload.length || frame.payload[end - 1] !== 44) throw new ChunkIntegrityError('Invalid JSON index')
        prior = end
      }
      if (prior !== frame.payload.length) throw new ChunkIntegrityError('Truncated JSON index')
      try { JSON.parse(`[${new TextDecoder('utf-8', { fatal: true }).decode(frame.payload.subarray(0, -1))}]`) } catch { throw new ChunkIntegrityError('Invalid JSON bytes') }
    }
  }
}

/** Fixed binary v1: identity, frame count, then offsets/lengths/indexes/raw bytes. */
export function encodeChunk(chunk: Chunk): Buffer {
  validateFrames(chunk.frames)
  const length = HEADER_BYTES + chunk.frames.reduce((sum, frame) => sum + FRAME_HEADER_BYTES + (frame.messageEnds?.length ?? 0) * 4 + frame.payload.length, 0)
  if (length > MAX_CHUNK_BYTES) throw new ChunkIntegrityError('Chunk exceeds format limit')
  const bytes = Buffer.allocUnsafe(length)
  MAGIC.copy(bytes)
  uuidBytes(chunk.installationId).copy(bytes, 8)
  uuidBytes(chunk.streamId).copy(bytes, 24)
  uuidBytes(chunk.chunkId).copy(bytes, 40)
  bytes.writeUInt32BE(chunk.frames.length, 56)
  let position = HEADER_BYTES
  for (const frame of chunk.frames) {
    bytes.write(frame.start, position, 33, 'ascii'); position += 33
    bytes.write(frame.end, position, 33, 'ascii'); position += 33
    bytes.writeUInt32BE(frame.payload.length, position); position += 4
    bytes.writeUInt32BE(frame.messageEnds === null ? 0xffffffff : frame.messageEnds.length, position); position += 4
    for (const end of frame.messageEnds ?? []) { bytes.writeUInt32BE(end, position); position += 4 }
    frame.payload.copy(bytes, position); position += frame.payload.length
  }
  return bytes
}

export function decodeChunk(bytes: Buffer, identity: ChunkIdentity, digest: string): Chunk {
  if (bytes.length < HEADER_BYTES || bytes.length > MAX_CHUNK_BYTES || !bytes.subarray(0, 8).equals(MAGIC) || chunkDigest(bytes) !== digest) throw new ChunkIntegrityError('Invalid chunk envelope or checksum')
  const actual = { installationId: uuidString(bytes.subarray(8, 24)), streamId: uuidString(bytes.subarray(24, 40)), chunkId: uuidString(bytes.subarray(40, 56)) }
  if (actual.installationId !== identity.installationId || actual.streamId !== identity.streamId || actual.chunkId !== identity.chunkId) throw new ChunkIntegrityError('Chunk identity mismatch')
  const count = bytes.readUInt32BE(56)
  if (!count || count > 1024) throw new ChunkIntegrityError('Invalid frame count')
  let position = HEADER_BYTES
  const frames: ChunkFrame[] = []
  for (let i = 0; i < count; i++) {
    if (position + FRAME_HEADER_BYTES > bytes.length) throw new ChunkIntegrityError('Truncated frame header')
    const start = bytes.toString('latin1', position, position + 33); position += 33
    const end = bytes.toString('latin1', position, position + 33); position += 33
    const length = bytes.readUInt32BE(position); position += 4
    const n = bytes.readUInt32BE(position); position += 4
    const indexBytes = n === 0xffffffff ? 0 : n * 4
    if (position + indexBytes + length > bytes.length) throw new ChunkIntegrityError('Truncated frame data')
    const messageEnds: number[] | null = n === 0xffffffff ? null : []
    for (let j = 0; j < indexBytes; j += 4) messageEnds!.push(bytes.readUInt32BE(position + j))
    position += indexBytes
    frames.push({ start, end, payload: bytes.subarray(position, position + length), messageEnds })
    position += length
  }
  if (position !== bytes.length) throw new ChunkIntegrityError('Unexpected trailing chunk bytes')
  validateFrames(frames)
  return { ...actual, frames }
}
