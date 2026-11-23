import { advanceOffset, isSuccessor, parseOffset } from './offset.js'
import { chunkDigest, ChunkIntegrityError, MAX_CHUNK_BYTES } from './chunk-types.js'
import type { Chunk, ChunkFrame, ChunkIdentity } from './chunk-types.js'

const MAGIC = Buffer.from('NDSBLK03')
const HEADER_BYTES = 68
const DIRECTORY_FIXED_BYTES = 56
const MAX_FRAMES = 131072
const MAX_BLOCK_FRAMES = 8192
const MAX_OFFSET_BYTES = 255

function validateOffset(value: string): void {
  try { parseOffset(value) }
  catch { throw new ChunkIntegrityError('Invalid block offset') }
}

export interface ChunkBlock {
  readonly firstFrame: number
  readonly frameCount: number
  readonly payloadBytes: number
  readonly byteStart: number
  readonly byteLength: number
  readonly sha256: string
  readonly start: string
  readonly end: string
}

export interface BlockObject {
  readonly bytes: Buffer
  readonly directory: readonly ChunkBlock[]
  readonly packedDirectory: Buffer
}

function uuidBytes(value: string): Buffer {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)) throw new ChunkIntegrityError('Invalid block identity')
  return Buffer.from(value.replaceAll('-', ''), 'hex')
}

function integerBytes(value: number): number {
  let length = 1
  while (value >= 128) { value = Math.floor(value / 128); length++ }
  return length
}

function writeInteger(bytes: Buffer, position: number, value: number): number {
  while (value >= 128) {
    bytes[position++] = value % 128 + 128
    value = Math.floor(value / 128)
  }
  bytes[position++] = value
  return position
}

function encodedFrameLength(frame: ChunkFrame): number {
  let length = integerBytes(frame.payload.length) + integerBytes(frame.messageEnds === null ? 0 : frame.messageEnds.length) + frame.payload.length
  let previous = 0
  for (const end of frame.messageEnds ?? []) {
    if (!Number.isSafeInteger(end) || end <= previous || end > frame.payload.length || frame.payload[end - 1] !== 44) throw new ChunkIntegrityError('Invalid block JSON index')
    length += integerBytes(end - previous)
    previous = end
  }
  if (frame.messageEnds !== null && previous !== frame.payload.length) throw new ChunkIntegrityError('Incomplete block JSON index')
  return length
}

/** Writes each payload once. Offsets and lengths live in the compact directory. */
export function encodeBlockObject(chunk: Chunk, targetBlockBytes = 65536): BlockObject {
  if (!Number.isSafeInteger(targetBlockBytes) || targetBlockBytes < 1 || targetBlockBytes > 16 * 1024 * 1024) throw new ChunkIntegrityError('Invalid block target')
  if (!chunk.frames.length || chunk.frames.length > MAX_FRAMES) throw new ChunkIntegrityError('Invalid block object frame count')
  const blocks: { frames: ChunkFrame[]; encodedBytes: number; payloadBytes: number; firstFrame: number }[] = []
  let current = { frames: [] as ChunkFrame[], encodedBytes: 0, payloadBytes: 0, firstFrame: 0 }
  let previous = chunk.frames[0]!.start
  for (const [index, frame] of chunk.frames.entries()) {
    validateOffset(frame.start)
    validateOffset(frame.end)
    if (frame.start !== previous || !frame.payload.length || !isSuccessor(frame.start, frame.end)) throw new ChunkIntegrityError('Noncontiguous block frame')
    previous = frame.end
    const length = encodedFrameLength(frame)
    if (current.frames.length && (current.frames.length >= MAX_BLOCK_FRAMES || current.encodedBytes >= targetBlockBytes || parseOffset(current.frames.at(-1)!.end).history !== parseOffset(frame.end).history)) {
      blocks.push(current)
      current = { frames: [], encodedBytes: 0, payloadBytes: 0, firstFrame: index }
    }
    current.frames.push(frame)
    current.encodedBytes += length
    current.payloadBytes += frame.payload.length
  }
  blocks.push(current)
  const directoryBytes = blocks.reduce((sum, block) => sum + DIRECTORY_FIXED_BYTES + block.frames[0]!.start.length + block.frames.at(-1)!.end.length, 0)
  const length = HEADER_BYTES + directoryBytes + blocks.reduce((sum, block) => sum + block.encodedBytes, 0)
  if (length > MAX_CHUNK_BYTES) throw new ChunkIntegrityError('Block object exceeds size limit')
  const bytes = Buffer.allocUnsafe(length)
  MAGIC.copy(bytes)
  uuidBytes(chunk.installationId).copy(bytes, 8)
  uuidBytes(chunk.streamId).copy(bytes, 24)
  uuidBytes(chunk.chunkId).copy(bytes, 40)
  bytes.writeUInt32BE(chunk.frames.length, 56)
  bytes.writeUInt32BE(blocks.length, 60)
  bytes.writeUInt32BE(directoryBytes, 64)
  let position = HEADER_BYTES + directoryBytes
  const directory: ChunkBlock[] = []
  for (const block of blocks) {
    const byteStart = position
    for (const frame of block.frames) {
      position = writeInteger(bytes, position, frame.payload.length)
      position = writeInteger(bytes, position, frame.messageEnds?.length ?? 0)
      let previousEnd = 0
      for (const end of frame.messageEnds ?? []) {
        position = writeInteger(bytes, position, end - previousEnd)
        previousEnd = end
      }
      bytes.set(frame.payload, position)
      position += frame.payload.length
    }
    directory.push({ firstFrame: block.firstFrame, frameCount: block.frames.length, payloadBytes: block.payloadBytes,
      byteStart, byteLength: position - byteStart, sha256: chunkDigest(bytes.subarray(byteStart, position)),
      start: block.frames[0]!.start, end: block.frames.at(-1)!.end })
  }
  const packedDirectory = packBlockDirectory(directory)
  packedDirectory.copy(bytes, HEADER_BYTES)
  return { bytes, directory, packedDirectory }
}

export function packBlockDirectory(directory: readonly ChunkBlock[]): Buffer {
  if (!directory.length || directory.length > MAX_FRAMES) throw new ChunkIntegrityError('Invalid block directory count')
  const length = directory.reduce((sum, block) => sum + DIRECTORY_FIXED_BYTES + block.start.length + block.end.length, 0)
  if (length > MAX_CHUNK_BYTES) throw new ChunkIntegrityError('Block directory exceeds size limit')
  const bytes = Buffer.allocUnsafe(length)
  let position = 0
  for (const block of directory) {
    validateOffset(block.start)
    validateOffset(block.end)
    if (![block.firstFrame, block.frameCount, block.payloadBytes, block.byteStart, block.byteLength].every((value) => Number.isSafeInteger(value) && value >= 0 && value <= 0xffffffff)) throw new ChunkIntegrityError('Invalid block directory integer')
    if (!/^[0-9a-f]{64}$/.test(block.sha256)) throw new ChunkIntegrityError('Invalid block checksum')
    if (block.start.length > MAX_OFFSET_BYTES || block.end.length > MAX_OFFSET_BYTES) throw new ChunkIntegrityError('Block offset exceeds encoding limit')
    for (const value of [block.firstFrame, block.frameCount, block.payloadBytes, block.byteStart, block.byteLength]) {
      bytes.writeUInt32BE(value, position)
      position += 4
    }
    Buffer.from(block.sha256, 'hex').copy(bytes, position)
    position += 32
    bytes.writeUInt16BE(block.start.length, position)
    bytes.writeUInt16BE(block.end.length, position + 2)
    position += 4
    position += bytes.write(block.start, position, 'ascii')
    position += bytes.write(block.end, position, 'ascii')
  }
  return bytes
}

/** Validates the complete trusted manifest before any range is requested. */
export function unpackBlockDirectory(bytes: Buffer, objectBytes: number, frameCount: number): readonly ChunkBlock[] {
  if (!Number.isSafeInteger(objectBytes) || objectBytes > MAX_CHUNK_BYTES || objectBytes < HEADER_BYTES
    || !Number.isSafeInteger(frameCount) || frameCount < 1 || frameCount > MAX_FRAMES || !bytes.length) throw new ChunkIntegrityError('Invalid block manifest bounds')
  const blocks: ChunkBlock[] = []
  let position = 0, nextFrame = 0, nextByte = HEADER_BYTES + bytes.length
  let previousEnd: string | undefined
  while (position < bytes.length) {
    if (position + DIRECTORY_FIXED_BYTES > bytes.length) throw new ChunkIntegrityError('Truncated block directory')
    const firstFrame = bytes.readUInt32BE(position), count = bytes.readUInt32BE(position + 4), payloadBytes = bytes.readUInt32BE(position + 8),
      byteStart = bytes.readUInt32BE(position + 12), byteLength = bytes.readUInt32BE(position + 16), sha256 = bytes.toString('hex', position + 20, position + 52),
      startLength = bytes.readUInt16BE(position + 52), endLength = bytes.readUInt16BE(position + 54)
    position += DIRECTORY_FIXED_BYTES
    if (!startLength || startLength > MAX_OFFSET_BYTES || !endLength || endLength > MAX_OFFSET_BYTES || position + startLength + endLength > bytes.length) throw new ChunkIntegrityError('Invalid block offset lengths')
    const start = bytes.toString('latin1', position, position + startLength)
    position += startLength
    const end = bytes.toString('latin1', position, position + endLength)
    position += endLength
    validateOffset(start)
    validateOffset(end)
    if (firstFrame !== nextFrame || !count || count > MAX_BLOCK_FRAMES || firstFrame + count > frameCount || !payloadBytes || byteStart !== nextByte
      || byteLength < count * 3 || payloadBytes > byteLength || byteStart + byteLength > objectBytes
      || start >= end || parseOffset(end).ordinal - parseOffset(start).ordinal !== BigInt(count) || (previousEnd !== undefined && start !== previousEnd)) throw new ChunkIntegrityError('Noncontiguous block directory')
    blocks.push({ firstFrame, frameCount: count, payloadBytes, byteStart, byteLength, sha256, start, end })
    nextFrame += count
    nextByte += byteLength
    previousEnd = end
  }
  if (nextFrame !== frameCount || nextByte !== objectBytes) throw new ChunkIntegrityError('Incomplete block directory')
  return blocks
}

/** Independently verifies a range before exposing any of its private views. */
export function decodeBlock(bytes: Buffer, block: ChunkBlock): readonly ChunkFrame[] {
  if (bytes.length !== block.byteLength || chunkDigest(bytes) !== block.sha256) throw new ChunkIntegrityError('Block size or checksum differs')
  let position = 0
  const integer = () => {
    let value = 0, factor = 1
    for (let index = 0; index < 5; index++) {
      if (position >= bytes.length) throw new ChunkIntegrityError('Truncated block integer')
      const digit = bytes[position++]!
      value += (digit & 127) * factor
      if (digit < 128) {
        if (value > 0xffffffff || (index > 0 && digit === 0)) throw new ChunkIntegrityError('Invalid block integer')
        return value
      }
      factor *= 128
    }
    throw new ChunkIntegrityError('Block integer exceeds limit')
  }
  const frames: ChunkFrame[] = []
  let next = block.start, payloadBytes = 0
  for (let index = 0; index < block.frameCount; index++) {
    const length = integer(), messageCount = integer()
    if (!length || length > bytes.length - position || messageCount > Math.floor(length / 2)) throw new ChunkIntegrityError('Invalid block frame bounds')
    const messageEnds: number[] | null = messageCount ? [] : null
    let previous = 0
    for (let message = 0; message < messageCount; message++) {
      const delta = integer()
      if (!delta || previous + delta > length) throw new ChunkIntegrityError('Invalid block message delta')
      previous += delta
      messageEnds!.push(previous)
    }
    if (position + length > bytes.length) throw new ChunkIntegrityError('Truncated block payload')
    const payload = bytes.subarray(position, position + length)
    position += length
    if (messageEnds) {
      if (previous !== length || messageEnds.some((end) => payload[end - 1] !== 44)) throw new ChunkIntegrityError('Block JSON boundaries differ')
      try { JSON.parse(`[${new TextDecoder('utf-8', { fatal: true }).decode(payload.subarray(0, -1))}]`) }
      catch { throw new ChunkIntegrityError('Invalid block JSON') }
    }
    const end = advanceOffset(next, parseOffset(block.end).history)
    frames.push({ start: next, end, payload, messageEnds })
    next = end
    payloadBytes += length
  }
  if (next !== block.end || payloadBytes !== block.payloadBytes || position !== bytes.length) throw new ChunkIntegrityError('Block differs from manifest')
  return frames
}

export function decodeBlockObject(bytes: Buffer, identity: ChunkIdentity, sha256: string): Chunk {
  if (bytes.length < HEADER_BYTES || bytes.length > MAX_CHUNK_BYTES || !bytes.subarray(0, 8).equals(MAGIC) || chunkDigest(bytes) !== sha256) throw new ChunkIntegrityError('Invalid block object envelope')
  if (!bytes.subarray(8, 24).equals(uuidBytes(identity.installationId)) || !bytes.subarray(24, 40).equals(uuidBytes(identity.streamId))
    || !bytes.subarray(40, 56).equals(uuidBytes(identity.chunkId))) throw new ChunkIntegrityError('Block object identity differs')
  const directoryEnd = HEADER_BYTES + bytes.readUInt32BE(64)
  if (directoryEnd > bytes.length) throw new ChunkIntegrityError('Truncated block object directory')
  const blocks = unpackBlockDirectory(bytes.subarray(HEADER_BYTES, directoryEnd), bytes.length, bytes.readUInt32BE(56))
  if (blocks.length !== bytes.readUInt32BE(60)) throw new ChunkIntegrityError('Block count differs')
  return { ...identity, frames: blocks.flatMap((block) => decodeBlock(bytes.subarray(block.byteStart, block.byteStart + block.byteLength), block)) }
}
