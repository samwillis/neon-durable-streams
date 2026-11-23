import { expect, test } from 'vitest'
import fc from 'fast-check'
import { encodeBlockObject, decodeBlockObject, decodeBlock, unpackBlockDirectory, packBlockDirectory } from '../../src/block-codec.js'
import { chunkDigest, ChunkIntegrityError } from '../../src/chunk-codec.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { preparePayload } from '../../src/payload.js'

const identity = { installationId: '00112233-4455-6677-8899-aabbccddeeff', streamId: '11111111-1111-4111-8111-111111111111', chunkId: '22222222-2222-4222-8222-222222222222' }
const framesFrom = (payloads: readonly Uint8Array[]) => {
  let next = ZERO_OFFSET
  return payloads.map((payload) => {
    const start = next
    next = advanceOffset(start)
    return { start, end: next, payload: Buffer.from(payload), messageEnds: null }
  })
}

test('block binary encoding has explicit compact lengths and a packed physical directory', () => {
  const frames = framesFrom([Buffer.from('hi')])
  const encoded = encodeBlockObject({ ...identity, frames })
  // Header 68, directory 56 + a 33-byte zero and 49-byte end offset, one 4-byte binary frame.
  expect(encoded.bytes.length).toBe(210)
  expect(encoded.directory[0]).toMatchObject({ firstFrame: 0, frameCount: 1, payloadBytes: 2, byteStart: 206, byteLength: 4 })
  expect(encoded.bytes.subarray(206).toString('hex')).toBe('02006869')
  expect(encoded.bytes.toString('ascii', 0, 8)).toBe('NDSBLK03')
  expect(unpackBlockDirectory(encoded.packedDirectory, encoded.bytes.length, 1)).toEqual(encoded.directory)
  expect(decodeBlockObject(encoded.bytes, identity, chunkDigest(encoded.bytes))).toEqual({ ...identity, frames })
})

test('generated independent blocks round-trip exact bytes and detect corruption in each selected range', () => {
  fc.assert(fc.property(fc.array(fc.uint8Array({ minLength: 1, maxLength: 4096 }), { minLength: 1, maxLength: 30 }),
    fc.integer({ min: 1, max: 8192 }), (payloads, target) => {
      const frames = framesFrom(payloads)
      const encoded = encodeBlockObject({ ...identity, frames }, target)
      const directory = unpackBlockDirectory(encoded.packedDirectory, encoded.bytes.length, frames.length)
      const decoded = directory.flatMap((block) => decodeBlock(encoded.bytes.subarray(block.byteStart, block.byteStart + block.byteLength), block))
      expect(decoded).toEqual(frames)
      for (const block of directory) {
        const bytes = Buffer.from(encoded.bytes.subarray(block.byteStart, block.byteStart + block.byteLength))
        bytes[bytes.length - 1]! ^= 1
        expect(() => decodeBlock(bytes, block)).toThrow(ChunkIntegrityError)
      }
    }), { numRuns: 75 })
})

test('JSON message deltas preserve exact numbers, UTF-8 and batch boundaries across blocks', () => {
  let next = ZERO_OFFSET
  const frames = ['[9007199254740993,1e400,-0,"🐘"]', '[{"value":[1,2]},true,null]'].map((input) => {
    const prepared = preparePayload(Buffer.from(input), 'application/json')
    const start = next
    next = advanceOffset(start)
    return { start, end: next, payload: prepared.bytes, messageEnds: prepared.messageEnds }
  })
  const encoded = encodeBlockObject({ ...identity, frames }, 1)
  expect(encoded.directory).toHaveLength(2)
  expect(decodeBlockObject(encoded.bytes, identity, chunkDigest(encoded.bytes))).toEqual({ ...identity, frames })
})

test('framing overhead does not halve blocks containing half-target payloads', () => {
  const frames = framesFrom([Buffer.alloc(32768), Buffer.alloc(32768), Buffer.alloc(32768)])
  const encoded = encodeBlockObject({ ...identity, frames }, 65536)
  expect(encoded.directory.map((block) => block.frameCount)).toEqual([2, 1])
  expect(decodeBlockObject(encoded.bytes, identity, chunkDigest(encoded.bytes)).frames).toEqual(frames)
})

test('manifest gaps, overlaps, wrong identity and truncated data fail before frames are returned', () => {
  const frames = framesFrom([Buffer.from('hello'), Buffer.from('world')])
  const encoded = encodeBlockObject({ ...identity, frames }, 1)
  const blocks = encoded.directory
  for (const directory of [
    [{ ...blocks[0]!, firstFrame: 1 }, blocks[1]!],
    [blocks[0]!, { ...blocks[1]!, byteStart: blocks[1]!.byteStart - 1 }],
    [blocks[0]!, { ...blocks[1]!, start: ZERO_OFFSET }],
    [{ ...blocks[0]!, byteLength: 0 }, blocks[1]!],
  ]) expect(() => unpackBlockDirectory(packBlockDirectory(directory), encoded.bytes.length, frames.length)).toThrow(ChunkIntegrityError)
  expect(() => decodeBlockObject(encoded.bytes, { ...identity, streamId: identity.chunkId }, chunkDigest(encoded.bytes))).toThrow(ChunkIntegrityError)
  for (const length of [0, 67, 68, encoded.bytes.length - 1]) {
    const truncated = encoded.bytes.subarray(0, length)
    expect(() => decodeBlockObject(truncated, identity, chunkDigest(truncated))).toThrow(ChunkIntegrityError)
  }
})

test('a valid checksum cannot hide malformed integers, invalid message counts or trailing bytes', () => {
  const encoded = encodeBlockObject({ ...identity, frames: framesFrom([Buffer.from('hi')]) })
  const block = encoded.directory[0]!
  for (const input of [
    [0, 0],
    [0x82, 0, 0, 104, 105],
    [2, 3, 1, 1, 1, 104, 105],
    [3, 0, 104, 105],
    [2, 0, 104, 105, 0],
    [0xff, 0xff, 0xff, 0xff, 0xff, 0],
  ]) {
    const bytes = Buffer.from(input)
    expect(() => decodeBlock(bytes, { ...block, byteLength: bytes.length, sha256: chunkDigest(bytes) })).toThrow(ChunkIntegrityError)
  }
})


test('tiny frames have a decoding-work bound independent of a large byte target', () => {
  const frames = framesFrom(Array.from({ length: 8193 }, () => Buffer.from('x')))
  const encoded = encodeBlockObject({ ...identity, frames }, 16 * 1024 * 1024)
  expect(encoded.directory.map((block) => block.frameCount)).toEqual([8192, 1])
  expect(decodeBlockObject(encoded.bytes, identity, chunkDigest(encoded.bytes)).frames).toEqual(frames)
})
