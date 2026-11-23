import { readFileSync } from 'node:fs'
import fc from 'fast-check'
import { expect, test } from 'vitest'
import { chunkDigest, ChunkIntegrityError, decodeChunk, encodeChunk } from '../../src/chunk-codec.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { preparePayload } from '../../src/payload.js'

const identity = { installationId: '00112233-4455-6677-8899-aabbccddeeff', streamId: '11111111-1111-4111-8111-111111111111', chunkId: '22222222-2222-4222-8222-222222222222' }
test('v2 matches the independently specified binary fixture', () => {
  const golden = Buffer.from(readFileSync(new URL('../fixtures/chunks/v2.hex', import.meta.url), 'utf8').trim(), 'hex')
  const frames = [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET, 2), payload: Buffer.from('hi'), messageEnds: null }]
  expect(encodeChunk({ ...identity, frames })).toEqual(golden)
  expect(decodeChunk(golden, identity, chunkDigest(golden))).toEqual({ ...identity, frames })
})
test('generated frames preserve all binary bytes, offsets, and frame ordering', () => {
  fc.assert(fc.property(fc.array(fc.uint8Array({ minLength: 1, maxLength: 1024 }), { minLength: 1, maxLength: 20 }), (payloads) => {
    let next = ZERO_OFFSET
    const frames = payloads.map((payload) => {
      const start = next
      next = advanceOffset(start, payload.length)
      return { start, end: next, payload: Buffer.from(payload), messageEnds: null }
    })
    const bytes = encodeChunk({ ...identity, frames })
    expect(decodeChunk(bytes, identity, chunkDigest(bytes)).frames).toEqual(frames)
    const corrupt = Buffer.from(bytes)
    corrupt[corrupt.length - 1] = corrupt[corrupt.length - 1]! ^ 1
    expect(() => decodeChunk(corrupt, identity, chunkDigest(bytes))).toThrow(ChunkIntegrityError)
  }))
})
test('JSON indexes, UTF-8 and numeric lexemes survive the sealed format', () => {
  const input = preparePayload(Buffer.from('[9007199254740993,1e400,-0,"🐘",[1,2]]'), 'application/json')
  const frame = { start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET, input.bytes.length), payload: input.bytes, messageEnds: input.messageEnds }
  const bytes = encodeChunk({ ...identity, frames: [frame] })
  expect(decodeChunk(bytes, identity, chunkDigest(bytes)).frames).toEqual([frame])
})
test('unknown envelopes, truncation, and an object from another stream fail closed', () => {
  const frames = [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET, 2), payload: Buffer.from('hi'), messageEnds: null }]
  const bytes = encodeChunk({ ...identity, frames })
  for (const length of [0, 8, 59, 60, bytes.length - 1]) {
    const truncated = bytes.subarray(0, length)
    expect(() => decodeChunk(truncated, identity, chunkDigest(truncated))).toThrow(ChunkIntegrityError)
  }
  expect(() => decodeChunk(bytes, { ...identity, streamId: identity.chunkId }, chunkDigest(bytes))).toThrow(ChunkIntegrityError)
})
