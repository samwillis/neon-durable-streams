import { expect, test } from 'vitest'
import { encodeBlockObject, decodeBlockObject } from '../../src/block-codec.js'
import { chunkDigest } from '../../src/chunk-types.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'

test('an object splits blocks at history transitions and preserves inherited start tokens', () => {
  const identity = { installationId: '00112233-4455-6677-8899-aabbccddeeff', streamId: '11111111-1111-4111-8111-111111111111', chunkId: '22222222-2222-4222-8222-222222222222' }
  const history = ['a'.repeat(32), 'a'.repeat(32), 'b'.repeat(32), 'b'.repeat(32)]
  let cursor = ZERO_OFFSET
  const frames = history.map((span) => {
    const start = cursor
    cursor = advanceOffset(start, span)
    return { start, end: cursor, payload: Buffer.from('x'), messageEnds: null }
  })
  const encoded = encodeBlockObject({ ...identity, frames })
  expect(encoded.directory).toHaveLength(2)
  expect(decodeBlockObject(encoded.bytes, identity, chunkDigest(encoded.bytes)).frames).toEqual(frames)
})
