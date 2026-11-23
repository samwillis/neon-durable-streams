import { expect, test } from 'vitest'
import { VerifiedChunkCache } from '../../src/chunk-cache.js'
import type { Chunk } from '../../src/chunk-codec.js'

const chunk: Chunk = { installationId: 'fixture', streamId: 'fixture', chunkId: 'fixture', frames: [] }
test('concurrent reads coalesce, failed loads retry, and zero budget retains no objects', async () => {
  const cache = new VerifiedChunkCache(0)
  let count = 0
  const load = async () => {
    count++
    await Promise.resolve()
    return chunk
  }
  expect(await Promise.all([cache.get('one', load, 10), cache.get('one', load, 10)])).toEqual([chunk, chunk])
  expect(count).toBe(1)
  await cache.get('one', load, 10)
  expect(count).toBe(2)
  await expect(
    cache.get(
      'bad',
      async () => {
        throw new Error('temporary')
      },
      10,
    ),
  ).rejects.toThrow('temporary')
  expect(await cache.get('bad', load, 10)).toBe(chunk)
})

test('LRU retention respects its byte budget and evicts old objects', async () => {
  const cache = new VerifiedChunkCache(50)
  let count = 0
  const load = async () => {
    count++
    return chunk
  }
  await cache.get('a', load, 10)
  await cache.get('b', load, 10)
  await cache.get('a', load, 10)
  expect(count).toBe(2)
  await cache.get('c', load, 10)
  await cache.get('a', load, 10)
  expect(count).toBe(3)
  await cache.get('b', load, 10)
  expect(count).toBe(4)
  cache.close()
  await cache.get('b', load, 10)
  expect(count).toBe(5)
  await cache.get('b', load, 10)
  expect(count).toBe(6)
})
