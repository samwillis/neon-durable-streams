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
  await expect(cache.get('b', load, 10)).rejects.toThrow('closed')
  expect(count).toBe(4)
})

test('canceling one reader preserves a shared read needed by another reader', async () => {
  const cache = new VerifiedChunkCache(1000)
  let enter!: () => void, finish!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve })
  const gate = new Promise<void>((resolve) => { finish = resolve })
  let loadCount = 0, sharedSignal!: AbortSignal
  const canceled = new AbortController()
  const first = cache.getMany([{ key: 'a', encodedBytes: 10 }, { key: 'b', encodedBytes: 10 }], async (_missing, signal) => {
    loadCount++; sharedSignal = signal; enter(); await gate; return [chunk, chunk]
  }, canceled.signal)
  const failed = expect(first).rejects.toThrow('reader canceled')
  await entered
  const second = cache.get('b', async () => { throw new Error('duplicate request') }, 10)
  canceled.abort(new Error('reader canceled'))
  await failed
  expect(sharedSignal.aborted).toBe(false)
  finish()
  expect(await second).toBe(chunk)
  expect(loadCount).toBe(1)
  expect(cache.inFlightBytes).toBe(0)
})

test('the last reader waits for aborted I/O to settle and releases its loading budget', async () => {
  const cache = new VerifiedChunkCache(1000, 20)
  const canceled = new AbortController()
  let enter!: () => void, finish!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve })
  const gate = new Promise<void>((resolve) => { finish = resolve })
  let signal!: AbortSignal
  const reading = cache.get('a', async (shared) => { signal = shared; enter(); await gate; return chunk }, 20, canceled.signal)
  const failed = expect(reading).rejects.toThrow()
  await entered
  canceled.abort()
  expect(signal.aborted).toBe(true)
  expect(cache.inFlightBytes).toBe(20)
  await expect(cache.get('b', async () => chunk, 10)).rejects.toThrow('capacity')
  finish()
  await failed
  expect(cache.inFlightBytes).toBe(0)
  expect(await cache.get('a', async () => chunk, 20)).toBe(chunk)
})
