import { createServer } from 'node:http'
import { once } from 'node:events'
import type { AddressInfo } from 'node:net'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createS3ChunkStorage } from '../../src/s3.js'
import { ChunkIntegrityError } from '../../src/chunk-codec.js'

const payload = Buffer.alloc(128 * 1024, 123)
let stalled!: () => void
const origin = createServer((request, response) => {
  const path = new URL(request.url!, 'http://fixture.test').pathname
  if (path.endsWith('/stall')) {
    response.setHeader('Content-Length', payload.length)
    response.write(payload.subarray(0, 1024))
    stalled()
    return
  }
  if (path.endsWith('/range') || path.endsWith('/wrong-range')) {
    const range = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range ?? '')
    if (!range) { response.writeHead(400); response.end(); return }
    const start = Number(range[1]), end = Number(range[2])
    response.writeHead(206, { 'Content-Length': end - start + 1,
      'Content-Range': `bytes ${start}-${end}/${path.endsWith('/wrong-range') ? payload.length + 1 : payload.length}` })
    response.end(payload.subarray(start, end + 1))
    return
  }
  if (path.endsWith('/missing-length')) {
    response.write(payload.subarray(0, 1024))
    response.end(payload.subarray(1024))
    return
  }
  response.setHeader('Content-Length', payload.length)
  response.write(payload.subarray(0, 1024))
  response.end(payload.subarray(1024))
})
let storage: ReturnType<typeof createS3ChunkStorage>
beforeAll(async () => {
  origin.listen(0, '127.0.0.1')
  await once(origin, 'listening')
  storage = createS3ChunkStorage({ endpoint: `http://127.0.0.1:${(origin.address() as AddressInfo).port}`, allowHttp: true,
    region: 'fixture', bucket: 'fixture', credentials: { accessKeyId: 'fixture', secretAccessKey: 'fixture' }, timeoutMs: 2000 })
})
afterAll(async () => {
  storage?.close()
  origin.closeAllConnections()
  await new Promise<void>((resolve, reject) => { origin.close((error) => { if (error) reject(error); else resolve() }) })
})

test('fragmented S3 downloads return exact owned bytes within the declared bound', async () => {
  const first = await storage.get('object', payload.length)
  expect(first).toEqual(payload)
  first.fill(0)
  expect(await storage.get('object', payload.length)).toEqual(payload)
})

test('chunked S3 downloads without Content-Length return an exact bounded buffer', async () => {
  const bytes = await storage.get('missing-length', payload.length + 1024)
  expect(bytes.length).toBe(payload.length)
  expect(bytes).toEqual(payload)
})

test('S3 downloads reject oversized declared and chunked bodies', async () => {
  await expect(storage.get('object', 1024)).rejects.toBeInstanceOf(ChunkIntegrityError)
  await expect(storage.get('missing-length', 1024)).rejects.toBeInstanceOf(ChunkIntegrityError)
})

test('half-open range requests return exactly the requested bytes, including the last byte', async () => {
  for (const [start, end] of [[0, 1], [1023, 4096], [payload.length - 1, payload.length]]) {
    expect(await storage.getRange('range', { start: start!, end: end!, totalBytes: payload.length })).toEqual(payload.subarray(start, end))
  }
})

test('range reads reject a server ignoring Range or reporting a different object size', async () => {
  const range = { start: 1024, end: 2048, totalBytes: payload.length }
  await expect(storage.getRange('object', range)).rejects.toBeInstanceOf(ChunkIntegrityError)
  await expect(storage.getRange('wrong-range', range)).rejects.toBeInstanceOf(ChunkIntegrityError)
  expect(() => storage.getRange('range', { ...range, start: -1 })).toThrow(RangeError)
  expect(() => storage.getRange('range', { ...range, end: payload.length + 1 })).toThrow(RangeError)
})

test('canceling queued and active downloads frees capacity for another reader', async () => {
  const bounded = createS3ChunkStorage({ endpoint: `http://127.0.0.1:${(origin.address() as AddressInfo).port}`, allowHttp: true,
    region: 'fixture', bucket: 'fixture', credentials: { accessKeyId: 'fixture', secretAccessKey: 'fixture' },
    timeoutMs: 2000, maxConcurrentRequests: 1, maxQueuedRequests: 1 })
  const active = new AbortController(), queued = new AbortController()
  const entered = new Promise<void>((resolve) => { stalled = resolve })
  const first = bounded.get('stall', payload.length, active.signal).then(
    () => ({ rejected: false }), (error: unknown) => ({ rejected: true, error }))
  try {
    await entered
    const waiting = bounded.get('object', payload.length, queued.signal)
    const waitingRejected = expect(waiting).rejects.toThrow('canceled queued reader')
    queued.abort(new Error('canceled queued reader'))
    await waitingRejected
    const replacement = bounded.get('object', payload.length)
    active.abort(new Error('canceled active reader'))
    expect(await first).toMatchObject({ rejected: true })
    expect(await replacement).toEqual(payload)
  } finally { bounded.close() }
})


test('streamed ranges await fragment consumers and validate exact lengths', async () => {
  const collected: Buffer[] = []
  let consuming = false
  await storage.readRange('range', { start: 1023, end: payload.length, totalBytes: payload.length }, async (bytes) => {
    expect(consuming).toBe(false)
    consuming = true
    await new Promise((resolve) => setTimeout(resolve, 1))
    collected.push(Buffer.from(bytes))
    consuming = false
  })
  expect(Buffer.concat(collected)).toEqual(payload.subarray(1023))
  await expect(storage.readRange('wrong-range', { start: 0, end: 12, totalBytes: payload.length }, () => {})).rejects.toBeInstanceOf(ChunkIntegrityError)
})

test('streamed ranges propagate consumer errors and cancellation', async () => {
  const cancel = new AbortController()
  await expect(storage.readRange('range', { start: 0, end: payload.length, totalBytes: payload.length }, () => {
    cancel.abort(new Error('stopped consumer'))
    cancel.signal.throwIfAborted()
  }, cancel.signal)).rejects.toThrow('stopped consumer')
  await expect(storage.readRange('range', { start: 0, end: 12, totalBytes: payload.length }, () => { throw new Error('bad consumer') })).rejects.toThrow('bad consumer')
  expect(await storage.getRange('range', { start: 0, end: 12, totalBytes: payload.length })).toEqual(payload.subarray(0, 12))
})
