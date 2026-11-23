import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, ChunkByteRange, DurableStreamsServer } from '../../src/index.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { migrate } from '../../src/migrations.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { unpackBlockDirectory } from '../../src/block-codec.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Block replay requires actual Neon services')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 8 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const servers: DurableStreamsServer[] = []
const payloads = Array.from({ length: 32 }, (_, index) => `${String(index).padStart(4, '0')}${'x'.repeat(16380)}`)
const offsets: string[] = []
let manifest: { id: string; object_key: string; encoded_bytes: number; block_directory: Buffer }
function server(storage: ChunkStorage = objects, maxPageFrames = 1) {
  const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema },
    storage: { objects: storage, targetChunkBytes: 524288, blockBytes: 65536 }, limits: { maxPageFrames },
    auth: { authenticate: () => ({ tenantId: 'blocks', subject: 'fixture' }), authorize: () => true } })
  servers.push(value)
  return value
}
const request = (handler: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))
const seek = (handler: DurableStreamsServer, index: number) => request(handler, `binary?offset=${offsets[index]}`)
function countingStorage() {
  let gets = 0
  const ranges: ChunkByteRange[] = []
  const storage: ChunkStorage = { ...objects,
    async get(key, maximum, signal) { gets++; return objects.get(key, maximum, signal) },
    async getRange(key, range, signal) { ranges.push(range); return objects.getRange(key, range, signal) },
    async readRange(key, range, consume, signal) { ranges.push(range); return objects.readRange(key, range, consume, signal) },
  }
  return { storage, ranges, gets: () => gets }
}

beforeAll(async () => {
  await migrate(direct, { schema, storageIdentity: objects.identity })
  const writer = server(objects, 1024)
  const created = await request(writer, 'binary', 'PUT')
  expect(created.status).toBe(201)
  offsets.push(created.headers.get('Stream-Next-Offset')!)
  for (const payload of payloads) {
    const appended = await request(writer, 'binary', 'POST', payload)
    expect(appended.status).toBe(204)
    offsets.push(appended.headers.get('Stream-Next-Offset')!)
  }
  expect((await writer.maintain({ maxChunks: 1 })).sealed).toBe(1)
  manifest = (await direct.query(`SELECT id,object_key,encoded_bytes,block_directory FROM "${schema}".chunks WHERE state='sealed'`)).rows[0]
  expect(unpackBlockDirectory(manifest.block_directory, manifest.encoded_bytes, payloads.length)).toHaveLength(8)
}, 120000)
afterAll(async () => {
  await Promise.all(servers.map((value) => value.close()))
  objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
})

test('a cold interior seek downloads one verified block and reuses it for another frame', async () => {
  const counted = countingStorage(), reader = server(counted.storage)
  const first = await seek(reader, 10)
  expect(first.status).toBe(200)
  expect(await first.text()).toBe(payloads[10])
  expect(first.headers.get('Stream-Next-Offset')).toBe(offsets[11])
  expect(counted.gets()).toBe(0)
  expect(counted.ranges).toHaveLength(1)
  expect(counted.ranges[0]!.end - counted.ranges[0]!.start).toBeLessThan(manifest.encoded_bytes / 4)
  expect(await (await seek(reader, 11)).text()).toBe(payloads[11])
  expect(counted.ranges).toHaveLength(1)
})

test('adjacent selected blocks coalesce into one partial HTTP request', async () => {
  const counted = countingStorage(), reader = server(counted.storage, 10)
  const response = await seek(reader, 1)
  expect(response.status).toBe(200)
  expect(await response.text()).toBe(payloads.slice(1, 11).join(''))
  expect(response.headers.get('Stream-Next-Offset')).toBe(offsets[11])
  const blocks = unpackBlockDirectory(manifest.block_directory, manifest.encoded_bytes, payloads.length)
  expect(counted.gets()).toBe(0)
  expect(counted.ranges).toEqual([{ start: blocks[0]!.byteStart,
    end: blocks[2]!.byteStart + blocks[2]!.byteLength, totalBytes: manifest.encoded_bytes }])
})

test('concurrent readers share the same block download', async () => {
  const counted = countingStorage(), reader = server(counted.storage)
  const responses = await Promise.all(Array.from({ length: 8 }, () => seek(reader, 21)))
  for (const response of responses) { expect(response.status).toBe(200); expect(await response.text()).toBe(payloads[21]) }
  expect(counted.gets()).toBe(0)
  expect(counted.ranges).toHaveLength(1)
})

test('a corrupt later block fails the complete response before any selected data is returned', async () => {
  const corrupt: ChunkStorage = { ...objects, readRange: undefined, async getRange(key, range, signal) {
    const bytes = await objects.getRange(key, range, signal)
    bytes[bytes.length - 1]! ^= 1
    return bytes
  } }
  const response = await seek(server(corrupt, 10), 1)
  expect(response.status).toBe(500)
  expect(await response.text()).not.toContain(payloads[1])
})

test('streaming corruption in a later block fails before returning data', async () => {
  const corrupt: ChunkStorage = { ...objects, async readRange(key, range, consume, signal) {
    const bytes = await objects.getRange(key, range, signal)
    bytes[bytes.length - 1]! ^= 1
    for (let position = 0; position < bytes.length; position += 997) await consume(bytes.subarray(position, position + 997))
  } }
  const response = await seek(server(corrupt, 10), 1)
  expect(response.status).toBe(500)
  expect(await response.text()).not.toContain(payloads[1])
})

test('invalid physical manifest bounds are rejected before issuing object requests', async () => {
  const counted = countingStorage()
  const malformed = Buffer.from(manifest.block_directory)
  malformed.writeUInt32BE(0, 12)
  await direct.query(`UPDATE "${schema}".chunks SET block_directory=$2 WHERE id=$1`, [manifest.id, malformed])
  try {
    expect((await seek(server(counted.storage), 0)).status).toBe(500)
    expect(counted.gets()).toBe(0)
    expect(counted.ranges).toHaveLength(0)
  } finally { await direct.query(`UPDATE "${schema}".chunks SET block_directory=$2 WHERE id=$1`, [manifest.id, manifest.block_directory]) }
})

test('a verified full-object cache entry also serves subsequent interior reads', async () => {
  const counted = countingStorage(), reader = server(counted.storage, 1024)
  expect(await (await seek(reader, 0)).text()).toBe(payloads.join(''))
  expect(await (await seek(reader, 31)).text()).toBe(payloads[31])
  expect(counted.gets()).toBe(1)
  expect(counted.ranges).toHaveLength(0)
})

test('partial JSON replay across a fork cap preserves exact numbers and UTF-8', async () => {
  const writer = server(objects, 1024)
  const json = Array.from({ length: 12 }, (_, index) => `[9007199254740993,1e400,-0,{"index":${index},"text":"${'🐘'.repeat(2048)}"}]`)
  const created = await request(writer, 'json', 'PUT', undefined, { 'Content-Type': 'application/json' })
  expect(created.status).toBe(201)
  const jsonOffsets = [created.headers.get('Stream-Next-Offset')!]
  for (const payload of json) {
    const appended = await request(writer, 'json', 'POST', payload, { 'Content-Type': 'application/json' })
    expect(appended.status).toBe(204)
    jsonOffsets.push(appended.headers.get('Stream-Next-Offset')!)
  }
  expect((await writer.maintain({ forceHot: true, maxChunks: 1 })).sealed).toBe(1)
  expect((await request(writer, 'json-child', 'PUT', undefined, { 'Content-Type': 'application/json',
    'Stream-Forked-From': '/v1/stream/json', 'Stream-Fork-Offset': jsonOffsets[10]! })).status).toBe(201)
  expect((await request(writer, 'json', 'POST', '["later"]', { 'Content-Type': 'application/json' })).status).toBe(204)
  const counted = countingStorage(), reader = server(counted.storage, 8)
  const response = await request(reader, `json-child?offset=${jsonOffsets[8]}`)
  expect(response.status).toBe(200)
  expect(await response.text()).toBe(`[${json[8]!.slice(1, -1)},${json[9]!.slice(1, -1)}]`)
  expect(response.headers.get('Stream-Next-Offset')).toBe(jsonOffsets[10])
  expect(response.headers.get('Stream-Up-To-Date')).toBe('true')
  expect(counted.gets()).toBe(0)
  expect(counted.ranges).toHaveLength(1)
})

test('a cold matching conditional GET acquires no pins and downloads no objects', async () => {
  const initial = await seek(server(), 10)
  const etag = initial.headers.get('ETag')!
  await initial.arrayBuffer()
  const counted = countingStorage()
  const response = await request(server(counted.storage), `binary?offset=${offsets[10]}`, 'GET', undefined, { 'If-None-Match': `W/${etag}` })
  expect(response.status).toBe(304)
  expect(response.headers.get('ETag')).toBe(etag)
  expect(await response.text()).toBe('')
  expect(counted.gets()).toBe(0)
  expect(counted.ranges).toHaveLength(0)
  expect((await direct.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toEqual([])
})

test('validators survive hot-to-sealed publication and change when closure becomes visible', async () => {
  const writer = server()
  expect((await request(writer, 'etag', 'PUT', 'stable bytes')).status).toBe(201)
  const hot = await request(writer, 'etag')
  const etag = hot.headers.get('ETag')!
  await hot.arrayBuffer()
  await writer.maintain({ forceHot: true, maxChunks: 16 })
  const sealed = await request(server(), 'etag', 'GET', undefined, { 'If-None-Match': etag })
  expect(sealed.status).toBe(304)
  expect(sealed.headers.get('ETag')).toBe(etag)
  expect((await request(writer, 'etag', 'POST', '', { 'Stream-Closed': 'true' })).status).toBe(204)
  const closed = await request(server(), 'etag', 'GET', undefined, { 'If-None-Match': etag })
  expect(closed.status).toBe(200)
  expect(closed.headers.get('ETag')).not.toBe(etag)
  expect(closed.headers.get('Stream-Closed')).toBe('true')
  expect(await closed.text()).toBe('stable bytes')
  const now = await request(server(), 'etag?offset=now', 'GET', undefined, { 'If-None-Match': '*' })
  expect(now.status).toBe(200)
  expect(now.headers.has('ETag')).toBe(false)
})

test('canceling one shared reader preserves the other reader and releases all pins', async () => {
  let entered!: () => void, resume!: () => void, reads = 0
  const started = new Promise<void>((resolve) => { entered = resolve })
  const gate = new Promise<void>((resolve) => { resume = resolve })
  const held: ChunkStorage = { ...objects, async readRange(key, range, consume, signal) {
    reads++; entered(); await gate
    return objects.readRange(key, range, consume, signal)
  } }
  const reader = server(held), canceled = new AbortController()
  const first = reader.fetch(new Request(`https://fixture.test/v1/stream/binary?offset=${offsets[21]}`, { signal: canceled.signal }))
  await started
  const second = seek(reader, 21)
  // Both planning transactions must acquire protection before abandoning one.
  const until = performance.now() + 10000
  while (Number((await direct.query(`SELECT count(*) FROM "${schema}".replay_pins`)).rows[0].count) < 2) {
    if (performance.now() > until) throw new Error('second reader never acquired its pin')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  canceled.abort(new Error('reader disconnected'))
  resume()
  await (await first).body?.cancel()
  const response = await second
  expect(await response.text()).toBe(payloads[21])
  expect(reads).toBe(1)
  expect((await direct.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toEqual([])
})

test('last-reader cancellation settles adapter I/O before releasing its replay pin', async () => {
  let entered!: () => void, settle!: () => void, settled = false
  const started = new Promise<void>((resolve) => { entered = resolve })
  const canceledIO = new Promise<void>((resolve) => { settle = resolve })
  const held: ChunkStorage = { ...objects, async readRange(_key, _range, _consume, signal) {
    entered()
    await new Promise<void>((resolve) => { signal!.addEventListener('abort', () => resolve(), { once: true }) })
    await canceledIO
    settled = true
    signal!.throwIfAborted()
  } }
  const reader = server(held), cancel = new AbortController()
  const pending = reader.fetch(new Request(`https://fixture.test/v1/stream/binary?offset=${offsets[21]}`, { signal: cancel.signal }))
  await started
  cancel.abort(new Error('reader disconnected'))
  expect((await direct.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toHaveLength(1)
  settle()
  await (await pending).body?.cancel()
  expect(settled).toBe(true)
  expect((await direct.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toEqual([])
})

test('a rewritten position under a fresh serving epoch cannot reuse the discarded validator', async () => {
  const original = server()
  expect((await request(original, 'rewrite', 'PUT', 'old')).status).toBe(201)
  const before = await request(original, 'rewrite')
  const etag = before.headers.get('ETag')!
  await before.arrayBuffer()
  // Isolate the restore-identity invariant. The recovery suite exercises the
  // provider restore itself. No application request writes these fields.
  await direct.query(`UPDATE "${schema}".installation SET serving_epoch=$1`, [randomUUID()])
  await direct.query(`UPDATE "${schema}".appends a SET payload=$1 FROM "${schema}".streams s WHERE a.stream_id=s.id AND s.path='rewrite'`, [Buffer.from('new')])
  expect((await request(original, 'rewrite')).status).toBe(503)
  const after = await request(server(), 'rewrite', 'GET', undefined, { 'If-None-Match': etag })
  expect(after.status).toBe(200)
  expect(after.headers.get('ETag')).not.toBe(etag)
  expect(await after.text()).toBe('new')
})
