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
  const corrupt: ChunkStorage = { ...objects, async getRange(key, range, signal) {
    const bytes = await objects.getRange(key, range, signal)
    bytes[bytes.length - 1]! ^= 1
    return bytes
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
