import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { migrate } from '../../src/migrations.js'
import { ZERO_OFFSET } from '../../src/offset.js'

const { TEST_DATABASE_URL, TEST_DATABASE_URL_UNPOOLED, AWS_ENDPOINT_URL_S3, AWS_REGION, NEON_TEST_BUCKET } = process.env
if (!TEST_DATABASE_URL || !TEST_DATABASE_URL_UNPOOLED || !AWS_ENDPOINT_URL_S3 || !AWS_REGION || !NEON_TEST_BUCKET) throw new Error('Fork qualification requires real Neon Postgres and Object Storage')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 12 })
const direct = new pg.Pool({ connectionString: TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: AWS_ENDPOINT_URL_S3, region: AWS_REGION, bucket: NEON_TEST_BUCKET })
const errors: unknown[] = []
const servers: DurableStreamsServer[] = []
const server = (storage: ChunkStorage = objects, denySourceRead = false) => {
  const handler = createDurableStreams({ postgres: { pool, schema }, storage: { objects: storage, targetChunkBytes: 8, maxChunkFrames: 4 },
    auth: { authenticate: (request) => ({ tenantId: request.headers.get('authorization') === 'Bearer b' ? 'b' : 'a', subject: 'fixture' }),
      authorize: ({ path, operation }) => !(denySourceRead && path === 'restricted' && operation === 'read') }, onError: (error) => errors.push(error) })
  servers.push(handler)
  return handler
}
const handler = server()
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>, target = handler) => target.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
const fork = (path: string, source: string, headers?: Record<string, string>, target = handler) => request(path, 'PUT', undefined, { 'Stream-Forked-From': `/v1/stream/${source}`, ...headers }, target)
async function assertReferences() {
  const result = await pool.query(`SELECT s.id FROM "${schema}".streams s WHERE s.ref_count <> (
    SELECT count(*) FROM "${schema}".streams f WHERE f.source_id = s.id AND f.edge_active)`)
  expect(result.rows).toEqual([])
}
beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }) })
afterAll(async () => {
  await Promise.all(servers.map((value) => value.close()))
  objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
  expect(errors).toEqual([])
})

test('concurrent duplicate partial forks acquire exactly one parent reference across sealed JSON', async () => {
  await request('json-source', 'PUT', '[9007199254740993,1e400,-0,"🐘"]', { 'Content-Type': 'application/json' })
  await handler.maintain({ maxChunks: 16 })
  const responses = await Promise.all(Array.from({ length: 12 }, () => fork('json-child', 'json-source', {
    'Content-Type': 'application/json', 'Stream-Fork-Offset': ZERO_OFFSET, 'Stream-Fork-Sub-Offset': '2',
  })))
  expect(responses.filter((response) => response.status === 201)).toHaveLength(1)
  expect(responses.filter((response) => response.status === 200)).toHaveLength(11)
  expect(await (await request('json-child')).text()).toBe('[9007199254740993,1e400]')
  expect((await pool.query(`SELECT ref_count FROM "${schema}".streams WHERE path = 'json-source'`)).rows[0].ref_count).toBe(1)
  await assertReferences()
})

test('fork creation racing source deletion either preserves the fixed generation or creates no fork', async () => {
  for (let i = 0; i < 8; i++) {
    const source = `delete-race-${i}`, child = `delete-race-child-${i}`
    await request(source, 'PUT', 'stable source bytes')
    const [created, removed] = await Promise.all([fork(child, source), request(source, 'DELETE')])
    expect(removed.status).toBe(204)
    expect([201, 404]).toContain(created.status)
    if (created.status === 201) {
      expect((await request(source, 'HEAD')).status).toBe(410)
      expect(await (await request(child)).text()).toBe('stable source bytes')
    } else expect((await request(child, 'HEAD')).status).toBe(404)
  }
  await assertReferences()
})

test('a prepared sealed prefix cannot be attached to a replacement source generation', async () => {
  await request('prefix-race', 'PUT', 'old sealed bytes')
  await handler.maintain({ maxChunks: 128 })
  let reached: () => void
  let resume: () => void
  const barrier = new Promise<void>((resolve) => { reached = resolve })
  const gate = new Promise<void>((resolve) => { resume = resolve })
  const delayed = server({ ...objects, async get(key, maximum) {
    const bytes = await objects.get(key, maximum)
    reached(); await gate
    return bytes
  } })
  const pending = fork('prefix-race-child', 'prefix-race', { 'Stream-Fork-Offset': ZERO_OFFSET, 'Stream-Fork-Sub-Offset': '3' }, delayed)
  try {
    await barrier
    expect((await request('prefix-race', 'DELETE')).status).toBe(204)
    expect((await request('prefix-race', 'PUT', 'replacement bytes')).status).toBe(201)
  } finally { resume!() }
  expect((await pending).status).toBe(409)
  expect((await request('prefix-race-child', 'HEAD')).status).toBe(404)
  expect(await (await request('prefix-race')).text()).toBe('replacement bytes')
  await assertReferences()
})

test('cascade cleanup retains inherited sealed data until the last descendant is removed', async () => {
  await request('cascade-root', 'PUT', 'root----')
  await handler.maintain({ maxChunks: 128 })
  await fork('cascade-child', 'cascade-root')
  await request('cascade-child', 'POST', 'child---')
  await fork('cascade-grandchild', 'cascade-child')
  await request('cascade-root', 'DELETE')
  await request('cascade-child', 'DELETE')
  expect((await request('cascade-root', 'HEAD')).status).toBe(410)
  expect((await request('cascade-child', 'HEAD')).status).toBe(410)
  expect(await (await request('cascade-grandchild')).text()).toBe('root----child---')
  await request('cascade-grandchild', 'DELETE')
  await handler.maintain({ maxChunks: 128 })
  expect((await request('cascade-root', 'HEAD')).status).toBe(404)
  expect((await request('cascade-child', 'HEAD')).status).toBe(404)
  expect((await request('cascade-root', 'PUT', 'new root')).status).toBe(201)
  expect(await (await request('cascade-root')).text()).toBe('new root')
  await assertReferences()
})

test('an already captured sealed replay survives concurrent logical cleanup and path recreation', async () => {
  await request('snapshot-source', 'PUT', 'immutable prior bytes')
  await fork('snapshot-child', 'snapshot-source')
  await handler.maintain({ maxChunks: 128 })
  let reached: () => void
  let resume: () => void
  const barrier = new Promise<void>((resolve) => { reached = resolve })
  const gate = new Promise<void>((resolve) => { resume = resolve })
  const delayed = server({ ...objects, async get(key, maximum) { const bytes = await objects.get(key, maximum); reached(); await gate; return bytes } })
  const replay = request('snapshot-child', 'GET', undefined, undefined, delayed)
  try {
    await barrier
    await request('snapshot-source', 'DELETE')
    await request('snapshot-child', 'DELETE')
    await handler.maintain({ maxChunks: 128 })
    expect((await request('snapshot-child', 'PUT', 'fresh generation')).status).toBe(201)
  } finally { resume!() }
  expect(await (await replay).text()).toBe('immutable prior bytes')
  expect(await (await request('snapshot-child')).text()).toBe('fresh generation')
  await assertReferences()
})

test('forking requires source-read authorization and cannot cross a tenant boundary', async () => {
  await request('restricted', 'PUT', 'secret source')
  expect((await fork('denied', 'restricted', undefined, server(objects, true))).status).toBe(403)
  expect((await fork('other-tenant', 'restricted', { Authorization: 'Bearer b' })).status).toBe(404)
  expect((await request('denied', 'HEAD')).status).toBe(404)
  expect((await pool.query(`SELECT ref_count FROM "${schema}".streams WHERE path = 'restricted'`)).rows[0].ref_count).toBe(0)
  await assertReferences()
})
