import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, afterEach, beforeAll, expect, test, vi } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer, ChunkStorage } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { beginRecovery, inspectInstallation, finishRecovery, verifyRecovery } from '../../src/operations.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { chunkDigest, encodeChunk } from '../../src/chunk-codec.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Reclamation requires actual Neon services')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 5, connectionTimeoutMillis: 20_000 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 3, connectionTimeoutMillis: 20_000 })
pool.on('error', () => {}); direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const servers: DurableStreamsServer[] = []
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>((accept) => { resolve = accept }); return { promise, resolve } }
const request = (server: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => server.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))
function server(storage: ChunkStorage = objects, clientPool = pool) {
  const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool: clientPool, schema, environmentId: 'reclamation-test' }, storage: { objects: storage, targetChunkBytes: 4 },
    auth: { authenticate: () => ({ tenantId: 'reclamation', subject: 'fixture' }), authorize: () => true } })
  servers.push(value); return value
}
async function sealed(path: string, body = 'exact sealed bytes') {
  const value = server()
  expect((await request(value, path, 'PUT', body)).status).toBe(201)
  await value.maintain()
  return value
}
async function pins() { return Number((await direct.query(`SELECT count(*) FROM "${schema}".replay_pins`)).rows[0].count) }
function blockedStorage() {
  const entered = deferred(), release = deferred()
  const storage: ChunkStorage = { ...objects, async get(key, maximum) { const bytes = await objects.get(key, maximum); entered.resolve(); await release.promise; return bytes } }
  return { storage, entered, release }
}
beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity, environmentId: 'reclamation-test' }) })
afterEach(async () => { vi.restoreAllMocks(); await Promise.all(servers.splice(0).map((value) => value.close())) })
afterAll(async () => { objects.close(); await direct.query(`DROP SCHEMA "${schema}" CASCADE`); await pool.end(); await direct.end() })

test('snapshot pins survive epoch fencing, block recovery activation, and release after the old replay fails closed', async () => {
  await sealed('pinned')
  expect((await direct.query(`SELECT enabled FROM "${schema}".reclamation_policy`)).rows[0].enabled).toBe(false)
  const block = blockedStorage(), reader = server(block.storage)
  const response = request(reader, 'pinned')
  try {
    await block.entered.promise
    expect(await pins()).toBe(1)
    // Recovery can acquire its installation lock while object I/O is paused;
    // no replay transaction or installation lock remains open across the get.
    const installation = await inspectInstallation(direct, { schema })
    const recovery = await beginRecovery(direct, { schema, operationId: randomUUID(), expectedInstallationId: installation.id,
      expectedServingEpoch: installation.serving_epoch, environmentId: 'reclamation-test', storageIdentity: objects.identity })
    await verifyRecovery(direct, { schema, token: recovery.token, objects })
    await expect(finishRecovery(direct, { schema, token: recovery.token })).rejects.toThrow('replay readers')
    block.release.resolve()
    expect((await response).status).toBe(503)
    expect(await pins()).toBe(0)
    await finishRecovery(direct, { schema, token: recovery.token })
  } finally { block.release.resolve(); await response }
})

test('a sealed page holds a durable pin only during materialization and returns exact bytes', async () => {
  await sealed('exact')
  const block = blockedStorage(), reader = server(block.storage)
  const response = request(reader, 'exact')
  try {
    await Promise.race([block.entered.promise, response.then((value) => { throw new Error(`Replay returned HTTP ${value.status} before object I/O`) })])
    expect(await pins()).toBe(1)
    block.release.resolve()
    const result = await response
    expect(result.status).toBe(200); expect(await result.text()).toBe('exact sealed bytes')
    expect(await pins()).toBe(0)
  } finally { block.release.resolve(); await response }
})

test('a suspended materialization cannot emit after its monotonic deadline and releases its pin', async () => {
  await sealed('deadline')
  let now = performance.now()
  vi.spyOn(performance, 'now').mockImplementation(() => now)
  const block = blockedStorage(), reader = server(block.storage)
  const response = request(reader, 'deadline')
  try {
    await Promise.race([block.entered.promise, response.then((value) => { throw new Error(`Replay returned HTTP ${value.status} before object I/O`) })])
    expect(await pins()).toBe(1)
    now += 60_001
    block.release.resolve()
    const result = await response
    expect(result.status).toBe(503); expect((await result.json() as { error: { code: string } }).error.code).toBe('REPLAY_DEADLINE')
    expect(await pins()).toBe(0)
  } finally { block.release.resolve(); await response }
})

test('partial fork preparation pins sealed bytes but cannot publish an edge onto a recreated source', async () => {
  const parent = await sealed('partial-source')
  const block = blockedStorage(), reader = server(block.storage)
  const response = request(reader, 'partial-child', 'PUT', undefined, {
    'Stream-Forked-From': '/v1/stream/partial-source', 'Stream-Fork-Offset': '0000000000000000_0000000000000000', 'Stream-Fork-Sub-Offset': '3',
  })
  try {
    await Promise.race([block.entered.promise, response.then((value) => { throw new Error(`Fork returned HTTP ${value.status} before object I/O`) })])
    expect(await pins()).toBe(1)
    expect((await request(parent, 'partial-source', 'DELETE')).status).toBe(204)
    expect((await request(parent, 'partial-source', 'PUT', 'replacement source')).status).toBe(201)
    block.release.resolve()
    expect((await response).status).toBe(409)
    expect(await pins()).toBe(0)
    expect((await request(parent, 'partial-child')).status).toBe(404)
  } finally { block.release.resolve(); await response }
})

test.each(['pin release', 'final validation'] as const)('a replay cannot return after %s exhausts its deadline', async (stage) => {
  const path = stage === 'pin release' ? 'release-deadline' : 'validation-deadline'
  await sealed(path)
  let now = performance.now(), released = false, delayed = false
  vi.spyOn(performance, 'now').mockImplementation(() => now)
  const delayedPool = {
    async query(text: string, parameters?: unknown[]) {
      const result = await pool.query(text, parameters)
      if (stage === 'final validation' && released && !delayed && text.includes('.installation')) { now += 60_001; delayed = true }
      return result
    },
    async connect() {
      const client = await pool.connect()
      return { release: client.release.bind(client), async query(text: string, parameters?: unknown[]) {
        const result = await client.query(text, parameters)
        if (text.includes('.replay_pins WHERE id = $1')) {
          released = true
          if (stage === 'pin release') { now += 60_001; delayed = true }
        }
        return result
      } }
    },
  } as unknown as pg.Pool
  const reader = server(objects, delayedPool), response = await request(reader, path)
  expect(delayed).toBe(true)
  expect(response.status).toBe(503)
  expect((await response.json() as { error: { code: string } }).error.code).toBe('REPLAY_DEADLINE')
  expect(await pins()).toBe(0)
})

test('Neon Object Storage provides bounded prefix inventory and idempotent exact-key deletion', async () => {
  const installationId = randomUUID(), prefix = `v1/${installationId}/`, keys: string[] = []
  try {
    for (let i = 0; i < 2; i++) {
      const streamId = randomUUID(), chunkId = randomUUID(), payload = Buffer.from(`inventory-${i}`)
      const bytes = encodeChunk({ installationId, streamId, chunkId, frames: [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET), payload, messageEnds: null }] })
      const key = `${prefix}${streamId}/${chunkId}/${chunkDigest(bytes)}.ndsc`
      keys.push(key)
      await objects.put(key, bytes)
    }
    const found: string[] = []
    let cursor: string | null = null
    for (let page = 0; page < 3; page++) {
      const inventory = await objects.list(prefix, cursor, 1)
      expect(inventory.objects.length).toBeLessThanOrEqual(1)
      for (const object of inventory.objects) { expect(object.size).toBeGreaterThan(0); found.push(object.key) }
      cursor = inventory.cursor
      if (cursor === null) break
    }
    expect(cursor).toBe(null)
    expect(found.toSorted()).toEqual(keys.toSorted())
    for (const key of keys) { await objects.delete(key); await objects.delete(key) }
    expect((await objects.list(prefix, null, 128)).objects).toEqual([])
    expect(() => objects.delete('unscoped-object')).toThrow('exact immutable chunk key')
  } finally { await Promise.all(keys.map((key) => objects.delete(key))) }
})

test('signed wake tokens reject stale work after wake and subscription pruning without authorizing ordinary reads', async () => {
  const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING!)
  await initializeSubscriptions(direct, { schema, keyring })
  const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema, environmentId: 'reclamation-test' }, storage: { objects, targetChunkBytes: 4 },
    subscriptions: { keyring }, auth: { authenticate: (request) => request.headers.get('authorization') === 'Bearer operator' ? { tenantId: 'reclamation', subject: 'fixture' } : null,
      authorize: () => true } })
  servers.push(value)
  const api = (path: string, method = 'GET', body?: string, headers: Record<string, string> = {}) => request(value, path, method, body, { Authorization: 'Bearer operator', ...headers })
  expect((await api('tokens/source', 'PUT')).status).toBe(201)
  expect((await api('tokens/wake', 'PUT', '[]', { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await api('__ds/subscriptions/signed', 'PUT', JSON.stringify({ type: 'pull-wake', streams: ['tokens/source'], wake_stream: 'tokens/wake' }), { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await api('tokens/source', 'POST', 'payload')).status).toBe(204)
  await value.maintain()
  const claimed = await api('__ds/subscriptions/signed/claim', 'POST', '{"worker":"fixture"}', { 'Content-Type': 'application/json' })
  expect(claimed.status).toBe(200)
  const claim = await claimed.json() as { token: string; wake_id: string; generation: number; streams: { path: string; tail_offset: string }[] }
  expect(claim.token).toMatch(/^dsw\.2\./)
  const body = JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, done: true,
    acks: [{ stream: 'tokens/source', offset: claim.streams[0]!.tail_offset }] })
  const scoped = { 'Content-Type': 'application/json', Authorization: `Bearer ${claim.token}` }
  expect((await api('tokens/source', 'GET', undefined, scoped)).status).toBe(401)
  expect((await api('__ds/subscriptions/signed/ack', 'POST', body, scoped)).status).toBe(200)
  await direct.query(`DELETE FROM "${schema}".subscription_wake_streams WHERE wake_id=$1`, [claim.wake_id.slice(2)])
  await direct.query(`DELETE FROM "${schema}".subscription_wakes WHERE id=$1`, [claim.wake_id.slice(2)])
  expect((await api('__ds/subscriptions/signed/ack', 'POST', body, scoped)).status).toBe(409)
  expect((await api('__ds/subscriptions/signed', 'DELETE')).status).toBe(204)
  await direct.query(`DELETE FROM "${schema}".subscriptions WHERE name='signed'`)
  expect((await api('__ds/subscriptions/signed/ack', 'POST', body, scoped)).status).toBe(409)
  const tampered = claim.token.slice(0, -1) + (claim.token.endsWith('A') ? 'B' : 'A')
  expect((await api('__ds/subscriptions/signed/ack', 'POST', body, { ...scoped, Authorization: `Bearer ${tampered}` })).status).toBe(401)
})
