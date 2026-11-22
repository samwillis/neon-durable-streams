import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { InstallationGuard } from '../../src/installation.js'
import { migrate } from '../../src/migrations.js'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../../src/operations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET
  || !env.NEON_REALTIME_URL || !env.NEON_REALTIME_SECRET || !env.DURABLE_STREAMS_KEYRING) throw new Error('Operations qualification requires real Neon services')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 8 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const notifier = createNeonRealtimeNotifier({ url: env.NEON_REALTIME_URL, secret: env.NEON_REALTIME_SECRET, database: new URL(env.TEST_DATABASE_URL).pathname.slice(1) })
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING)
const servers: DurableStreamsServer[] = []
let environmentId = 'original-environment'
let expectedStorage = objects
const errors: unknown[] = []
const server = (storage: ChunkStorage = expectedStorage, environment = environmentId) => {
  const handler = createDurableStreams({ postgres: { pool, schema, environmentId: environment }, storage: { objects: storage, targetChunkBytes: 16 },
    auth: { authenticate: () => ({ tenantId: 'ops', subject: 'fixture' }), authorize: () => true },
    live: { notifier, fallbackIntervalMs: 100 }, subscriptions: { keyring }, onError: (error) => errors.push(error) })
  servers.push(handler)
  return handler
}
const request = (handler: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>((accept) => { resolve = accept }); return { promise, resolve } }
async function poll(check: () => Promise<boolean>) {
  const deadline = Date.now() + 10_000
  while (!await check()) { if (Date.now() > deadline) throw new Error('Operations barrier timed out'); await new Promise((resolve) => setTimeout(resolve, 25)) }
}
async function startRecovery() {
  const identity = await inspectInstallation(pool, { schema })
  return beginRecovery(direct, { schema, operationId: randomUUID(), expectedInstallationId: identity.id, expectedServingEpoch: identity.serving_epoch, environmentId, storageIdentity: expectedStorage.identity })
}
beforeAll(async () => {
  await migrate(direct, { schema, environmentId, storageIdentity: objects.identity, maxLiveReaders: 2 })
  await initializeSubscriptions(direct, { schema, keyring })
})
afterAll(async () => {
  await Promise.all(servers.map((handler) => handler.close()))
  notifier.close(); objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end(); await direct.end()
  expect(errors).toEqual([])
})

test('installation-wide reader slots cover separate server instances and release on cancellation', async () => {
  const one = server(), two = server(), three = server()
  expect((await request(one, 'live', 'PUT')).status).toBe(201)
  const a = await request(one, 'live?live=sse&offset=now')
  const b = await request(two, 'live?live=sse&offset=now')
  expect(a.status).toBe(200); expect(b.status).toBe(200)
  expect((await request(three, 'live?live=sse&offset=now')).status).toBe(429)
  await a.body!.cancel()
  await poll(async () => (await pool.query(`SELECT count(*) FROM "${schema}".live_leases`)).rows[0].count === '1')
  const c = await request(three, 'live?live=sse&offset=now')
  expect(c.status).toBe(200)
  await b.body!.cancel(); await c.body!.cancel()
  await Promise.all([one.close(), two.close(), three.close()])
  expect((await pool.query(`SELECT count(*) FROM "${schema}".live_leases`)).rows[0].count).toBe('0')
})

test('database expiry recovers abandoned admission slots after process loss', async () => {
  const { serving_epoch: epoch } = await inspectInstallation(pool, { schema })
  await pool.query(`INSERT INTO "${schema}".live_leases (id, serving_epoch, expires_at) VALUES ($1, $2, clock_timestamp() - interval '1 second')`, [randomUUID(), epoch])
  const handler = server()
  const response = await request(handler, 'live?live=sse&offset=now')
  expect(response.status).toBe(200)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".live_leases`)).rows[0].count).toBe('1')
  await response.body!.cancel(); await handler.close()
})

test('quiescence waits for writers, fences cached runtimes, and resumes only after verification', async () => {
  const old = server()
  expect((await request(old, 'before', 'PUT', 'preserved sealed data')).status).toBe(201)
  await old.maintain()
  const entered = deferred(), resume = deferred()
  const guard = new InstallationGuard(pool, schema, environmentId)
  const writer = guard.run({}, async (client) => { await client.query('SELECT 1'); entered.resolve(); await resume.promise })
  await entered.promise
  let finished = false
  const identity = await inspectInstallation(pool, { schema })
  const recoveryOptions = { schema, operationId: randomUUID(), expectedInstallationId: identity.id,
    expectedServingEpoch: identity.serving_epoch, environmentId, storageIdentity: objects.identity }
  const recovering = beginRecovery(direct, recoveryOptions).then((result) => { finished = true; return result })
  try {
    await poll(async () => (await direct.query(`SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE $1`, [`SELECT * FROM "${schema}".installation FOR UPDATE%`])).rowCount! > 0)
    expect(finished).toBe(false)
  } finally { resume.resolve() }
  await writer
  const recovery = await recovering
  const { token } = recovery
  expect(await beginRecovery(direct, recoveryOptions)).toEqual(recovery)
  await expect(beginRecovery(direct, { ...recoveryOptions, environmentId: 'wrong-retry-target' })).rejects.toThrow('Installation changed')
  expect((await request(old, 'before', 'POST', 'blocked')).status).toBe(503)
  await expect(finishRecovery(pool, { schema, token })).rejects.toThrow('Verify every sealed manifest')
  await expect(verifyRecovery(pool, { schema, token, objects })).rejects.toThrow('wrapping keyring')
  const corrupt = { ...objects, async get(key: string, maximum: number) { const bytes = Buffer.from(await objects.get(key, maximum)); bytes[bytes.length - 1]! ^= 1; return bytes } }
  await expect(verifyRecovery(pool, { schema, token, keyring, objects: corrupt })).rejects.toThrow()
  expect((await pool.query(`SELECT recovery_verified FROM "${schema}".installation`)).rows[0].recovery_verified).toBe('0')
  const checked = await verifyRecovery(pool, { schema, token, keyring, objects })
  expect(checked).toEqual({ complete: true, verified: 1 })
  await finishRecovery(pool, { schema, token })
  expect((await request(old, 'before')).status).toBe(503)
  expect(await (await request(server(), 'before')).text()).toBe('preserved sealed data')
  expect((await finishRecovery(pool, { schema, token })).serving_state).toBe('active')
})

test('a worker paused outside SQL cannot publish after the serving epoch changes', async () => {
  const entered = deferred(), resume = deferred()
  let blocked = false
  const slow = server({ ...objects, async get(key, maximum) { const bytes = await objects.get(key, maximum); if (!blocked) { blocked = true; entered.resolve(); await resume.promise } return bytes } })
  expect((await request(slow, 'worker', 'PUT', 'frozen data preserved')).status).toBe(201)
  const work = slow.maintain({ maxChunks: 1 }).then(() => null, (error: unknown) => error)
  await entered.promise
  const { token } = await startRecovery()
  resume.resolve()
  expect(await work).toMatchObject({ code: 'INSTALLATION_FENCED' })
  const pending = await pool.query(`SELECT state FROM "${schema}".chunks c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = 'worker'`)
  expect(pending.rows).toEqual([{ state: 'frozen' }])
  await verifyRecovery(pool, { schema, token, keyring, objects }); await finishRecovery(pool, { schema, token })
  const fresh = server()
  expect((await fresh.maintain()).sealed).toBe(1)
  expect(await (await request(fresh, 'worker')).text()).toBe('frozen data preserved')
})

test('adoption binds the new environment and rejects matching data with stale configuration', async () => {
  const old = server()
  await old.inspectHealth()
  environmentId = 'adopted-environment'
  expectedStorage = { ...objects, identity: `${objects.identity}:adopted` }
  const { token } = await startRecovery()
  await expect(verifyRecovery(pool, { schema, token, keyring, objects })).rejects.toThrow('recorded target')
  let result
  do { result = await verifyRecovery(pool, { schema, token, keyring, objects: expectedStorage, maxChunks: 1 }) } while (!result.complete)
  expect(result.verified).toBe(2)
  const identity = await finishRecovery(pool, { schema, token })
  expect(identity.environment_id).toBe(environmentId)
  expect((await request(old, 'before')).status).toBe(503)
  expect(await (await request(server(), 'before')).text()).toBe('preserved sealed data')
  const wrong = server(expectedStorage, 'original-environment')
  expect((await request(wrong, 'before')).status).toBe(500)
  expect(errors.pop()).toMatchObject({ name: 'StorageCompatibilityError' })
})
