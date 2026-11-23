import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { migrate } from '../../src/migrations.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { InstallationGuard } from '../../src/installation.js'
import { ReclamationManager } from '../../src/reclamation.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Replay concurrency tests require actual Neon')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 6 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const servers: DurableStreamsServer[] = []
function server(database = pool, storage: ChunkStorage = objects) {
  const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool: database, schema },
    storage: { objects: storage, targetChunkBytes: 1024 * 1024 },
    auth: { authenticate: () => ({ tenantId: 'replay', subject: 'fixture' }), authorize: () => true } })
  servers.push(value)
  return value
}
const normal = server()
const request = (handler: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))

function pausedReader(path: string) {
  let enter!: () => void, release!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve })
  const gate = new Promise<void>((resolve) => { release = resolve })
  let armed = true
  const database = {
    query: pool.query.bind(pool),
    async connect() {
      const client = await pool.connect()
      return {
        release: client.release.bind(client),
        async query(text: string, parameters?: unknown[]) {
          const result = await client.query(text, parameters)
          if (armed && text.includes('.paths p JOIN') && parameters?.[1] === path) {
            armed = false
            enter()
            await gate
          }
          return result
        },
      }
    },
  } as unknown as pg.Pool
  return { handler: server(database), entered, release }
}

beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }) })
afterAll(async () => {
  await Promise.all(servers.map((handler) => handler.close()))
  objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
})

test('a non-TTL read holds a consistent snapshot while an appender commits', async () => {
  const created = await request(normal, 'append', 'PUT', 'before')
  const paused = pausedReader('append')
  const reading = request(paused.handler, 'append')
  try {
    await paused.entered
    const appended = await request(normal, 'append', 'POST', 'after')
    expect(appended.status).toBe(204)
    // The appender committed while the reader still owns its planning snapshot.
  } finally { paused.release() }
  const response = await reading
  expect(response.status).toBe(200)
  expect(await response.text()).toBe('before')
  expect(response.headers.get('Stream-Next-Offset')).toBe(created.headers.get('Stream-Next-Offset'))
  expect(await (await request(normal, 'append')).text()).toBe('beforeafter')
})

test('sealing after a read snapshot preserves its captured staging bytes', async () => {
  await request(normal, 'seal', 'PUT', 'captured before sealing')
  const paused = pausedReader('seal')
  const reading = request(paused.handler, 'seal')
  try {
    await paused.entered
    expect((await normal.maintain({ forceHot: true, maxChunks: 128 })).sealed).toBeGreaterThan(0)
    expect((await pool.query(`SELECT count(*) FROM "${schema}".appends a JOIN "${schema}".streams s ON a.stream_id=s.id WHERE s.path='seal'`)).rows[0].count).toBe('0')
  } finally { paused.release() }
  expect(await (await reading).text()).toBe('captured before sealing')
  expect(await (await request(normal, 'seal')).text()).toBe('captured before sealing')
})

test('delete and recreation during planning cannot mix generation metadata and bytes', async () => {
  await request(normal, 'recreate', 'PUT', 'original')
  const paused = pausedReader('recreate')
  const reading = request(paused.handler, 'recreate')
  try {
    await paused.entered
    expect((await request(normal, 'recreate', 'DELETE')).status).toBe(204)
    expect((await request(normal, 'recreate', 'PUT', 'replacement')).status).toBe(201)
  } finally { paused.release() }
  expect(await (await reading).text()).toBe('original')
  expect(await (await request(normal, 'recreate')).text()).toBe('replacement')
})

test('sliding TTL replay renews access and its durable expiry schedule atomically', async () => {
  await request(normal, 'ttl', 'PUT', 'renew', { 'Stream-TTL': '60' })
  await pool.query(`UPDATE "${schema}".streams SET accessed_at=clock_timestamp()-interval '30 seconds' WHERE path='ttl'`)
  expect(await (await request(normal, 'ttl')).text()).toBe('renew')
  const result = await pool.query(`SELECT s.accessed_at>clock_timestamp()-interval '10 seconds' AS renewed,
    e.due_at=s.accessed_at+interval '60 seconds' AS scheduled FROM "${schema}".streams s
    JOIN "${schema}".stream_expirations e ON e.stream_id=s.id WHERE s.path='ttl'`)
  expect(result.rows).toEqual([{ renewed: true, scheduled: true }])
})

test('retirement after a captured snapshot rejects pin acquisition without leaked pins', async () => {
  await request(normal, 'retire', 'PUT', 'retire after capture')
  await normal.maintain({ forceHot: true, maxChunks: 128 })
  const paused = pausedReader('retire')
  const reading = request(paused.handler, 'retire')
  try {
    await paused.entered
    expect((await request(normal, 'retire', 'DELETE')).status).toBe(204)
    await direct.query(`UPDATE "${schema}".reclamation_policy SET enabled=true,work_phase=0`)
    const collector = new ReclamationManager(`"${schema}"`, new InstallationGuard(pool, schema), objects)
    expect((await collector.maintain()).reclaimedChunks).toBeGreaterThan(0)
    await direct.query(`UPDATE "${schema}".reclamation_policy SET enabled=false`)
  } finally { paused.release() }
  const response = await reading
  expect(response.status).toBe(503)
  expect((await response.json()).error.code).toBe('REPLAY_RETRY')
  expect((await pool.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toEqual([])
})

test('expired replay pins fail the whole response and are released after validation rolls back', async () => {
  await request(normal, 'expired-pin', 'PUT', 'protected bytes')
  await normal.maintain({ forceHot: true, maxChunks: 128 })
  const expired = server(pool, { ...objects, async get(key, maximum) {
    const bytes = await objects.get(key, maximum)
    await direct.query(`UPDATE "${schema}".replay_pins SET expires_at=clock_timestamp()-interval '1 second'`)
    return bytes
  } })
  const response = await request(expired, 'expired-pin')
  expect(response.status).toBe(503)
  expect((await response.json()).error.code).toBe('REPLAY_EXPIRED')
  expect((await pool.query(`SELECT * FROM "${schema}".replay_pins`)).rows).toEqual([])
  expect(await (await request(normal, 'expired-pin')).text()).toBe('protected bytes')
})
