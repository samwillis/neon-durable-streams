import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'
import { transaction } from '../../src/transaction.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET || !env.DURABLE_STREAMS_KEYRING) throw new Error('Maintenance qualification requires real Neon services')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 4 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {}); direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING)
const errors: unknown[] = []
const handler = createDurableStreams({ postgres: { pool, schema, ...postgresTestLimits }, storage: { objects, targetChunkBytes: 32, maxChunkFrames: 4 },
  subscriptions: { keyring }, auth: { authenticate: () => ({ tenantId: 'maintenance', subject: 'fixture' }), authorize: () => true }, onError: (error) => errors.push(error) })
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, { method,
  headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}) }))
beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }); await initializeSubscriptions(direct, { schema, keyring }) })
afterAll(async () => {
  await handler.close(); objects.close(); await direct.query(`DROP SCHEMA "${schema}" CASCADE`); await pool.end(); await direct.end()
  expect(errors).toEqual([])
})

test('a hint uploads full frozen chunks while preserving small hot tails', async () => {
  expect((await request('hot', 'PUT', 'hot')).status).toBe(201)
  await handler.maintain({ maxChunks: 128, timeBudgetMs: 1 })
  let data = await pool.query(`SELECT a.payload FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id = a.stream_id WHERE s.path = 'hot'`)
  expect(data.rows).toEqual([{ payload: Buffer.from('hot') }])
  await request('hot', 'POST', 'a complete full chunk of payload bytes')
  expect((await handler.maintain({ maxChunks: 128, timeBudgetMs: 1 })).sealed).toBe(1)
  data = await pool.query(`SELECT a.payload FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id = a.stream_id WHERE s.path = 'hot'`)
  expect(data.rows).toEqual([])
  expect((await pool.query(`SELECT c.frame_count FROM "${schema}".chunks c JOIN "${schema}".streams s ON s.id=c.stream_id WHERE s.path='hot'`)).rows).toEqual([{ frame_count: 2 }])
})

test('a tiny soft budget still gives wake publication and sealing a turn under outbox backlog', async () => {
  await request('fair/source', 'PUT')
  await request('wake', 'PUT', '[]', { 'Content-Type': 'application/json' })
  expect((await request('__ds/subscriptions/fair', 'PUT', JSON.stringify({ type: 'pull-wake', pattern: 'fair/*', wake_stream: 'wake' }), { 'Content-Type': 'application/json' })).status).toBe(201)
  await handler.maintain({ maxChunks: 8, maxSubscriptionWork: 16 })
  const stream = (await pool.query<{ id: string; change_version: string }>(`SELECT id, change_version FROM "${schema}".streams WHERE path = 'fair/source'`)).rows[0]!
  let offset = ZERO_OFFSET
  const frames = Array.from({ length: 130 }, (_, i) => {
    const start = offset; offset = advanceOffset(offset, 1)
    return { start, end: offset, version: String(BigInt(stream.change_version) + BigInt(i + 1)) }
  })
  // Seed valid committed append intent in one setup transaction. Automatic
  // rotation is exercised by the following ordinary HTTP append.
  await transaction(pool, {}, async (client) => {
    await client.query(`INSERT INTO "${schema}".appends (stream_id,start_offset,end_offset,payload,payload_bytes)
      SELECT $1,x.start,x.end,$3,1 FROM jsonb_to_recordset($2::jsonb) x(start text,"end" text)`, [stream.id, JSON.stringify(frames), Buffer.from('x')])
    await client.query(`INSERT INTO "${schema}".stream_changes (stream_id,version,before_offset,after_offset,kind)
      SELECT $1,x.version::bigint,x.start,x.end,'appended' FROM jsonb_to_recordset($2::jsonb) x(start text,"end" text,version text)`, [stream.id, JSON.stringify(frames)])
    await client.query(`UPDATE "${schema}".streams SET tail_offset=$2, change_version=change_version+130,hot_frames=130,hot_bytes=130 WHERE id=$1`, [stream.id, offset])
  })
  expect((await request('fair/source', 'POST', 'x')).status).toBe(204)
  const stats = await handler.maintain({ maxChunks: 128, maxSubscriptionWork: 128, timeBudgetMs: 1 })
  expect(stats.changes).toBe(128)
  expect(stats.sealed).toBe(1)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".stream_changes WHERE stream_id=$1`, [stream.id])).rows[0].count).toBe('3')
  // Completed outbox work must not grow with every append. The three pending
  // changes still carry the next transaction's durable delivery intent.
  expect((await pool.query(`SELECT count(*) FROM "${schema}".stream_changes WHERE stream_id=$1`, [stream.id])).rows[0].count).toBe('3')
  const wake = await request('wake')
  expect((await wake.json() as { type: string }[]).some((value) => value.type === 'wake')).toBe(true)
  const first = await request('fair/source')
  expect(await first.text()).toBe('x'.repeat(128))
  expect(first.headers.has('Stream-Up-To-Date')).toBe(false)
  const rest = await request(`fair/source?offset=${first.headers.get('Stream-Next-Offset')}`)
  expect(await rest.text()).toBe('xxx')
  expect(rest.headers.get('Stream-Up-To-Date')).toBe('true')
  await handler.maintain({ maxChunks: 1, maxSubscriptionWork: 8 })
  expect((await pool.query(`SELECT count(*) FROM "${schema}".stream_changes WHERE stream_id=$1`, [stream.id])).rows[0].count).toBe('0')
})
