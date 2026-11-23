import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { advanceOffset, parseOffset, ZERO_OFFSET } from '../../src/offset.js'
import { PostgresStreamStore } from '../../src/store.js'
import { transaction } from '../../src/transaction.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Ordinal qualification requires actual Neon')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 6 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {}); direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const servers: DurableStreamsServer[] = []
function server() {
  const value = createDurableStreams({ postgres: { pool, schema, ...postgresTestLimits }, storage: { objects },
    auth: { authenticate: () => ({ tenantId: 'ordinal', subject: 'fixture' }), authorize: () => true } })
  servers.push(value); return value
}
const request = (value: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => value.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))
beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }) })
afterAll(async () => {
  await Promise.all(servers.map((value) => value.close())); objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`); await pool.end(); await direct.end()
})
async function seed(value: DurableStreamsServer, path: string, count: number, payload: string) {
  expect((await request(value, path, 'PUT')).status).toBe(201)
  const id = (await pool.query(`SELECT id FROM "${schema}".streams WHERE path=$1`, [path])).rows[0].id as string
  const history = id.replaceAll('-', '')
  const offsets = [ZERO_OFFSET]
  for (let index = 0; index < count; index++) offsets.push(advanceOffset(offsets.at(-1)!, history))
  await transaction(pool, {}, async (client) => {
    await client.query(`INSERT INTO "${schema}".appends(stream_id,start_offset,end_offset,payload,payload_bytes)
      SELECT $1,x.start,x.end,$3,$4 FROM jsonb_to_recordset($2::jsonb) x(start text,"end" text)`,
    [id, JSON.stringify(offsets.slice(1).map((end, index) => ({ start: offsets[index], end }))), Buffer.from(payload), Buffer.byteLength(payload)])
    await client.query(`UPDATE "${schema}".streams SET tail_offset=$2,hot_frames=$3,hot_bytes=$4 WHERE id=$1`, [id, offsets.at(-1), count, count * Buffer.byteLength(payload)])
    await client.query(`INSERT INTO "${schema}".hot_chunks VALUES($1,clock_timestamp()+interval '1 hour')`, [id])
  })
  return { id, offsets }
}

test('dense tiny history retains only block metadata and replays 16384 appends in two default pages', async () => {
  const writer = server()
  const { id, offsets } = await seed(writer, 'dense', 16384, 'x'.repeat(32))
  expect((await writer.maintain({ forceHot: true, maxChunks: 1 })).sealed).toBe(1)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".appends WHERE stream_id=$1`, [id])).rows[0].count).toBe('0')
  const manifests = (await pool.query(`SELECT frame_count,octet_length(block_directory) AS directory_bytes FROM "${schema}".chunks WHERE stream_id=$1`, [id])).rows
  expect(manifests).toHaveLength(1)
  expect(manifests[0].frame_count).toBe(16384)
  // Outbox housekeeping remains active without configuring subscriptions or GC.
  expect((await pool.query(`SELECT count(*) FROM "${schema}".stream_changes WHERE stream_id=$1`, [id])).rows[0].count).toBe('0')
  expect(manifests[0].directory_bytes).toBeLessThan(2048)
  const reader = server()
  const first = await request(reader, 'dense')
  expect(first.headers.get('Stream-Next-Offset')).toBe(offsets[8192])
  expect(await first.text()).toBe('x'.repeat(8192 * 32))
  const second = await request(reader, `dense?offset=${offsets[8192]}`)
  expect(second.headers.get('Stream-Next-Offset')).toBe(offsets[16384])
  expect(second.headers.get('Stream-Up-To-Date')).toBe('true')
  expect(await second.text()).toBe('x'.repeat(8192 * 32))
  expect((await request(reader, `dense?offset=${offsets[100]!.slice(0, 17)}${'f'.repeat(32)}`)).status).toBe(400)
}, 120000)

test('producer retry and close-only append consume no ordinal', async () => {
  const value = server()
  await request(value, 'producer', 'PUT')
  const producer = { 'Producer-Id': 'writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  const first = await request(value, 'producer', 'POST', 'x', producer)
  expect(first.status).toBe(200)
  const offset = first.headers.get('Stream-Next-Offset')!
  expect(parseOffset(offset).ordinal).toBe(1n)
  const retry = await request(value, 'producer', 'POST', 'different', producer)
  expect(retry.status).toBe(204)
  expect(retry.headers.get('Stream-Next-Offset')).toBe(offset)
  const close = await request(value, 'producer', 'POST', '', { 'Stream-Closed': 'true' })
  expect(close.status).toBe(204)
  expect(close.headers.get('Stream-Next-Offset')).toBe(offset)
})

test('restore at 100 rejects abandoned 105 after rewriting through 110 and preserves inherited prefix tokens', async () => {
  const old = server()
  const { id, offsets } = await seed(old, 'restore', 105, 'old;')
  const anchor = offsets[100]!, abandoned = offsets[105]!
  expect((await request(old, 'child', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/restore', 'Stream-Fork-Offset': anchor })).status).toBe(201)
  // This models the database point-in-time boundary. Provider restore drills
  // separately exercise branch creation and the stopped-Function restore API.
  await transaction(pool, {}, async (client) => {
    await client.query(`DELETE FROM "${schema}".appends WHERE stream_id=$1 AND end_offset>$2`, [id, anchor])
    await client.query(`UPDATE "${schema}".streams SET tail_offset=$2,hot_frames=100,hot_bytes=400 WHERE id=$1`, [id, anchor])
    await client.query(`UPDATE "${schema}".installation SET serving_epoch=$1`, [randomUUID()])
  })
  expect((await request(old, 'restore')).status).toBe(503)
  const current = server()
  let rewritten105 = ''
  for (let index = 101; index <= 110; index++) {
    const append = await request(current, 'restore', 'POST', 'new;')
    expect(append.status).toBe(204)
    expect(parseOffset(append.headers.get('Stream-Next-Offset')!).ordinal).toBe(BigInt(index))
    if (index === 105) rewritten105 = append.headers.get('Stream-Next-Offset')!
  }
  expect(rewritten105).not.toBe(abandoned)
  for (const sealed of [false, true]) {
    if (sealed) await current.maintain({ forceHot: true, maxChunks: 16 })
    expect((await request(server(), `restore?offset=${abandoned}`)).status).toBe(400)
    expect(await (await request(server(), `restore?offset=${rewritten105}`)).text()).toBe('new;'.repeat(5))
    expect(await (await request(server(), `restore?offset=${anchor}`)).text()).toBe('new;'.repeat(10))
    expect(await (await request(server(), 'child')).text()).toBe('old;'.repeat(100))
    const store = new PostgresStreamStore(pool, schema, { storage: { objects } })
    try {
      await expect(store.transaction({}, (client) => store.validateAckBoundaries(client, [{ stream_id: id, offset: abandoned }]))).rejects.toMatchObject({ code: 'INVALID_OFFSET' })
      await store.transaction({}, (client) => store.validateAckBoundaries(client, [{ stream_id: id, offset: anchor }, { stream_id: id, offset: rewritten105 }]))
    } finally { store.close() }
  }
}, 120000)
