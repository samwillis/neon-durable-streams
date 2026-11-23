import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { transaction } from '../../src/transaction.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'

const env = process.env
if (
  !env.TEST_DATABASE_URL ||
  !env.TEST_DATABASE_URL_UNPOOLED ||
  !env.AWS_ENDPOINT_URL_S3 ||
  !env.AWS_REGION ||
  !env.NEON_TEST_BUCKET ||
  !env.DURABLE_STREAMS_KEYRING
)
  throw new Error('Compact storage qualification requires actual Neon')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 6 }),
  direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING),
  servers: DurableStreamsServer[] = [],
  errors: unknown[] = []
function server(storage: ChunkStorage = objects, maxChunkAgeMs = 3_600_000, cacheBytes = 32 * 1024 * 1024) {
  const value = createDurableStreams({
    postgres: { ...postgresTestLimits, pool, schema },
    storage: { objects: storage, maxChunkAgeMs, cacheBytes },
    subscriptions: { keyring },
    auth: { authenticate: () => ({ tenantId: 'compact', subject: 'fixture' }), authorize: () => true },
    onError: (error) => errors.push(error),
  })
  servers.push(value)
  return value
}
const request = (value: DurableStreamsServer, path: string, method = 'GET', body?: string, headers: Record<string, string> = {}) =>
  value.fetch(
    new Request(`https://fixture.test/v1/stream/${path}`, {
      method,
      headers: { 'Content-Type': 'text/plain', ...headers },
      ...(body === undefined ? {} : { body }),
    }),
  )
beforeAll(async () => {
  await migrate(direct, { schema, storageIdentity: objects.identity })
  await initializeSubscriptions(direct, { schema, keyring })
})
afterAll(async () => {
  await Promise.all(servers.map((value) => value.close()))
  objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
  expect(errors).toEqual([])
})

test('1024 variable-size frames seal without retained rows; eight replay pages fetch one verified object', async () => {
  const writer = server()
  expect((await request(writer, 'packed', 'PUT')).status).toBe(201)
  const id = (await pool.query(`SELECT id FROM "${schema}".streams WHERE path='packed'`)).rows[0].id
  const parts = Array.from({ length: 1023 }, (_, i) => String.fromCharCode(97 + (i % 26)).repeat((i % 23) + 1)),
    offsets = [ZERO_OFFSET]
  for (const part of [...parts, 'last']) offsets.push(advanceOffset(offsets.at(-1)!, Buffer.byteLength(part)))
  await transaction(pool, {}, async (client) => {
    await client.query(
      `INSERT INTO "${schema}".appends(stream_id,start_offset,end_offset,payload,payload_bytes)
      SELECT $1,x.start,x.end,decode(x.payload,'hex'),x.bytes FROM jsonb_to_recordset($2::jsonb) x(start text,"end" text,payload text,bytes integer)`,
      [
        id,
        JSON.stringify(
          parts.map((part, i) => ({ start: offsets[i], end: offsets[i + 1], payload: Buffer.from(part).toString('hex'), bytes: part.length })),
        ),
      ],
    )
    await client.query(`UPDATE "${schema}".streams SET tail_offset=$2,hot_frames=1023,hot_bytes=$3 WHERE id=$1`, [
      id,
      offsets[1023],
      parts.join('').length,
    ])
    await client.query(`INSERT INTO "${schema}".hot_chunks VALUES($1,clock_timestamp()+interval '1 hour')`, [id])
  })
  expect((await request(writer, 'packed', 'POST', 'last')).status).toBe(204)
  expect((await writer.maintain()).sealed).toBe(1)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".appends WHERE stream_id=$1`, [id])).rows[0].count).toBe('0')
  const manifest = (await pool.query(`SELECT end_deltas FROM "${schema}".chunks WHERE stream_id=$1`, [id])).rows[0]
  expect(manifest.end_deltas).toHaveLength(1024)
  let gets = 0
  const reader = server({
    ...objects,
    async get(key, max) {
      gets++
      return objects.get(key, max)
    },
  })
  let cursor = '-1',
    actual = '',
    pages = 0
  do {
    const response = await request(reader, `packed?offset=${cursor}`)
    expect(response.status).toBe(200)
    actual += await response.text()
    pages++
    cursor = response.headers.get('Stream-Next-Offset')!
    if (response.headers.has('Stream-Up-To-Date')) break
  } while (pages < 10)
  expect(actual).toBe(parts.join('') + 'last')
  expect(pages).toBe(8)
  expect(gets).toBe(1)
  const invalid = advanceOffset(ZERO_OFFSET, 2)
  expect(offsets).not.toContain(invalid)
  expect((await request(reader, `packed?offset=${invalid}`)).status).toBe(400)
  const forkHeaders = { 'Stream-Forked-From': '/v1/stream/packed', 'Stream-Fork-Offset': offsets[300]! }
  expect((await request(reader, 'child', 'PUT', undefined, forkHeaders)).status).toBe(201)
  expect(
    (
      await request(reader, 'grandchild', 'PUT', undefined, {
        ...forkHeaders,
        'Stream-Forked-From': '/v1/stream/child',
        'Stream-Fork-Offset': offsets[100]!,
      })
    ).status,
  ).toBe(201)
  expect((await request(reader, 'packed', 'DELETE')).status).toBe(204)
  expect(await (await request(reader, 'grandchild')).text()).toBe(parts.slice(0, 100).join(''))
  // Cache eviction/restart is never required for correctness.
  const cold = server(objects, 3_600_000, 0)
  expect(await (await request(cold, `child?offset=${offsets[256]}`)).text()).toBe(parts.slice(256, 300).join(''))
})

test('normal cron passes preserve small tails; first-append age and explicit drain seal them', async () => {
  const value = server()
  await request(value, 'age', 'PUT', 'first')
  const due = (await pool.query(`SELECT h.due_at FROM "${schema}".hot_chunks h JOIN "${schema}".streams s ON s.id=h.stream_id WHERE s.path='age'`))
    .rows[0].due_at
  for (let i = 0; i < 3; i++) await value.maintain()
  expect(
    (await pool.query(`SELECT count(*) FROM "${schema}".chunks c JOIN "${schema}".streams s ON s.id=c.stream_id WHERE s.path='age'`)).rows[0].count,
  ).toBe('0')
  await request(value, 'age', 'POST', 'second')
  expect(
    (await pool.query(`SELECT h.due_at FROM "${schema}".hot_chunks h JOIN "${schema}".streams s ON s.id=h.stream_id WHERE s.path='age'`)).rows[0]
      .due_at,
  ).toEqual(due)
  await pool.query(
    `UPDATE "${schema}".hot_chunks SET due_at=clock_timestamp()-interval '1 second' WHERE stream_id=(SELECT id FROM "${schema}".streams WHERE path='age')`,
  )
  expect((await value.maintain()).sealed).toBe(1)
  expect(await (await request(value, 'age')).text()).toBe('firstsecond')
  await request(value, 'drain', 'PUT', 'tiny')
  expect((await value.maintain({ forceHot: true })).sealed).toBe(1)
})

test('an ordinary append updates its stream head once and leaves the due record unchanged', async () => {
  const value = server()
  await request(value, 'head-updates', 'PUT', 'initial;')
  const id = (await pool.query(`SELECT id FROM "${schema}".streams WHERE path='head-updates'`)).rows[0].id
  await direct.query(`CREATE TABLE "${schema}".head_audit(id uuid);
    CREATE FUNCTION "${schema}".audit_head() RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN INSERT INTO "${schema}".head_audit VALUES(NEW.id);RETURN NEW;END $fn$;
    CREATE TRIGGER audit_head AFTER UPDATE ON "${schema}".streams FOR EACH ROW EXECUTE FUNCTION "${schema}".audit_head()`)
  try {
    const due = (await pool.query(`SELECT xmin,due_at FROM "${schema}".hot_chunks WHERE stream_id=$1`, [id])).rows[0]
    expect((await request(value, 'head-updates', 'POST', 'ordinary;')).status).toBe(204)
    expect((await pool.query(`SELECT count(*) FROM "${schema}".head_audit WHERE id=$1`, [id])).rows[0].count).toBe('1')
    expect((await pool.query(`SELECT xmin,due_at FROM "${schema}".hot_chunks WHERE stream_id=$1`, [id])).rows[0]).toEqual(due)
  } finally {
    await direct.query(`DROP TRIGGER audit_head ON "${schema}".streams;DROP FUNCTION "${schema}".audit_head();DROP TABLE "${schema}".head_audit`)
  }
})

test('atomic ack batches validate sealed interior boundaries, inherited ranges and post-snapshot appends', async () => {
  const value = server(),
    json = { 'Content-Type': 'application/json' }
  for (const path of ['acks/a', 'acks/b']) await request(value, path, 'PUT')
  await request(value, 'acks/wake', 'PUT', '[]', json)
  const sub = '__ds/subscriptions/packed'
  expect(
    (
      await request(
        value,
        sub,
        'PUT',
        JSON.stringify({ type: 'pull-wake', streams: ['acks/a', 'acks/b'], wake_stream: 'acks/wake', lease_ttl_ms: 600_000 }),
        json,
      )
    ).status,
  ).toBe(201)
  const anchors: Record<string, string> = {}
  for (const path of ['acks/a', 'acks/b']) {
    const response = await request(value, path, 'POST', 'first;')
    anchors[path] = response.headers.get('Stream-Next-Offset')!
    await request(value, path, 'POST', 'second;')
  }
  await value.maintain({ forceHot: true })
  const claim = (await (await request(value, `${sub}/claim`, 'POST', '{"worker":"fixture"}', json)).json()) as {
    token: string
    wake_id: string
    generation: number
  }
  const auth = { ...json, Authorization: `Bearer ${claim.token}` }
  const before = (await pool.query(`SELECT path,acked_offset FROM "${schema}".subscription_members ORDER BY path`)).rows
  const body = (acks: unknown[]) => JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, acks })
  expect(
    (
      await request(
        value,
        `${sub}/ack`,
        'POST',
        body([
          { stream: 'acks/a', offset: anchors['acks/a'] },
          { stream: 'acks/b', offset: advanceOffset(ZERO_OFFSET, 1) },
        ]),
        auth,
      )
    ).status,
  ).toBe(409)
  expect((await pool.query(`SELECT path,acked_offset FROM "${schema}".subscription_members ORDER BY path`)).rows).toEqual(before)
  const late = (await request(value, 'acks/a', 'POST', 'later;')).headers.get('Stream-Next-Offset')!
  await request(value, 'acks/a', 'POST', 'tail;')
  await value.maintain({ forceHot: true })
  expect(
    (
      await request(
        value,
        `${sub}/ack`,
        'POST',
        body([
          { stream: 'acks/a', offset: late },
          { stream: 'acks/b', offset: anchors['acks/b'] },
        ]),
        auth,
      )
    ).status,
  ).toBe(200)
  expect((await pool.query(`SELECT acked_offset FROM "${schema}".subscription_members WHERE path='acks/a'`)).rows[0].acked_offset).toBe(late)
  expect(
    (await request(value, 'acks/child', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/acks/a', 'Stream-Fork-Offset': anchors['acks/a']! }))
      .status,
  ).toBe(201)
  expect(await (await request(value, 'acks/child')).text()).toBe('first;')
})

test('explicitly enabling object storage enrolls existing hot-only tails and fences the old binding', async () => {
  const isolated = `ds_test_${randomUUID().replaceAll('-', '')}`
  const auth = { authenticate: () => ({ tenantId: 'compact', subject: 'fixture' }), authorize: () => true }
  const hot = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema: isolated }, auth })
  const tiered = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema: isolated }, storage: { objects }, auth })
  try {
    await migrate(direct, { schema: isolated })
    expect((await request(hot, 'existing', 'PUT', 'pre-binding;')).status).toBe(201)
    await migrate(direct, { schema: isolated, storageIdentity: objects.identity })
    expect((await pool.query(`SELECT count(*) FROM "${isolated}".hot_chunks`)).rows[0].count).toBe('1')
    expect((await request(hot, 'existing', 'POST', 'fenced;')).status).toBe(503)
    expect((await tiered.maintain({ forceHot: true })).sealed).toBe(1)
    expect(await (await request(tiered, 'existing')).text()).toBe('pre-binding;')
  } finally {
    await hot.close()
    await tiered.close()
    await direct.query(`DROP SCHEMA "${isolated}" CASCADE`)
  }
})
