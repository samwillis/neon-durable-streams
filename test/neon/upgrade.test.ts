import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { encodeChunk, chunkDigest } from '../../src/chunk-codec.js'
import { convertSealedStorage, migrate } from '../../src/migrations.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { transaction } from '../../src/transaction.js'
import { InstallationGuard } from '../../src/installation.js'

const env = process.env
if (!env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Upgrade qualification requires real Neon services')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const schemas: string[] = [], servers: DurableStreamsServer[] = [], keys: string[] = []
afterAll(async () => {
  await Promise.all(servers.map((value) => value.close()))
  for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
  for (const key of keys) await objects.delete(key)
  await pool.end(); objects.close()
})
async function fixture(target: '0004_subscriptions' | '0007_provider_recovery', fork = true) {
  const schema = `ds_test_${randomUUID().replaceAll('-', '')}`, streamId = randomUUID(), chunkId = randomUUID(), childId = randomUUID()
  schemas.push(schema)
  await migrate(pool, { schema, target, storageIdentity: objects.identity, ...(target === '0007_provider_recovery' ? { environmentId: 'upgraded' } : {}) })
  const installationId = (await pool.query(`SELECT id FROM "${schema}".installation`)).rows[0].id
  let offset = ZERO_OFFSET
  const frames = ['previous;', 'exact bytes;', 'tail;'].map((value) => {
    const start = offset, payload = Buffer.from(value); offset = advanceOffset(start, payload.length)
    return { start, end: offset, payload, messageEnds: null }
  })
  const encoded = encodeChunk({ installationId, streamId, chunkId, frames }), digest = chunkDigest(encoded)
  const key = `v1/${installationId}/${streamId}/${chunkId}/${digest}.ndsc`
  keys.push(key); await objects.put(key, encoded)
  await transaction(pool, {}, async (client) => {
    await client.query(`INSERT INTO "${schema}".paths (tenant_id,path) VALUES ('upgrade','source')`)
    await client.query(`INSERT INTO "${schema}".streams (id,tenant_id,path,content_type,tail_offset,change_version,ref_count)
      VALUES ($1,'upgrade','source','text/plain',$2,1,$3)`, [streamId, offset, fork ? 1 : 0])
    await client.query(`UPDATE "${schema}".paths SET active_stream_id=$1 WHERE path='source'`, [streamId])
    await client.query(`INSERT INTO "${schema}".chunks (id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes,object_key,sha256,encoded_bytes,sealed_at)
      VALUES ($1,$2,$3,$4,'sealed',3,$5,$6,$7,$8,clock_timestamp())`, [chunkId, streamId, ZERO_OFFSET, offset, frames.reduce((n, f) => n+f.payload.length, 0), key, digest, encoded.length])
    for (const frame of frames) await client.query(`INSERT INTO "${schema}".appends (stream_id,start_offset,end_offset,payload_bytes,chunk_id) VALUES ($1,$2,$3,$4,$5)`, [streamId, frame.start, frame.end, frame.payload.length, chunkId])
    await client.query(`INSERT INTO "${schema}".producers VALUES ($1,'writer',7,12)`, [streamId])
    if (fork) {
      await client.query(`INSERT INTO "${schema}".paths (tenant_id,path) VALUES ('upgrade','fork')`)
      await client.query(`INSERT INTO "${schema}".streams (id,tenant_id,path,content_type,tail_offset,source_id,fork_offset,fork_depth,edge_active)
        VALUES ($1,'upgrade','fork','text/plain',$2,$3,$2,1,true)`, [childId, frames[1]!.end, streamId])
      await client.query(`UPDATE "${schema}".paths SET active_stream_id=$1 WHERE path='fork'`, [childId])
    }
  })
  const prepare = () => migrate(pool, { schema, target: '0008_compact_prepare', storageIdentity: objects.identity, environmentId: 'upgraded' })
  const server = () => {
    const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema, environmentId: 'upgraded' }, storage: { objects },
      auth: { authenticate: () => ({ tenantId: 'upgrade', subject: 'fixture' }), authorize: () => true } })
    servers.push(value); return value
  }
  return { schema, frames, chunkId, streamId, key, prepare, server }
}
const request = (value: DurableStreamsServer, path: string, method = 'GET', body?: string, headers: Record<string,string> = {}) => value.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))

test('stopped upgrade from schema 0004 preserves exact v1 objects, interior forks and producer retries', async () => {
  const f = await fixture('0004_subscriptions')
  await f.prepare()
  await expect(migrate(pool, { schema: f.schema })).rejects.toThrow('convertSealedStorage')
  expect(await convertSealedStorage(pool, { schema: f.schema, objects, maxChunks: 1 })).toEqual({ complete: true, converted: 1 })
  expect(await convertSealedStorage(pool, { schema: f.schema, objects })).toEqual({ complete: true, converted: 0 })
  expect(await migrate(pool, { schema: f.schema })).toEqual(['0009_compact_storage'])
  expect(await migrate(pool, { schema: f.schema })).toEqual([])
  expect((await pool.query(`SELECT count(*) FROM "${f.schema}".appends`)).rows[0].count).toBe('0')
  const current = f.server()
  expect(await (await request(current, 'source')).text()).toBe('previous;exact bytes;tail;')
  expect(await (await request(current, 'fork')).text()).toBe('previous;exact bytes;')
  const tuple = { 'Producer-Id': 'writer', 'Producer-Epoch': '7', 'Producer-Seq': '12' }
  expect((await request(current, 'source', 'POST', 'duplicate', tuple)).status).toBe(204)
  expect((await request(current, 'source', 'POST', 'new;', { ...tuple, 'Producer-Seq': '13' })).status).toBe(200)
  expect(await (await request(current, 'source')).text()).toBe('previous;exact bytes;tail;new;')
})

test('preparation fences warm epochs; failed object verification retains every old row and conversion resumes', async () => {
  const f = await fixture('0007_provider_recovery'), guard = new InstallationGuard(pool, f.schema, 'upgraded')
  await guard.check()
  await f.prepare()
  await expect(guard.check()).rejects.toThrow('quiesced')
  const corrupt = { ...objects, async get(key: string, max: number) { const bytes = Buffer.from(await objects.get(key,max)); bytes[bytes.length-1] = bytes.at(-1)! ^ 1; return bytes } }
  await expect(convertSealedStorage(pool, { schema: f.schema, objects: corrupt })).rejects.toThrow('checksum')
  expect((await pool.query(`SELECT count(*) FROM "${f.schema}".appends`)).rows[0].count).toBe('3')
  expect((await pool.query(`SELECT end_deltas FROM "${f.schema}".chunks`)).rows[0].end_deltas).toBeNull()
  let inspected = false
  const checked = { ...objects, async get(key: string, max: number) {
    // This would block if conversion held its installation lock during I/O.
    await transaction(pool, { lockTimeoutMs: 1000 }, async (client) => { await client.query(`SELECT id FROM "${f.schema}".installation FOR UPDATE`) })
    inspected = true; return objects.get(key,max)
  } }
  expect((await convertSealedStorage(pool, { schema: f.schema, objects: checked })).converted).toBe(1)
  expect(inspected).toBe(true)
  await migrate(pool, { schema: f.schema })
  expect(await (await request(f.server(), `source?offset=${f.frames[0]!.end}`)).text()).toBe('exact bytes;tail;')
})

test('retired metadata converts from its exact retained index after the object has been collected', async () => {
  const f = await fixture('0007_provider_recovery', false)
  await pool.query(`UPDATE "${f.schema}".chunks SET retired_at=clock_timestamp() WHERE id=$1`, [f.chunkId])
  await pool.query(`UPDATE "${f.schema}".streams SET deleted_at=clock_timestamp() WHERE id=$1`, [f.streamId])
  await pool.query(`UPDATE "${f.schema}".paths SET active_stream_id=NULL`)
  await objects.delete(f.key)
  await f.prepare()
  expect(await convertSealedStorage(pool, { schema: f.schema, objects })).toEqual({ complete: true, converted: 1 })
  await migrate(pool, { schema: f.schema })
  expect((await pool.query(`SELECT end_deltas FROM "${f.schema}".chunks`)).rows[0].end_deltas).toHaveLength(3)
  expect((await pool.query(`SELECT count(*) FROM "${f.schema}".appends`)).rows[0].count).toBe('0')
})

test('conversion preserves old frozen and unassigned hot frames across the monotonic staging floor', async () => {
  const f = await fixture('0007_provider_recovery', false), frozenId = randomUUID()
  const start = f.frames.at(-1)!.end, frozen = Buffer.from('frozen;'), hot = Buffer.from('hot;')
  const end = advanceOffset(start, frozen.length), tail = advanceOffset(end, hot.length)
  await transaction(pool, {}, async (client) => {
    await client.query(`INSERT INTO "${f.schema}".chunks (id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes)
      VALUES($1,$2,$3,$4,'frozen',1,$5)`, [frozenId,f.streamId,start,end,frozen.length])
    await client.query(`INSERT INTO "${f.schema}".appends(stream_id,start_offset,end_offset,payload,payload_bytes,chunk_id)
      VALUES($1,$2,$3,$4,$5,$6),($1,$3,$7,$8,$9,NULL)`, [f.streamId,start,end,frozen,frozen.length,frozenId,tail,hot,hot.length])
    await client.query(`UPDATE "${f.schema}".streams SET tail_offset=$2,hot_bytes=$3,hot_frames=1 WHERE id=$1`, [f.streamId,tail,hot.length])
  })
  await f.prepare(); await convertSealedStorage(pool,{schema:f.schema,objects}); await migrate(pool,{schema:f.schema})
  expect((await pool.query(`SELECT hot_start_offset FROM "${f.schema}".streams WHERE id=$1`,[f.streamId])).rows[0].hot_start_offset).toBe(end)
  const value=f.server(), expected='previous;exact bytes;tail;frozen;hot;'
  expect(await(await request(value,'source')).text()).toBe(expected)
  expect((await value.maintain()).sealed).toBe(1)
  expect((await pool.query(`SELECT count(*) FROM "${f.schema}".appends`)).rows[0].count).toBe('1')
  expect((await value.maintain({forceHot:true})).sealed).toBe(1)
  expect((await pool.query(`SELECT count(*) FROM "${f.schema}".appends`)).rows[0].count).toBe('0')
  expect(await(await request(value,'source')).text()).toBe(expected)
})
