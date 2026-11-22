import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { encodeChunk, chunkDigest } from '../../src/chunk-codec.js'
import { migrate, migrationPlan } from '../../src/migrations.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { transaction } from '../../src/transaction.js'
import { inspectReclamation } from '../../src/operations.js'

const env = process.env
if (!env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Upgrade qualification requires real Neon services')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
let server: DurableStreamsServer | undefined
afterAll(async () => { await server?.close(); await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`); await pool.end(); objects.close() })

test('quiesced upgrade from migration 0004 preserves v1 sealed bytes, offsets, forks, and producer sessions', async () => {
  const installationId = randomUUID(), streamId = randomUUID(), chunkId = randomUUID(), childId = randomUUID()
  const payload = Buffer.from('previous binary format'), next = advanceOffset(ZERO_OFFSET, payload.length)
  const encoded = encodeChunk({ installationId, streamId, chunkId, frames: [{ start: ZERO_OFFSET, end: next, payload, messageEnds: null }] })
  const digest = chunkDigest(encoded), key = `v1/${installationId}/${streamId}/${chunkId}/${digest}.ndsc`
  await objects.put(key, encoded)
  const prior = (await migrationPlan(schema)).slice(0, 4)
  await transaction(pool, { timeoutMs: 60_000 }, async (client) => {
    await client.query(`CREATE SCHEMA "${schema}"`)
    await client.query(`CREATE TABLE "${schema}".migrations (id text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`)
    for (const migration of prior) {
      await client.query(migration.sql)
      await client.query(`INSERT INTO "${schema}".migrations (id, checksum) VALUES ($1, $2)`, [migration.id, migration.checksum])
    }
    await client.query(`INSERT INTO "${schema}".installation (id, storage_version, storage_identity) VALUES ($1, 1, $2)`, [installationId, objects.identity])
    await client.query(`INSERT INTO "${schema}".paths (tenant_id, path) VALUES ('upgrade', 'source'), ('upgrade', 'fork')`)
    await client.query(`INSERT INTO "${schema}".streams (id, tenant_id, path, content_type, tail_offset, change_version, ref_count)
      VALUES ($1, 'upgrade', 'source', 'text/plain', $2, 1, 1)`, [streamId, next])
    await client.query(`INSERT INTO "${schema}".streams (id, tenant_id, path, content_type, tail_offset, source_id, fork_offset, fork_depth, edge_active)
      VALUES ($1, 'upgrade', 'fork', 'text/plain', $2, $3, $2, 1, true)`, [childId, next, streamId])
    await client.query(`UPDATE "${schema}".paths SET active_stream_id = CASE WHEN path = 'source' THEN $1::uuid ELSE $2::uuid END`, [streamId, childId])
    await client.query(`INSERT INTO "${schema}".chunks (id, stream_id, start_offset, end_offset, state, frame_count, payload_bytes, object_key, sha256, encoded_bytes, sealed_at)
      VALUES ($1, $2, $3, $4, 'sealed', 1, $5, $6, $7, $8, clock_timestamp())`, [chunkId, streamId, ZERO_OFFSET, next, payload.length, key, digest, encoded.length])
    await client.query(`INSERT INTO "${schema}".appends (stream_id, start_offset, end_offset, payload_bytes, chunk_id) VALUES ($1, $2, $3, $4, $5)`, [streamId, ZERO_OFFSET, next, payload.length, chunkId])
    await client.query(`INSERT INTO "${schema}".producers (stream_id, producer_id, epoch, last_seq) VALUES ($1, 'writer', 7, 12)`, [streamId])
  })
  expect(await migrate(pool, { schema, storageIdentity: objects.identity, environmentId: 'upgraded' })).toEqual(['0005_operations', '0006_reclamation'])
  expect(await migrate(pool, { schema, storageIdentity: objects.identity, environmentId: 'upgraded' })).toEqual([])
  server = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema, environmentId: 'upgraded' }, storage: { objects },
    auth: { authenticate: () => ({ tenantId: 'upgrade', subject: 'fixture' }), authorize: () => true } })
  const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>) => server!.fetch(new Request(`https://fixture.test/v1/stream/${path}`, { method,
    headers: { 'Content-Type': 'text/plain', ...headers }, ...(body ? { body } : {}) }))
  for (const path of ['source', 'fork']) {
    const read = await request(path)
    expect(read.status).toBe(200); expect(read.headers.get('Stream-Next-Offset')).toBe(next)
    expect(await read.text()).toBe(payload.toString())
  }
  const tuple = { 'Producer-Id': 'writer', 'Producer-Epoch': '7', 'Producer-Seq': '12' }
  expect((await request('source', 'POST', 'duplicate', tuple)).status).toBe(204)
  expect((await request('source', 'POST', ' new', { ...tuple, 'Producer-Seq': '13' })).status).toBe(200)
  expect(await (await request('source')).text()).toBe('previous binary format new')
  expect(await (await request('fork')).text()).toBe('previous binary format')
})

test('stopped schema-0005 upgrade fences warmed runtimes and preserves producer and fork continuity', async () => {
  const isolated = `ds_test_${randomUUID().replaceAll('-', '')}`, runtimes: DurableStreamsServer[] = []
  const make = () => {
    const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema: isolated, environmentId: 'stopped-upgrade' }, storage: { objects, targetChunkBytes: 4 },
      auth: { authenticate: () => ({ tenantId: 'upgrade', subject: 'fixture' }), authorize: () => true } })
    runtimes.push(value); return value
  }
  const request = (value: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string,string>) => value.fetch(new Request(`https://fixture.test/v1/stream/${path}`, { method,
    headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }) }))
  try {
    await migrate(pool, { schema: isolated, target: '0005_operations', storageIdentity: objects.identity, environmentId: 'stopped-upgrade' })
    expect(await inspectReclamation(pool, { schema: isolated })).toEqual({ supported: false })
    const old = make()
    expect((await request(old, 'source', 'PUT')).status).toBe(201)
    const tuple = { 'Producer-Id': 'writer', 'Producer-Epoch': '7', 'Producer-Seq': '0' }
    expect((await request(old, 'source', 'POST', 'retained before upgrade', tuple)).status).toBe(200)
    await old.maintain()
    expect((await request(old, 'fork', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/source' })).status).toBe(201)
    // No traffic/workers are admitted while DDL runs. Retain this stopped
    // object solely to prove epoch fencing if an old process attempts to resume.
    expect(await migrate(pool, { schema: isolated, storageIdentity: objects.identity, environmentId: 'stopped-upgrade' })).toEqual(['0006_reclamation'])
    expect((await request(old, 'source')).status).toBe(503)
    expect((await request(old, 'source', 'POST', 'must not append', tuple)).status).toBe(503)
    const current = make()
    for (const path of ['source','fork']) expect(await (await request(current, path)).text()).toBe('retained before upgrade')
    expect((await request(current, 'source', 'POST', 'duplicate', tuple)).status).toBe(204)
    expect((await request(current, 'source', 'POST', ' after', { ...tuple, 'Producer-Seq': '1' })).status).toBe(200)
    expect(await (await request(current, 'source')).text()).toBe('retained before upgrade after')
    expect(await (await request(current, 'fork')).text()).toBe('retained before upgrade')
    expect((await pool.query(`SELECT enabled FROM "${isolated}".reclamation_policy`)).rows[0].enabled).toBe(false)
  } finally {
    await Promise.all(runtimes.map((value) => value.close()))
    await pool.query(`DROP SCHEMA IF EXISTS "${isolated}" CASCADE`)
  }
})
