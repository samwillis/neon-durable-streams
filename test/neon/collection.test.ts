import { createHash, randomBytes, randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, afterEach, beforeEach, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { beginRecovery, configureReclamation, finishRecovery, inspectInstallation, inspectReclamation, verifyRecovery } from '../../src/operations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { InstallationGuard } from '../../src/installation.js'
import { ReclamationManager } from '../../src/reclamation.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { chunkDigest, encodeChunk } from '../../src/chunk-codec.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Collection requires actual Neon services')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 5, connectionTimeoutMillis: 20_000 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 3, connectionTimeoutMillis: 20_000 })
pool.on('error', () => {}); direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
let schema: string, installationId: string
const servers: DurableStreamsServer[] = []
const sql = (text: string, params: unknown[] = []) => direct.query(text.replaceAll('ds.', `"${schema}".`), params)
function server(storage: ChunkStorage = objects, subscriptions?: Parameters<typeof createDurableStreams>[0]['subscriptions'], clientPool = pool) {
  const value = createDurableStreams({ postgres: { ...postgresTestLimits, pool: clientPool, schema, environmentId: 'collection-test' },
    storage: { objects: storage, targetChunkBytes: 4 }, ...(subscriptions ? { subscriptions } : {}),
    auth: { authenticate: () => ({ tenantId: 'collection', subject: 'test' }), authorize: () => true } })
  servers.push(value); return value
}
const request = (value: DurableStreamsServer, path: string, method = 'GET', body?: string, headers: Record<string, string> = {}) => value.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))
async function enable() {
  const before = await inspectInstallation(direct, { schema })
  const recovery = await beginRecovery(direct, { schema, operationId: randomUUID(), expectedInstallationId: before.id, expectedServingEpoch: before.serving_epoch,
    environmentId: 'collection-test', storageIdentity: objects.identity })
  const policy = { schema, token: recovery.token, enabled: true, recoveryWindowMs: 0 }
  await configureReclamation(direct, policy); await configureReclamation(direct, policy)
  await expect(configureReclamation(direct, { ...policy, recoveryWindowMs: 1 })).rejects.toThrow('different reclamation policy')
  await verifyRecovery(direct, { schema, token: recovery.token, objects })
  await finishRecovery(direct, { schema, token: recovery.token })
}
async function phase(index: number, storage: ChunkStorage = objects) {
  await sql('UPDATE ds.reclamation_policy SET work_phase=$1', [index])
  return new ReclamationManager(`"${schema}"`, new InstallationGuard(pool, schema, 'collection-test'), storage).maintain()
}
// Fixture time travel changes only this test's random schema. Production API
// enforces >=15 minutes and provides no way to bypass persisted promises.
async function elapse() {
  await sql("UPDATE ds.reclamation_policy SET hold_until=clock_timestamp()-interval '1 second'")
  await sql("UPDATE ds.chunks SET collect_after=clock_timestamp()-interval '1 second' WHERE retired_at IS NOT NULL")
  await sql("UPDATE ds.chunk_objects SET collect_after=clock_timestamp()-interval '1 second',next_attempt_at=clock_timestamp()-interval '1 second' WHERE retired_at IS NOT NULL")
  await sql("UPDATE ds.streams SET reclaim_after=clock_timestamp()-interval '1 second' WHERE reclaim_after IS NOT NULL")
  await sql("UPDATE ds.stream_changes SET created_at=clock_timestamp()-interval '1 day'")
}
async function sealed(path = 'source', body = 'exact durable payload') {
  const value = server()
  expect((await request(value, path, 'PUT', body)).status).toBe(201)
  await value.maintain()
  const chunk = (await sql('SELECT c.* FROM ds.chunks c JOIN ds.streams s ON s.id=c.stream_id WHERE s.path=$1', [path])).rows[0]!
  return { value, chunk }
}
beforeEach(async () => {
  schema = `ds_test_${randomUUID().replaceAll('-', '')}`
  await migrate(direct, { schema, storageIdentity: objects.identity, environmentId: 'collection-test' })
  installationId = (await inspectInstallation(direct, { schema })).id
})
afterEach(async () => {
  await Promise.all(servers.splice(0).map((value) => value.close()))
  let cursor: string | null = null
  do {
    const page = await objects.list(`v1/${installationId}/`, cursor, 128)
    for (const object of page.objects) await objects.delete(object.key)
    cursor = page.cursor
  } while (cursor !== null)
  await sql(`DROP SCHEMA "${schema}" CASCADE`)
})
afterAll(async () => { objects.close(); await pool.end(); await direct.end() })

test('policy requires quiesced administration and collection respects both persisted and global retention promises', async () => {
  await expect(configureReclamation(direct, { schema, token: randomUUID(), enabled: true, recoveryWindowMs: 0 })).rejects.toThrow('installation is active')
  await expect(configureReclamation(direct, { schema, token: randomUUID(), enabled: true, recoveryWindowMs: 0, safetyMarginMs: 1 })).rejects.toThrow('policy')
  await enable()
  const { value, chunk } = await sealed()
  expect((await sql('SELECT * FROM ds.chunk_objects WHERE chunk_id=$1', [chunk.id])).rows).toHaveLength(1)
  expect((await request(value, 'source', 'DELETE')).status).toBe(204)
  expect((await phase(0)).reclaimedChunks).toBe(1)
  expect(await inspectReclamation(direct, { schema, includeBacklog: true })).toMatchObject({ supported: true, enabled: true, recoveryWindowMs: 0,
    safetyMarginMs: 900_000, backlog: { pendingDeletes: 1, orphanCandidates: 0, quarantinedObjects: 0 } })
  expect((await phase(1)).deletedObjects).toBe(0)
  expect((await objects.get(chunk.object_key, chunk.encoded_bytes)).length).toBe(chunk.encoded_bytes)
  await sql("UPDATE ds.chunk_objects SET collect_after=clock_timestamp()-interval '1 second'")
  expect((await phase(1)).deletedObjects).toBe(0) // Global hold independently applies.
  await elapse()
  expect((await phase(1)).deletedObjects).toBe(1)
  expect(await inspectReclamation(direct, { schema, includeBacklog: true })).toMatchObject({ backlog: { pendingDeletes: 0 } })
  expect((await phase(1)).deletedObjects).toBe(0)
  await expect(objects.get(chunk.object_key, chunk.encoded_bytes)).rejects.toThrow('not found')
  expect((await sql('SELECT collected_at FROM ds.chunk_objects WHERE chunk_id=$1', [chunk.id])).rows[0].collected_at).not.toBe(null)
})

test('an in-flight replay pin protects a retired object, then releases before physical deletion', async () => {
  await enable()
  const { value, chunk } = await sealed()
  let enter!: () => void, release!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve }), paused = new Promise<void>((resolve) => { release = resolve })
  const reader = server({ ...objects, async get(key, maximum) { const bytes = await objects.get(key, maximum); enter(); await paused; return bytes } })
  const pending = request(reader, 'source')
  try {
    await entered
    expect((await request(value, 'source', 'DELETE')).status).toBe(204)
    await phase(0); await elapse()
    expect((await phase(1)).deletedObjects).toBe(0)
    expect((await sql('SELECT * FROM ds.replay_pins WHERE chunk_id=$1', [chunk.id])).rows).toHaveLength(1)
    release()
    const response = await pending
    expect(response.status).toBe(200); expect(await response.text()).toBe('exact durable payload')
    expect((await phase(1)).deletedObjects).toBe(1)
  } finally { release(); await pending }
})

test('a live descendant retains its deleted parent and exact inherited bytes until the last edge detaches', async () => {
  await enable()
  const { value, chunk } = await sealed()
  expect((await request(value, 'child', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/source' })).status).toBe(201)
  expect((await request(value, 'source', 'DELETE')).status).toBe(204)
  expect((await phase(0)).reclaimedChunks).toBe(0)
  expect(await (await request(value, 'child')).text()).toBe('exact durable payload')
  expect((await request(value, 'child', 'DELETE')).status).toBe(204)
  await value.maintain()
  await phase(0); await elapse()
  expect((await phase(1)).deletedObjects).toBe(1)
  expect((await sql('SELECT retired_at FROM ds.chunks WHERE id=$1', [chunk.id])).rows[0].retired_at).not.toBe(null)
})

test('an ambiguous successful delete preserves its retry identity and retries the same key idempotently', async () => {
  await enable()
  const { value, chunk } = await sealed()
  await request(value, 'source', 'DELETE'); await phase(0); await elapse()
  const attempts: string[] = []
  const ambiguous: ChunkStorage = { ...objects, async delete(key) { attempts.push(key); await objects.delete(key); throw new Error('lost successful response') } }
  await expect(phase(1, ambiguous)).rejects.toThrow('lost successful response')
  expect((await sql('SELECT collected_at,claim_token FROM ds.chunk_objects WHERE chunk_id=$1', [chunk.id])).rows[0]).toMatchObject({ collected_at: null, claim_token: null })
  await elapse()
  const retried: ChunkStorage = { ...objects, async delete(key) { attempts.push(key); await objects.delete(key) } }
  expect((await phase(1, retried)).deletedObjects).toBe(1)
  expect(attempts).toEqual([chunk.object_key, chunk.object_key])
})

test('retirement fences an uploading worker before it can publish or discard local bytes', async () => {
  await enable()
  let enter!: () => void, release!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve }), paused = new Promise<void>((resolve) => { release = resolve })
  const storage: ChunkStorage = { ...objects, async put(key, bytes) { enter(); await paused; await objects.put(key, bytes) } }
  const writer = server(storage), operator = server()
  expect((await request(writer, 'uploading', 'PUT', 'pending upload bytes')).status).toBe(201)
  const pending = writer.maintain({ maxChunks: 1, maxSubscriptionWork: 1 })
  try {
    await entered
    const intent = (await sql('SELECT * FROM ds.chunk_objects')).rows[0]
    expect(intent).toBeDefined() // Exact identity persisted before external put.
    expect((await request(operator, 'uploading', 'DELETE')).status).toBe(204)
    expect((await phase(0)).reclaimedChunks).toBe(1)
    release()
    expect((await pending).sealed).toBe(0)
    expect((await sql('SELECT state,retired_at FROM ds.chunks')).rows[0]).toMatchObject({ state: 'frozen', retired_at: expect.any(Date) })
    expect((await sql('SELECT payload FROM ds.appends')).rows[0].payload.toString()).toBe('pending upload bytes')
    await elapse(); expect((await phase(1)).deletedObjects).toBe(1)
    await expect(objects.get(intent.object_key, intent.encoded_bytes)).rejects.toThrow('not found')
  } finally { release(); await pending }
})

test('inventory verifies orphans, gives each a fresh hold, and rediscovers late resumed uploads', async () => {
  await enable()
  const streamId = randomUUID(), chunkId = randomUUID(), payload = Buffer.from('late immutable orphan')
  const bytes = encodeChunk({ installationId, streamId, chunkId, frames: [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET, payload.length), payload, messageEnds: null }] })
  const key = `v1/${installationId}/${streamId}/${chunkId}/${chunkDigest(bytes)}.ndsc`
  await objects.put(key, bytes)
  expect((await phase(3)).inventoriedObjects).toBe(1)
  expect((await phase(1)).deletedObjects).toBe(0) // Listing alone never authorizes deletion.
  await phase(4)
  const first = (await sql('SELECT * FROM ds.chunk_objects WHERE object_key=$1', [key])).rows[0]
  expect(first.orphan_candidate).toBe(false); expect(first.retired_at).toBeInstanceOf(Date)
  expect(first.collect_after.getTime()-first.retired_at.getTime()).toBeGreaterThanOrEqual(899_999)
  await elapse(); expect((await phase(1)).deletedObjects).toBe(1)
  await objects.put(key, bytes) // An already-issued external write can outlive SQL fencing.
  await sql('UPDATE ds.reclamation_policy SET inventory_completed_at=NULL')
  await phase(3); await phase(4)
  expect((await phase(1)).deletedObjects).toBe(0)
  const second = (await sql('SELECT * FROM ds.chunk_objects WHERE object_key=$1', [key])).rows[0]
  expect(second.collected_at).toBe(null); expect(second.retired_at.getTime()).toBeGreaterThan(first.retired_at.getTime())
  await elapse(); expect((await phase(1)).deletedObjects).toBe(1)
})

test('fair durable phase rotation and dependency-ordered pruning remove dead generation and namespace metadata', async () => {
  await enable()
  const { value } = await sealed()
  await request(value, 'source', 'DELETE'); await phase(0); await elapse()
  // A fresh manager per single-round invocation models cold Function hints.
  const phases: number[] = []
  await sql('UPDATE ds.reclamation_policy SET work_phase=0')
  for (let i = 0; i < 6; i++) {
    phases.push((await sql('SELECT work_phase FROM ds.reclamation_policy')).rows[0].work_phase)
    await new ReclamationManager(`"${schema}"`, new InstallationGuard(pool, schema, 'collection-test'), objects).maintain()
  }
  expect(phases).toEqual([0,1,2,3,4,5])
  for (let i = 0; i < 24; i++) {
    await phase(2)
    await phase(5)
  }
  for (const table of ['appends','chunks','streams','paths','stream_changes','producers']) expect(Number((await sql(`SELECT count(*) FROM ds.${table}`)).rows[0].count)).toBe(0)
  expect((await request(value, 'source', 'PUT', 'new generation')).status).toBe(201)
  expect(await (await request(value, 'source')).text()).toBe('new generation')
})

test('pruned signed and legacy wakes still fence authentic tokens, and recreated subscriptions keep increasing generations', async () => {
  await enable()
  const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING!)
  await initializeSubscriptions(direct, { schema, keyring })
  const value = server(objects, { keyring })
  await request(value, 'tokens/source', 'PUT')
  await request(value, 'tokens/wake', 'PUT', '[]', { 'Content-Type': 'application/json' })
  const name = '__ds/subscriptions/pruned', config = JSON.stringify({ type: 'pull-wake', streams: ['tokens/source'], wake_stream: 'tokens/wake' })
  const json = { 'Content-Type': 'application/json' }
  const issue = async () => {
    expect((await request(value, name, 'PUT', config, json)).status).toBeOneOf([200,201])
    await request(value, 'tokens/source', 'POST', 'payload')
    await value.maintain()
    const response = await request(value, `${name}/claim`, 'POST', '{"worker":"fixture"}', json)
    expect(response.status).toBe(200)
    const claim = await response.json() as { token: string; wake_id: string; generation: number; streams: { tail_offset: string }[] }
    const body = JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, done: true,
      acks: [{ stream: 'tokens/source', offset: claim.streams[0]!.tail_offset }] })
    expect((await request(value, `${name}/ack`, 'POST', body, { ...json, Authorization: `Bearer ${claim.token}` })).status).toBe(200)
    return { ...claim, body }
  }
  const first = await issue()
  const legacy = `dsw.${first.wake_id.slice(2)}.${randomBytes(32).toString('base64url')}`
  await sql('UPDATE ds.subscription_wakes SET token_version=1,token_hash=$2 WHERE id=$1', [first.wake_id.slice(2), createHash('sha256').update(legacy).digest()])
  await sql("UPDATE ds.subscription_wakes SET created_at=clock_timestamp()-interval '1 day'")
  await sql('UPDATE ds.reclamation_policy SET metadata_phase=0'); await phase(5)
  expect((await sql('SELECT * FROM ds.subscription_wakes WHERE id=$1', [first.wake_id.slice(2)])).rows).toHaveLength(0)
  expect((await sql('SELECT * FROM ds.legacy_wake_tokens WHERE wake_id=$1', [first.wake_id.slice(2)])).rows).toHaveLength(1)
  expect((await request(value, `${name}/ack`, 'POST', first.body, { ...json, Authorization: `Bearer ${legacy}` })).status).toBe(409)
  expect((await request(value, name, 'DELETE')).status).toBe(204)
  await sql("UPDATE ds.subscriptions SET deleted_at=clock_timestamp()-interval '1 day' WHERE deleted_at IS NOT NULL")
  await sql('UPDATE ds.reclamation_policy SET metadata_phase=1'); await phase(5)
  expect((await sql('SELECT * FROM ds.subscriptions')).rows).toHaveLength(0)
  expect((await request(value, `${name}/ack`, 'POST', first.body, { ...json, Authorization: `Bearer ${first.token}` })).status).toBe(409)
  expect((await request(value, `${name}/ack`, 'POST', first.body, { ...json, Authorization: `Bearer ${legacy}` })).status).toBe(409)
  const next = await issue()
  expect(next.generation).toBeGreaterThan(first.generation)
  await sql("UPDATE ds.subscription_wakes SET created_at=clock_timestamp()-interval '1 day'")
  await sql('UPDATE ds.reclamation_policy SET metadata_phase=0'); await phase(5)
  expect((await request(value, `${name}/ack`, 'POST', next.body, { ...json, Authorization: `Bearer ${next.token}` })).status).toBe(409)
})

test('recovery ignores collected unreachable manifests, requires every reachable object, and renews collection holds', async () => {
  await enable()
  const dead = await sealed('dead'), live = await sealed('live', 'reachable bytes')
  const liveBytes = await objects.get(live.chunk.object_key, live.chunk.encoded_bytes)
  await request(dead.value, 'dead', 'DELETE'); await phase(0); await elapse()
  expect((await phase(1)).deletedObjects).toBe(1)
  const before = await inspectInstallation(direct, { schema })
  const operation = await beginRecovery(direct, { schema, operationId: randomUUID(), expectedInstallationId: before.id,
    expectedServingEpoch: before.serving_epoch, environmentId: 'collection-test', storageIdentity: objects.identity })
  await objects.delete(live.chunk.object_key)
  await expect(verifyRecovery(direct, { schema, token: operation.token, objects })).rejects.toThrow('not found')
  await expect(finishRecovery(direct, { schema, token: operation.token })).rejects.toThrow('Verify every sealed manifest')
  await objects.put(live.chunk.object_key, liveBytes)
  expect(await verifyRecovery(direct, { schema, token: operation.token, objects })).toEqual({ complete: true, verified: 1 })
  await finishRecovery(direct, { schema, token: operation.token })
  expect((await sql('SELECT hold_until>clock_timestamp() AS held FROM ds.reclamation_policy')).rows[0].held).toBe(true)
  expect((await request(live.value, 'live')).status).toBe(503)
  expect(await (await request(server(), 'live')).text()).toBe('reachable bytes')
})

test('creation and forks retry safely when namespace collection wins after INSERT ON CONFLICT', async () => {
  await enable()
  const normal = server()
  expect((await request(normal, 'source', 'PUT', 'source bytes')).status).toBe(201)
  for (const fork of [false,true]) {
    const path = fork ? 'z-raced-fork' : 'raced-create'
    await sql("INSERT INTO ds.paths (tenant_id,path) VALUES ('collection',$1)", [path])
    let armed = true
    const racedPool = {
      query: pool.query.bind(pool),
      async connect() {
        const client = await pool.connect()
        return {
          release: client.release.bind(client),
          async query(text: string, parameters?: unknown[]) {
            const result = await client.query(text, parameters)
            if (armed && text.includes('INSERT INTO') && text.includes('.paths ') && parameters?.[1] === path) {
              armed = false
              await sql('UPDATE ds.reclamation_policy SET metadata_phase=5')
              expect((await phase(5)).prunedRows).toBe(1)
            }
            return result
          },
        }
      },
    } as unknown as pg.Pool
    const value = server(objects, undefined, racedPool), headers = fork ? { 'Stream-Forked-From': '/v1/stream/source' } : {}
    const raced = await request(value, path, 'PUT', undefined, headers)
    expect(raced.status).toBe(503)
    expect((await raced.json() as { error: { code: string } }).error.code).toBe('NAMESPACE_RETRY')
    expect((await sql('SELECT id FROM ds.streams WHERE path=$1', [path])).rows).toHaveLength(0)
    expect((await request(normal, path, 'PUT', undefined, headers)).status).toBe(201)
  }
})

test('expired delete claims cannot finalize a replacement claim or lose the exact immutable retry key', async () => {
  await enable()
  const { value, chunk } = await sealed()
  await request(value, 'source', 'DELETE'); await phase(0); await elapse()
  let enter!: () => void, release!: () => void
  const entered = new Promise<void>((resolve) => { enter = resolve }), paused = new Promise<void>((resolve) => { release = resolve })
  const delayed: ChunkStorage = { ...objects, async delete(key) { await objects.delete(key); enter(); await paused } }
  const first = phase(1, delayed)
  try {
    await entered
    const before = (await sql('SELECT claim_token FROM ds.chunk_objects WHERE object_key=$1', [chunk.object_key])).rows[0].claim_token
    expect(before).not.toBe(null)
    await sql("UPDATE ds.chunk_objects SET claim_until=clock_timestamp()-interval '1 second' WHERE object_key=$1", [chunk.object_key])
    expect((await phase(1)).deletedObjects).toBe(1)
    const committed = (await sql('SELECT collected_at,claim_epoch FROM ds.chunk_objects WHERE object_key=$1', [chunk.object_key])).rows[0]
    release(); expect((await first).deletedObjects).toBe(0)
    expect((await sql('SELECT collected_at,claim_epoch FROM ds.chunk_objects WHERE object_key=$1', [chunk.object_key])).rows[0]).toEqual(committed)
  } finally { release(); await first }
})

test('inventory preserves live chunks, quarantines corrupt objects, and handles a listed object disappearing', async () => {
  await enable()
  const { chunk } = await sealed()
  await sql('DELETE FROM ds.chunk_objects') // Model a pre-0006 sealed manifest.
  await sql('UPDATE ds.reclamation_policy SET inventory_completed_at=NULL')
  await phase(3); await phase(4); await elapse()
  expect((await phase(1)).deletedObjects).toBe(0)
  const retained = (await sql('SELECT * FROM ds.chunk_objects WHERE object_key=$1', [chunk.object_key])).rows[0]
  expect(retained).toMatchObject({ retired_at: null, orphan_candidate: false, quarantine_reason: null })
  const badKey = `v1/${installationId}/${randomUUID()}/${randomUUID()}/${'0'.repeat(64)}.ndsc`
  await objects.put(badKey, Buffer.from('invalid chunk'))
  await sql('UPDATE ds.reclamation_policy SET inventory_completed_at=NULL')
  await phase(3); await phase(4)
  expect((await sql('SELECT quarantine_reason FROM ds.chunk_objects WHERE object_key=$1', [badKey])).rows[0].quarantine_reason).toBe('Invalid immutable chunk envelope')
  const streamId = randomUUID(), chunkId = randomUUID(), payload = Buffer.from('disappearing')
  const bytes = encodeChunk({ installationId, streamId, chunkId, frames: [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET,payload.length), payload, messageEnds: null }] })
  const goneKey = `v1/${installationId}/${streamId}/${chunkId}/${chunkDigest(bytes)}.ndsc`
  await objects.put(goneKey,bytes)
  await sql('UPDATE ds.reclamation_policy SET inventory_completed_at=NULL')
  await phase(3); await objects.delete(goneKey); await phase(4)
  expect((await sql('SELECT retired_at,collected_at,orphan_candidate FROM ds.chunk_objects WHERE object_key=$1', [goneKey])).rows[0])
    .toMatchObject({ retired_at: expect.any(Date), collected_at: expect.any(Date), orphan_candidate: false })
})
