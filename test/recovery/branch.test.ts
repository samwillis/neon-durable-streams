import { randomUUID } from 'node:crypto'
import { readFile, writeFile, unlink } from 'node:fs/promises'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../../src/operations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
if (!env.NEON_API_KEY || !env.NEON_PROJECT_ID || !env.NEON_TEST_BRANCH_ID || !env.TEST_DATABASE_URL_UNPOOLED
  || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET || !env.DURABLE_STREAMS_KEYRING) throw new Error('Branch recovery qualification requires a provisioned isolated Neon environment')
const projectId = env.NEON_PROJECT_ID, parentId = env.NEON_TEST_BRANCH_ID
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8')) as { projectId: string; branchId: string; name: string }
if (state.projectId !== projectId || state.branchId !== parentId || !state.name.startsWith('ds-test-')) throw new Error('Recovery drill must use the recorded test branch')
const cleanupFile = '.neon-test-state-recovery.json'
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true, waitForReadiness: true, wait: { timeoutMs: 120_000 } })
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const parentPool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 3 })
parentPool.on('error', () => {})
const parentObjects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING)
const servers: DurableStreamsServer[] = []
let ownsCleanup = false
let parentExpiry: string | undefined
let branchId: string | undefined, childPool: pg.Pool | undefined, childObjects: ReturnType<typeof createS3ChunkStorage> | undefined
const handler = (pool = parentPool, objects = parentObjects, environmentId = parentId) => {
  const server = createDurableStreams({ postgres: { pool, schema, environmentId }, storage: { objects, targetChunkBytes: 16 }, subscriptions: { keyring },
    auth: { authenticate: () => ({ tenantId: 'recovery', subject: 'fixture' }), authorize: () => true } })
  servers.push(server)
  return server
}
const request = (server: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => server.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
const mask = (value: string) => { if (env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${value}`) }
beforeAll(async () => {
  // Refuse to overwrite cleanup state from an interrupted drill.
  await writeFile(cleanupFile, '', { flag: 'wx', mode: 0o600 })
  ownsCleanup = true
  await migrate(parentPool, { schema, environmentId: parentId, storageIdentity: parentObjects.identity })
  await initializeSubscriptions(parentPool, { schema, keyring })
})
afterAll(async () => {
  await Promise.all(servers.map((server) => server.close()))
  await childPool?.end(); childObjects?.close()
  if (branchId) await neon.branches.delete({ projectId, branchId })
  if (parentExpiry) await neon.branches.update({ projectId, branchId: parentId, expires_at: parentExpiry })
  if (ownsCleanup) await unlink(cleanupFile)
  await parentPool.query(`DROP SCHEMA "${schema}" CASCADE`)
  await parentPool.end(); parentObjects.close()
})

test('a real point-in-time branch adopts sealed objects, fences restored tokens, and isolates new writes', async () => {
  const parent = handler()
  const tuple = { 'Producer-Id': 'recoverable-writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  expect((await request(parent, 'data/root', 'PUT', 'initial recovery bytes')).status).toBe(201)
  expect((await request(parent, 'data/root', 'POST', ' committed once', tuple)).status).toBe(200)
  expect((await request(parent, 'child', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/data/root' })).status).toBe(201)
  expect((await request(parent, 'wake', 'PUT', '[]', { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await request(parent, '__ds/subscriptions/recover', 'PUT', JSON.stringify({ type: 'pull-wake', streams: ['data/root'], wake_stream: 'wake', lease_ttl_ms: 120_000 }), { 'Content-Type': 'application/json' })).status).toBe(201)
  await request(parent, 'data/root', 'POST', ' pending')
  await parent.maintain({ maxChunks: 16 })
  const claim = await (await request(parent, '__ds/subscriptions/recover/claim', 'POST', JSON.stringify({ worker: 'lost-worker' }), { 'Content-Type': 'application/json' })).json() as { wake_id: string; token: string; generation: number }
  expect(claim.token).toMatch(/^dsw\./)
  const sourceLsn = (await parentPool.query<{ lsn: string }>('SELECT pg_current_wal_flush_lsn() AS lsn')).rows[0]!.lsn
  // This committed future and its immutable objects are beyond the recovery point.
  await request(parent, 'data/root', 'POST', ' future data')
  await parent.maintain({ maxChunks: 16 })
  // Neon forbids children of expiring branches. This is the recorded disposable
  // CI branch; restore its expiry after child cleanup. The scheduled janitor
  // also covers runner loss during this short window.
  const parentBranch = await neon.branches.get({ projectId, branchId: parentId })
  if (parentBranch.expires_at) {
    parentExpiry = parentBranch.expires_at
    await neon.branches.update({ projectId, branchId: parentId, expires_at: null })
  }
  const child = await neon.branches.create({ projectId, parent_id: parentId, parent_lsn: sourceLsn,
    name: `ds-test-recovery-${randomUUID()}`, expires_at: new Date(Date.now() + 3600_000).toISOString(),
    compute: { minCu: 0.25, maxCu: 1, suspendTimeoutSeconds: 300 } })
  branchId = child.id
  await writeFile(cleanupFile, JSON.stringify({ projectId, branchId, name: child.name }), { mode: 0o600 })
  const [connection, storage, credential] = await Promise.all([
    neon.postgres.connectionString({ projectId, branchId, pooled: false }), neon.storage.get({ projectId, branchId }),
    neon.credentials.create({ projectId, branchId, name: 'recovery-drill', principal_type: 'user', scopes: ['storage:read', 'storage:write'], expires_at: new Date(Date.now() + 3600_000).toISOString() }),
  ])
  for (const value of [connection, credential.token_id, credential.s3_secret_access_key!]) mask(value)
  const verifiedConnection = new URL(connection); verifiedConnection.searchParams.set('sslmode', 'verify-full')
  childPool = new pg.Pool({ connectionString: verifiedConnection.href, max: 3 }); childPool.on('error', () => {})
  childObjects = createS3ChunkStorage({ endpoint: storage.s3_endpoint, region: storage.region, bucket: env.NEON_TEST_BUCKET!,
    credentials: { accessKeyId: credential.token_id, secretAccessKey: credential.s3_secret_access_key! } })
  expect(childObjects.identity).not.toBe(parentObjects.identity)
  await expect(handler(childPool, childObjects, branchId).inspectHealth()).rejects.toThrow('Environment identity differs')
  const identity = await inspectInstallation(childPool, { schema })
  expect(identity.environment_id).toBe(parentId)
  const { token } = await beginRecovery(childPool, { schema, operationId: randomUUID(), expectedInstallationId: identity.id, expectedServingEpoch: identity.serving_epoch,
    environmentId: branchId, storageIdentity: childObjects.identity })
  let reconciled
  do { reconciled = await verifyRecovery(childPool, { schema, token, keyring, objects: childObjects, maxChunks: 1 }) } while (!reconciled.complete)
  expect(reconciled.verified).toBeGreaterThanOrEqual(2)
  await finishRecovery(childPool, { schema, token })
  const recovered = handler(childPool, childObjects, branchId)
  expect(await (await request(recovered, 'data/root')).text()).toBe('initial recovery bytes committed once pending')
  expect(await (await request(recovered, 'child')).text()).toBe('initial recovery bytes committed once')
  expect((await request(recovered, 'data/root', 'POST', 'retry must deduplicate', tuple)).status).toBe(204)
  const callback = await request(recovered, '__ds/subscriptions/recover/ack', 'POST', JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, done: true }), { Authorization: `Bearer ${claim.token}`, 'Content-Type': 'application/json' })
  expect(callback.status).toBe(409)
  await recovered.maintain({ maxChunks: 16 })
  const next = await (await request(recovered, '__ds/subscriptions/recover/claim', 'POST', JSON.stringify({ worker: 'replacement' }), { 'Content-Type': 'application/json' })).json() as { token: string }
  expect(next.token).toMatch(/^dsw\./); expect(next.token).not.toBe(claim.token)
  await request(recovered, 'data/root', 'POST', ' child only')
  expect(await (await request(parent, 'data/root')).text()).toBe('initial recovery bytes committed once pending future data')
  expect(await (await request(recovered, 'data/root')).text()).toBe('initial recovery bytes committed once pending child only')
})
