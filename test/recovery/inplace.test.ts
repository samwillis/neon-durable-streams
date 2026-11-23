import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { access, mkdir, readFile, writeFile } from 'node:fs/promises'
import { parseEnv, promisify } from 'node:util'
import { createNeonClient, raw } from '@neon/sdk'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../../src/operations.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import type { HeadWatch, RealtimeAppend } from '../../src/neon.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
const testSha256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex')
if (!env.NEON_API_KEY || !env.NEON_PROJECT_ID || !env.NEON_PARENT_BRANCH_ID || !env.NEON_TEST_BRANCH_ID) throw new Error('In-place recovery requires a recorded isolated Neon test environment')
const projectId = env.NEON_PROJECT_ID, parentId = env.NEON_PARENT_BRANCH_ID
const coordinator = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8')) as { projectId: string; branchId: string; name: string }
if (coordinator.projectId !== projectId || coordinator.branchId !== env.NEON_TEST_BRANCH_ID || !coordinator.name.startsWith('ds-test-') || parentId === coordinator.branchId) throw new Error('Unrecognized recovery coordinator')
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true, waitForReadiness: true, wait: { timeoutMs: 180_000 } })
const fixtureId = randomUUID(), name = `ds-test-inplace-${fixtureId}`, preservedName = `ds-test-preserved-${fixtureId}`
const stateFile = '.neon-test-state-inplace.json', branchFile = '.neon-test-state-inplace-branch.json'
const functionFile = '.env.neon.inplace.function', environmentFile = '.env.neon.inplace'
const run = promisify(execFile)
let ownsFixture = false, branchId = '', schema = ''
let data: Record<string, string> = {}, deployed: Record<string, string> = {}
let pool: pg.Pool | undefined, objects: ReturnType<typeof createS3ChunkStorage> | undefined
let realtimeObserver: ReturnType<typeof createNeonRealtimeNotifier> | undefined
const liveAbort = new AbortController()
let liveReader: ReadableStreamDefaultReader<Uint8Array> | undefined
const stages: string[] = []
const startedAt = performance.now(), stageTimes: { stage: string; elapsedMs: number }[] = []
const stage = (value: string) => { stages.push(value); stageTimes.push({ stage: value, elapsedMs: Math.round(performance.now() - startedAt) }); console.info(`In-place restore: ${value}`) }
const mask = (value: string) => { if (env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${value}`) }
const verified = (uri: string) => { const url = new URL(uri); url.searchParams.set('sslmode', 'verify-full'); return url.href }
async function script(path: string, extra: Record<string, string | undefined> = {}) {
  try {
    await run(process.execPath, [path], { env: { ...env, ...data, ...extra, GITHUB_ENV: undefined }, timeout: 240_000, maxBuffer: 8 * 1024 * 1024 })
  } catch (error) {
    await mkdir('qualification-results', { recursive: true })
    const failure = error as { stdout?: string; stderr?: string; code?: unknown }
    await writeFile('qualification-results/inplace-control.log', `${failure.stdout ?? ''}\n${failure.stderr ?? ''}`, { mode: 0o600 })
    throw new Error(`${path} failed (${String(failure.code)}); private diagnostic log saved under qualification-results`)
  }
}
async function deploy(noCron = false) {
  await script('scripts/neon-test-function.mjs', { NEON_TEST_STATE_FILE: branchFile, NEON_TEST_FUNCTION_ENV_FILE: functionFile,
    NEON_TEST_MAINTENANCE_HINTS: 'off', NEON_TEST_NO_CRON: noCron ? '1' : '0' })
  deployed = Object.fromEntries(Object.entries(parseEnv(await readFile(functionFile, 'utf8'))).filter((entry): entry is [string, string] => entry[1] !== undefined))
  for (const [key, value] of Object.entries(deployed)) if (/KEY|URL/.test(key)) mask(value)
  schema = deployed.TEST_FUNCTION_SCHEMA!
  if (!/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Invalid restored Function schema')
}
function connect() {
  pool = new pg.Pool({ connectionString: data.TEST_DATABASE_URL_UNPOOLED!, max: 3, connectionTimeoutMillis: 20_000, query_timeout: 60_000 })
  pool.on('error', () => {})
  objects = createS3ChunkStorage({ endpoint: data.AWS_ENDPOINT_URL_S3!, region: data.AWS_REGION!, bucket: data.NEON_TEST_BUCKET!,
    credentials: { accessKeyId: data.AWS_ACCESS_KEY_ID!, secretAccessKey: data.AWS_SECRET_ACCESS_KEY! } })
}
async function request(path: string, method = 'GET', body?: string | Uint8Array, headers: Record<string, string> = {}, signal?: AbortSignal) {
  try {
    return await fetch(new URL(path, deployed.TEST_FUNCTION_URL), { method,
      headers: { Authorization: `Bearer ${deployed.TEST_FUNCTION_API_KEY}`, 'Content-Type': 'application/octet-stream', ...headers },
      ...(body === undefined ? {} : { body: typeof body === 'string' ? body : new Uint8Array(body) }), signal: signal ?? AbortSignal.timeout(20_000) })
  } catch (error) { throw new Error(`In-place public ${method} ${path} failed during ${stages.at(-1)}`, { cause: error }) }
}
const stream = (path: string) => `v1/stream/${path}`
async function maintain() {
  const response = await request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': deployed.TEST_FUNCTION_MAINTENANCE_KEY! })
  expect(response.status).toBe(200); await response.arrayBuffer()
}

beforeAll(async () => {
  for (const path of [stateFile, branchFile, functionFile, environmentFile]) {
    try { await access(path) } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue; throw error }
    throw new Error(`Refusing to overwrite ${path}; clean up the prior in-place drill first`)
  }
  const parent = await neon.branches.get({ projectId, branchId: parentId })
  if (!parent.default || parent.expires_at) throw new Error('In-place fixture requires the non-expiring default test parent')
  await writeFile(stateFile, JSON.stringify({ projectId, parentId, name, preservedName }), { mode: 0o600, flag: 'wx' })
  ownsFixture = true
  const branch = await neon.branches.create({ projectId, parent_id: parentId, name,
    expires_at: new Date(Date.now() + 3600_000).toISOString(), compute: { minCu: 0.25, maxCu: 1, suspendTimeoutSeconds: 300 } })
  branchId = branch.id
  await writeFile(stateFile, JSON.stringify({ projectId, parentId, name, preservedName, branchId }), { mode: 0o600 })
  await writeFile(branchFile, JSON.stringify({ projectId, name, branchId }), { mode: 0o600, flag: 'wx' })
  await neon.realtime.enable({ projectId, branchId })
  await neon.storage.buckets.create({ projectId, branchId, name: 'inplace-drill', access_level: 'private' })
  const [pooled, direct, realtime, secret, storage, credential] = await Promise.all([
    neon.postgres.connectionString({ projectId, branchId, pooled: true }), neon.postgres.connectionString({ projectId, branchId, pooled: false }),
    neon.realtime.get({ projectId, branchId }), neon.realtime.secret({ projectId, branchId }), neon.storage.get({ projectId, branchId }),
    neon.credentials.create({ projectId, branchId, name: 'inplace-drill', principal_type: 'user', scopes: ['storage:read', 'storage:write'], expires_at: new Date(Date.now() + 3600_000).toISOString() }),
  ])
  if (!realtime.enabled || !realtime.invocation_url || realtime.pending || secret.pending) throw new Error('In-place Realtime is not ready')
  data = { TEST_DATABASE_URL: verified(pooled), TEST_DATABASE_URL_UNPOOLED: verified(direct), NEON_REALTIME_URL: realtime.invocation_url,
    NEON_REALTIME_SECRET: secret.secret, NEON_TEST_BRANCH_ID: branchId, NEON_TEST_PROJECT_ID: projectId,
    AWS_ENDPOINT_URL_S3: storage.s3_endpoint, AWS_REGION: storage.region, AWS_ACCESS_KEY_ID: credential.token_id,
    AWS_SECRET_ACCESS_KEY: credential.s3_secret_access_key!, NEON_TEST_BUCKET: 'inplace-drill',
    DURABLE_STREAMS_KEYRING: JSON.stringify({ currentKeyId: 'v1', keys: { v1: randomBytes(32).toString('base64') } }) }
  for (const value of Object.values(data)) mask(value)
  await writeFile(environmentFile, Object.entries(data).map(([key, value]) => `${key}=${value}`).join('\n') + '\n', { mode: 0o600, flag: 'wx' })
  stage('deploy initial Function and cron')
  await deploy(); connect()
}, 300_000)

afterAll(async () => {
  realtimeObserver?.close()
  liveAbort.abort(); await liveReader?.cancel().catch(() => {})
  await pool?.end(); objects?.close()
  if (ownsFixture && !(env.GITHUB_ACTIONS !== 'true' && env.NEON_TEST_KEEP_INPLACE === '1')) await script('scripts/neon-test-inplace-cleanup.mjs')
}, 180_000)

test('a stopped deployed Function restores its own branch, reconciles sealed bytes, and resumes in a fresh epoch', async () => {
  stage('seed binary, JSON, fork, and subscription state')
  const initialDeploymentId = deployed.TEST_FUNCTION_DEPLOYMENT_ID
  expect(initialDeploymentId).toMatch(/^[0-9a-f-]{36}$/)
  const initial = Buffer.from(Array.from({ length: 8192 }, (_, i) => i % 256))
  const tuple = { 'Producer-Id': 'inplace-writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  expect((await request(stream('data/binary'), 'PUT', initial)).status).toBe(201)
  expect((await request(stream('data/binary'), 'POST', ' once', tuple)).status).toBe(200)
  expect((await request(stream('fork'), 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/data/binary' })).status).toBe(201)
  const json = '[9007199254740993,1e400,-0,{"ok":true}]'
  expect((await request(stream('data/json'), 'PUT', json, { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await request(stream('wake'), 'PUT', '[]', { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await request(stream('__ds/subscriptions/restore'), 'PUT', JSON.stringify({ type: 'pull-wake', streams: ['data/binary'], wake_stream: 'wake', lease_ttl_ms: 600_000 }), { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await request(stream('data/binary'), 'POST', ' pending')).status).toBe(204)
  await maintain()
  const claimed = await request(stream('__ds/subscriptions/restore/claim'), 'POST', '{"worker":"old-worker"}', { 'Content-Type': 'application/json' })
  expect(claimed.status).toBe(200)
  const claim = await claimed.json() as { token: string; wake_id: string; generation: number }
  expect(claim.token).toMatch(/^dsw\./); mask(claim.token)
  // Restore across sealing as well: this frame is HOT at the recovery point,
  // then becomes SEALED on the deliberately discarded future timeline.
  expect((await request(stream('data/hot'), 'PUT', 'hot across restore')).status).toBe(201)
  const original = await inspectInstallation(pool!, { schema })
  const lsn = (await pool!.query<{ lsn: string }>('SELECT pg_current_wal_flush_lsn() AS lsn')).rows[0]!.lsn
  const oldHost = new URL(data.TEST_DATABASE_URL_UNPOOLED!).hostname
  expect(Number((await pool!.query(`SELECT count(*) FROM "${schema}".chunks WHERE state='sealed'`)).rows[0].count)).toBeGreaterThanOrEqual(3)

  stage('observe live future bytes before rollback')
  const live = await request(`${stream('data/binary')}?offset=now&live=sse`, 'GET', undefined, {}, liveAbort.signal)
  expect(live.status).toBe(200); liveReader = live.body!.getReader()
  const decoder = new TextDecoder(); let liveText = ''
  while (!liveText.includes('event: control')) liveText += decoder.decode((await liveReader.read()).value)
  const future = await request(stream('data/binary'), 'POST', ' future', { ...tuple, 'Producer-Seq': '1' })
  expect(future.status).toBe(200)
  const discardedOffset = future.headers.get('Stream-Next-Offset')!
  while (!liveText.includes(Buffer.from(' future').toString('base64'))) {
    const part = await liveReader.read(); if (part.done) throw new Error('Live stream ended before future bytes')
    liveText += decoder.decode(part.value)
  }
  expect((await request(stream('future-only'), 'PUT', 'deliberately rolled back')).status).toBe(201)
  await maintain()

  stage('stop scheduling, quiesce, and drain the active reader')
  for (const trigger of await neon.triggers.list({ projectId, branchId })) await neon.triggers.delete({ projectId, branchId, triggerId: trigger.trigger_id })
  const beforeRestore = await beginRecovery(pool!, { schema, operationId: randomUUID(), expectedInstallationId: original.id,
    expectedServingEpoch: original.serving_epoch, environmentId: branchId, storageIdentity: objects!.identity })
  for (;;) { if ((await liveReader.read()).done) break }
  liveReader.releaseLock(); liveReader = undefined
  await expect.poll(async () => Number((await pool!.query(`SELECT count(*) FROM "${schema}".live_leases WHERE expires_at>clock_timestamp()`)).rows[0].count), { timeout: 10_000, interval: 100 }).toBe(0)
  for (const fn of await neon.functions.list({ projectId, branchId }).all()) await neon.functions.delete({ projectId, branchId, slug: fn.slug })
  expect(await neon.functions.list({ projectId, branchId }).all()).toEqual([])
  expect(await neon.triggers.list({ projectId, branchId })).toEqual([])
  // The Functions runtime documents SIGINT followed by forced termination
  // after five seconds. Let that shutdown window elapse before rollback can
  // restore an epoch that an old runtime remembers.
  await new Promise((resolve) => setTimeout(resolve, 6000))
  await pool!.end(); pool = undefined; objects!.close(); objects = undefined

  stage('restore only the recorded disposable branch at its own WAL position')
  const target = await neon.branches.get({ projectId, branchId })
  if (target.name !== name || target.default || target.protected || target.id === parentId) throw new Error('Refusing restore outside the disposable fixture')
  // Self-restore preserves the replaced state as another branch. The exact
  // reserved name is already in the cleanup ledger, before this request.
  await neon.branches.update({ projectId, branchId, expires_at: null })
  const restored = await raw.restoreProjectBranch({ client: neon.client, path: { project_id: projectId, branch_id: branchId },
    body: { source_branch_id: branchId, source_lsn: lsn, preserve_under_name: preservedName }, throwOnError: false })
  if (!restored.data) throw new Error(`Neon self-restore failed with HTTP ${restored.response?.status}`)
  await neon.operations.waitFor({ operations: restored.data.operations })
  const preserved = (await neon.branches.list({ projectId }).all()).find((branch) => branch.name === preservedName)
  expect(preserved).toBeDefined(); expect(preserved!.id).not.toBe(branchId)
  // Neon makes the preserved history a parent of the restored branch. A
  // parent cannot expire; the ledger and scheduled janitor cover its cleanup.
  await neon.branches.update({ projectId, branchId, expires_at: new Date(Date.now() + 3600_000).toISOString() })
  const afterConnection = verified(await neon.postgres.connectionString({ projectId, branchId, pooled: false }))
  expect(new URL(afterConnection).hostname).toBe(oldHost)
  data.TEST_DATABASE_URL_UNPOOLED = afterConnection
  connect()

  stage('verify rollback and reconcile before exposing ingress')
  const rolledBack = await inspectInstallation(pool!, { schema })
  expect(rolledBack.serving_epoch).toBe(original.serving_epoch)
  expect(rolledBack.environment_id).toBe(branchId)
  expect((await pool!.query(`SELECT lease_until>clock_timestamp() AS unexpired FROM "${schema}".subscriptions WHERE name='restore'`)).rows[0].unexpired).toBe(true)
  expect(Number((await pool!.query(`SELECT count(*) FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id=a.stream_id WHERE s.path='data/hot' AND a.payload IS NOT NULL`)).rows[0].count)).toBe(1)
  await expect(finishRecovery(pool!, { schema, token: beforeRestore.token })).rejects.toThrow('stale')
  const recovery = await beginRecovery(pool!, { schema, operationId: randomUUID(), expectedInstallationId: original.id,
    expectedServingEpoch: rolledBack.serving_epoch, environmentId: branchId, storageIdentity: objects!.identity })
  expect(recovery.servingEpoch).not.toBe(original.serving_epoch)
  expect(recovery.servingEpoch).not.toBe(beforeRestore.servingEpoch)
  let verifiedChunks = 0
  for (;;) {
    const result = await verifyRecovery(pool!, { schema, token: recovery.token, objects: objects!, keyring: parseSubscriptionKeyring(data.DURABLE_STREAMS_KEYRING!), maxChunks: 1 })
    verifiedChunks = result.verified
    if (result.complete) break
  }
  expect(verifiedChunks).toBeGreaterThanOrEqual(3)
  const activated = await finishRecovery(pool!, { schema, token: recovery.token })
  expect(await finishRecovery(pool!, { schema, token: recovery.token })).toEqual(activated)
  expect(activated.serving_epoch).toBe(recovery.servingEpoch)
  expect((await pool!.query(`SELECT state FROM "${schema}".subscription_wakes WHERE id=$1`, [claim.wake_id.slice(2)])).rows[0].state).toBe('fenced')
  expect(await neon.functions.list({ projectId, branchId }).all()).toEqual([])

  stage('redeploy fresh instances and validate public protocol continuity')
  await deploy(true)
  expect(await neon.triggers.list({ projectId, branchId })).toEqual([])
  expect(deployed.TEST_FUNCTION_DEPLOYMENT_ID).not.toBe(initialDeploymentId)
  const health = await request('health')
  if (!health.ok) throw new Error(`Post-restore public health HTTP ${health.status}: ${await health.text()}`)
  expect(health.status).toBe(200)
  expect(await health.json()).toMatchObject({ ready: true, deploymentId: deployed.TEST_FUNCTION_DEPLOYMENT_ID })
  const binary = await request(stream('data/binary'))
  if (!binary.ok) throw new Error(`Post-restore public replay HTTP ${binary.status}: ${await binary.text()}`)
  expect(binary.status).toBe(200)
  expect(Buffer.from(await binary.arrayBuffer())).toEqual(Buffer.concat([initial, Buffer.from(' once pending')]))
  expect(Buffer.from(await (await request(stream('fork'))).arrayBuffer())).toEqual(Buffer.concat([initial, Buffer.from(' once')]))
  expect(await (await request(stream('data/json'))).text()).toBe(json)
  expect(await (await request(stream('data/hot'))).text()).toBe('hot across restore')
  expect((await request(stream('future-only'))).status).toBe(404)
  expect((await request(`${stream('data/binary')}?offset=${discardedOffset}`)).status).toBe(400)
  expect((await request(stream('data/binary'), 'POST', 'must deduplicate', tuple)).status).toBe(204)
  stage('verify restored Realtime before resuming workers')
  // An SSE setup catch-up can deliver an append even before periodic SQL
  // fallback. Independently observe its committed payload through Realtime,
  // with the raw subscription ready before the public append is issued.
  const currentStream = (await pool!.query<{ id: string; tenant_id: string; tail_offset: string }>(
    `SELECT id,tenant_id,tail_offset FROM "${schema}".streams WHERE path='data/binary' AND deleted_at IS NULL`)).rows[0]!
  let observedAppend: RealtimeAppend | undefined
  const restoredPayload = Buffer.from(' new timeline')
  // Function HTTP readiness and Realtime query readiness recover separately.
  // Bound startup before reopening the live fixture; the append/delivery
  // deadlines below start only AFTER an authoritative raw watch is ready.
  const realtimeStarted = performance.now(), realtimeDeadline = realtimeStarted + 180_000
  const realtimeReadiness: { elapsedMs: number; ready: boolean; reason?: string }[] = []
  let appendWatch: HeadWatch | undefined
  for (;;) {
    const remaining = realtimeDeadline - performance.now()
    if (remaining <= 0) throw new Error('Restored Realtime readiness exceeded its 180-second startup budget')
    realtimeObserver = createNeonRealtimeNotifier({ url: data.NEON_REALTIME_URL!, secret: data.NEON_REALTIME_SECRET!,
      database: new URL(data.TEST_DATABASE_URL_UNPOOLED!).pathname.slice(1), readyTimeoutMs: Math.min(30_000, Math.max(1, Math.floor(remaining / 2))) })
    appendWatch = realtimeObserver.watchAppends!({ schema, tenantId: currentStream.tenant_id, streamId: currentStream.id,
      after: currentStream.tail_offset, installation: activated }, (update) => {
      if (update.type === 'invalidate') return
      observedAppend ??= update.rows.find((row) => row.streamId === currentStream.id && row.start === currentStream.tail_offset
        && row.payload !== null && Buffer.from(row.payload).equals(restoredPayload))
    })
    try {
      await appendWatch.ready
      realtimeReadiness.push({ elapsedMs: Math.round(performance.now() - realtimeStarted), ready: true })
      break
    } catch (error) {
      appendWatch.close(); realtimeObserver.close(); realtimeObserver = undefined
      // Identity/authorization/SQL failures are not a reason to keep waiting.
      if (!(error instanceof Error) || !['Timed out waiting for live-query rows', 'Realtime append snapshot deadline exceeded'].includes(error.message)) throw error
      realtimeReadiness.push({ elapsedMs: Math.round(performance.now() - realtimeStarted), ready: false, reason: error.message })
      if (performance.now() >= realtimeDeadline) throw new Error('Restored Realtime readiness exceeded its 180-second startup budget', { cause: error })
      console.info(`In-place restore: Realtime startup snapshot not ready after ${realtimeReadiness.at(-1)!.elapsedMs}ms; retrying a fresh subscription`)
    }
  }
  const resumed = await request(`${stream('data/binary')}?offset=now&live=sse`, 'GET', undefined, {}, AbortSignal.any([liveAbort.signal, AbortSignal.timeout(45_000)]))
  expect(resumed.status).toBe(200); liveReader = resumed.body!.getReader(); liveText = ''
  while (!liveText.includes('event: control')) liveText += decoder.decode((await liveReader.read()).value)
  const restoredAppend = await request(stream('data/binary'), 'POST', restoredPayload, { ...tuple, 'Producer-Seq': '1' })
  expect(restoredAppend.status).toBe(200)
  await expect.poll(() => observedAppend?.end, { timeout: 30_000, interval: 50,
    message: 'The restored branch must independently emit the committed raw Realtime payload' }).toBe(restoredAppend.headers.get('Stream-Next-Offset'))
  while (!liveText.includes(Buffer.from(' new timeline').toString('base64'))) {
    const part = await liveReader.read(); if (part.done) throw new Error('Restored Realtime session ended before new bytes')
    liveText += decoder.decode(part.value)
  }
  await liveReader.cancel(); liveReader.releaseLock(); liveReader = undefined
  appendWatch.close(); realtimeObserver!.close(); realtimeObserver = undefined
  stage('verify restored token fencing and resumed subscription work')
  const oldAck = await request(stream('__ds/subscriptions/restore/ack'), 'POST', JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, done: true }),
    { Authorization: `Bearer ${claim.token}`, 'Content-Type': 'application/json' })
  expect(oldAck.status).toBe(409)
  await maintain()
  const nextClaim = await request(stream('__ds/subscriptions/restore/claim'), 'POST', '{"worker":"fresh-worker"}', { 'Content-Type': 'application/json' })
  expect(nextClaim.status).toBe(200)
  expect((await nextClaim.json() as { token: string }).token).not.toBe(claim.token)
  stage('resume scheduling after public validation')
  const cron = await neon.triggers.create({ projectId, branchId, type: 'schedule', name: 'durable-streams-maintenance',
    function_slug: 'durablestreams', function_path: '/_trigger', schedule: { cron: '* * * * *' } })
  expect(cron.enabled).toBe(true)
  await mkdir('qualification-results', { recursive: true })
  const fn = await neon.functions.get({ projectId, branchId, slug: 'durablestreams' })
  await writeFile('qualification-results/inplace-restore.json', JSON.stringify({ passed: true, stages, stageTimes, durationMs: Math.round(performance.now() - startedAt), verifiedChunks, generatedAt: new Date().toISOString(),
    testSha256, node: process.version, region: data.AWS_REGION,
    branchId, preservedBranchId: preserved!.id, endpointPreserved: true, originalEpoch: original.serving_epoch,
    recoveryEpoch: recovery.servingEpoch, hotReplayAcrossSealing: true, restoredRealtime: true,
    restoredRealtimePayloadObserved: true, restoredRealtimeObservedOffset: observedAppend!.end,
    restoredRealtimeReadiness: realtimeReadiness,
    discardedOffsetRejected: true, unexpiredTokenFenced: true, cronResumedAfterValidation: true,
    sourceCommit: deployed.TEST_FUNCTION_SOURCE_COMMIT, sourceDirty: deployed.TEST_FUNCTION_SOURCE_DIRTY,
    deploymentId: deployed.TEST_FUNCTION_DEPLOYMENT_ID, previousDeploymentId: initialDeploymentId,
    memoryMiB: fn.active_deployment?.memory_mib, deployedBundle: deployed.TEST_FUNCTION_BUNDLE_SHA }, null, 2) + '\n')
}, 600_000)
