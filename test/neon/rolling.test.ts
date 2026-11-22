import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { access, mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { build } from 'esbuild'
import { zipSync } from 'fflate'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { migrate, migrationPlan } from '../../src/migrations.js'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../../src/operations.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'

const env = process.env
if (!env.NEON_API_KEY || !env.NEON_PROJECT_ID || !env.NEON_TEST_BRANCH_ID || !env.TEST_DATABASE_URL_UNPOOLED
  || !env.NEON_REALTIME_URL || !env.NEON_REALTIME_SECRET || !env.NEON_TEST_BUCKET || !env.DURABLE_STREAMS_KEYRING
  || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION) throw new Error('Mixed-version qualification requires recorded actual Neon services')
const projectId = env.NEON_PROJECT_ID, branchId = env.NEON_TEST_BRANCH_ID
const coordinator = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8')) as { projectId: string; branchId: string; name: string }
if (coordinator.projectId !== projectId || coordinator.branchId !== branchId || !coordinator.name.startsWith('ds-test-')) throw new Error('Unrecorded mixed-version coordinator')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`, suffix = randomUUID().replaceAll('-', '').slice(0, 12)
const slugs = ['a', 'b', 'c', 'd'].map((label) => `dsmix${label}${suffix}`)
const stateFile = '.neon-test-state-rolling.json', artifactsFile = '.neon-test-state-rolling-artifacts.json'
const testSha256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex')
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true, waitForReadiness: true, wait: { timeoutMs: 180_000 } })
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 3, connectionTimeoutMillis: 20_000, query_timeout: 60_000 })
pool.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING)
const adminKey = randomBytes(32).toString('hex'), userA = randomBytes(32).toString('hex'), userB = randomBytes(32).toString('hex')
const run = promisify(execFile)
interface Artifact { commit: string; sourceDirty: boolean; packageRoot: string; consumer: string; sourceSnapshotSha256: string; tarballSha256: string; sourceLockSha256: string; consumerLockSha256: string }
interface Artifacts { directory: string; baselineSourceArchiveSha256: string; artifacts: { baseline: Artifact; current: Artifact } }
interface Runtime { kind: 'baseline' | 'current'; slug: string; url: string; revision: string; instance: string; bundleSha256: string; memoryMiB: number | undefined; runtimeNode?: string; architecture?: string }
let prepared: Artifacts, old: Runtime, current: Runtime, owned = false
const bundles = new Map<string, { code: Uint8Array<ArrayBuffer>; hash: string }>()
const runtimes: Runtime[] = []
const liveSessions = new Set<{ close(): Promise<void> }>()
const passed: string[] = []
let expected = 0
function qualify(name: string, work: () => Promise<void>, timeout = 60_000) {
  expected++
  test(name, async () => { await work(); passed.push(name) }, timeout)
}
const mask = (value: string) => { if (env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${value}`) }
async function script(path: string) {
  try { await run(process.execPath, [path], { env, timeout: 360_000, maxBuffer: 8 * 1024 * 1024 }) }
  catch (error) {
    const result = error as { stdout?: string; stderr?: string }
    await mkdir('qualification-results', { recursive: true })
    await writeFile('qualification-results/rolling-private.log', `${result.stdout ?? ''}\n${result.stderr ?? ''}`, { mode: 0o600 })
    throw new Error(`Rolling fixture tooling failed: ${path}; private diagnostic log saved`)
  }
}
async function deploy(kind: Runtime['kind'], slot: number): Promise<Runtime> {
  const artifact = prepared.artifacts[kind]
  let compiled = bundles.get(kind)
  if (!compiled) {
    const result = await build({ entryPoints: ['test/fixtures/rolling-function.ts'], bundle: true, platform: 'node', target: 'node24', format: 'esm', write: false,
      external: ['pg-native', 'bufferutil', 'utf-8-validate'], alias: {
        '@samwillis/durable-streams-neon': join(artifact.packageRoot, 'dist/index.js'),
        '@samwillis/durable-streams-neon/neon': join(artifact.packageRoot, 'dist/neon.js'),
        '@samwillis/durable-streams-neon/s3': join(artifact.packageRoot, 'dist/s3.js'),
        '@samwillis/durable-streams-neon/subscriptions': join(artifact.packageRoot, 'dist/subscriptions.js'),
        pg: join(artifact.consumer, 'node_modules/pg/lib/index.js'),
      }, banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' } })
    if (result.outputFiles.length !== 1) throw new Error('Expected one self-contained mixed-version bundle')
    const code = new Uint8Array(result.outputFiles[0]!.contents)
    compiled = { code, hash: createHash('sha256').update(code).digest('hex') }; bundles.set(kind, compiled)
  }
  const slug = slugs[slot]!, instance = randomUUID()
  const deployed = await neon.functions.deploy({ projectId, branchId, slug, runtime: 'nodejs24',
    zip: new File([zipSync({ 'index.mjs': compiled.code })], 'mixed-version.zip', { type: 'application/zip' }),
    environment: JSON.stringify({ DS_SCHEMA: schema, DS_ENVIRONMENT_ID: branchId, DS_BUCKET: env.NEON_TEST_BUCKET,
      DS_KEYRING_JSON: env.DURABLE_STREAMS_KEYRING, DS_REALTIME_URL: env.NEON_REALTIME_URL, DS_REALTIME_SECRET: env.NEON_REALTIME_SECRET,
      DS_ADMIN_KEY: adminKey, DS_USER_A_KEY: userA, DS_USER_B_KEY: userB, DS_REVISION: artifact.commit, DS_BUILD_HASH: compiled.hash, DS_INSTANCE: instance }) })
  const deadline = Date.now() + 180_000
  let runtime: Runtime | undefined, consecutive = 0
  while (Date.now() < deadline) {
    if (!runtime) {
      const fn = await neon.functions.get({ projectId, branchId, slug })
      if (fn.current_deployment?.id === deployed.id && fn.current_deployment.status === 'failed') throw new Error('Mixed-version Function failed to build')
      if (fn.active_deployment?.id === deployed.id && fn.invocation_url) runtime = { kind, slug, url: fn.invocation_url,
        revision: artifact.commit, instance, bundleSha256: compiled.hash, memoryMiB: fn.active_deployment.memory_mib }
    }
    if (runtime) {
      try {
        const response = await fetch(new URL('_health', runtime.url), { headers: { 'x-ds-admin-key': adminKey }, signal: AbortSignal.timeout(10_000) })
        const health = response.ok ? await response.json() as Record<string, unknown> : undefined
        if (!response.ok) await response.arrayBuffer()
        consecutive = health?.ready === true && health.revision === artifact.commit && health.build === compiled.hash && health.instance === instance ? consecutive + 1 : 0
        if (consecutive >= 3) {
          if (typeof health?.runtimeNode === 'string') runtime.runtimeNode = health.runtimeNode
          if (typeof health?.architecture === 'string') runtime.architecture = health.architecture
          runtimes.push(runtime); return runtime
        }
      } catch { consecutive = 0 }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('Mixed-version Function readiness deadline exceeded')
}
const request = (runtime: Runtime, path: string, method = 'GET', body?: string, headers: Record<string, string> = {}, signal?: AbortSignal) => fetch(new URL(`v1/stream/${path}`, runtime.url), {
  method, headers: { Authorization: `Bearer ${userA}`, 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }), signal: signal ?? AbortSignal.timeout(20_000),
})
async function maintain(runtime: Runtime) {
  const response = await fetch(new URL('_maintain', runtime.url), { method: 'POST', headers: { 'x-ds-admin-key': adminKey }, signal: AbortSignal.timeout(30_000) })
  expect(response.status).toBe(200); return response.json() as Promise<{ sealed: number }>
}
async function read(runtime: Runtime, path: string) {
  let offset = '-1', data = ''
  for (let i = 0; i < 100; i++) {
    const response = await request(runtime, `${path}?offset=${offset}`)
    expect(response.status).toBe(200); data += await response.text()
    if (response.headers.get('Stream-Up-To-Date') === 'true') return data
    const next = response.headers.get('Stream-Next-Offset')!; expect(next).not.toBe(offset); offset = next
  }
  throw new Error('Mixed-version replay did not reach the tail')
}
async function open(runtime: Runtime, path: string) {
  const abort = new AbortController()
  const response = await request(runtime, `${path}?live=sse&offset=now`, 'GET', undefined, {}, AbortSignal.any([abort.signal, AbortSignal.timeout(10_000)]))
  expect(response.status).toBe(200)
  const reader = response.body!.getReader(), decoder = new TextDecoder(); let content = ''
  const until = async (text: string) => {
    while (!content.includes(text)) { const part = await reader.read(); if (part.done) throw new Error(`Live session ended before ${text}`); content += decoder.decode(part.value) }
    return content
  }
  await until('event: control')
  let closed = false
  const session = { until, reader, async close() {
    if (closed) return
    closed = true; liveSessions.delete(session)
    abort.abort(); await reader.cancel().catch(() => {}); reader.releaseLock()
  } }
  liveSessions.add(session)
  return session
}

beforeAll(async () => {
  for (const path of [stateFile, artifactsFile]) { try { await access(path) } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue; throw error }; throw new Error('Clean up the prior mixed-version fixture before starting another') }
  for (const value of [adminKey, userA, userB]) mask(value)
  const uri = new URL(env.TEST_DATABASE_URL_UNPOOLED!)
  await writeFile(stateFile, JSON.stringify({ projectId, branchId, branchName: coordinator.name, schema, slugs, databaseHost: uri.hostname, databaseName: uri.pathname }), { flag: 'wx', mode: 0o600 })
  owned = true
  await script('scripts/neon-test-rolling-artifacts.mjs')
  prepared = JSON.parse(await readFile(artifactsFile, 'utf8')) as Artifacts
  expect(prepared.artifacts.baseline.commit).toBe('b22c1675f7eb5c32b52f39be39279c819ea5f21f')
  for (const migration of await migrationPlan(schema)) {
    const prior = await readFile(join(prepared.artifacts.baseline.packageRoot, 'migrations', `${migration.id}.sql`))
    expect(createHash('sha256').update(prior).digest('hex')).toBe(migration.checksum)
  }
  await migrate(pool, { schema, environmentId: branchId, storageIdentity: objects.identity, maxLiveReaders: 2 })
  await initializeSubscriptions(pool, { schema, keyring })
  old = await deploy('baseline', 0); current = await deploy('current', 1)
}, 600_000)
afterAll(async () => {
  await Promise.all([...liveSessions].map((session) => session.close()))
  await pool.end(); objects.close()
  let cleaned = false
  try {
    if (owned) await script('scripts/neon-test-rolling-cleanup.mjs')
    cleaned = true
  } finally {
    await mkdir('qualification-results', { recursive: true })
    await writeFile('qualification-results/rolling-upgrade.json', JSON.stringify({ passed: cleaned && passed.length === expected && expected === 8,
      checks: { expected, passed: passed.length, names: passed }, cleaned, generatedAt: new Date().toISOString(), testSha256,
      baselineSourceArchiveSha256: prepared?.baselineSourceArchiveSha256, node: process.version, region: env.AWS_REGION,
      artifacts: prepared ? Object.fromEntries(Object.entries(prepared.artifacts).map(([key, { packageRoot: _root, consumer: _consumer, ...artifact }]) => [key, artifact])) : {},
      runtimes: runtimes.map(({ url: _url, ...runtime }) => runtime), storageVersion: 1,
      migrations: (await migrationPlan(schema)).map(({ id, checksum }) => ({ id, checksum })),
    }, null, 2) + '\n')
  }
}, 180_000)

qualify('both packed versions share producer deduplication, ordering, and fencing during concurrent writes', async () => {
  expect((await request(old, 'producer', 'PUT', 'seed;')).status).toBe(201)
  const tuple = { 'Producer-Id': 'mixed', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  expect((await request(old, 'producer', 'POST', 'old;', tuple)).status).toBe(200)
  expect((await request(current, 'producer', 'POST', 'ignored', tuple)).status).toBe(204)
  expect((await request(current, 'producer', 'POST', 'new;', { ...tuple, 'Producer-Seq': '1' })).status).toBe(200)
  expect((await request(old, 'producer', 'POST', 'ignored', { ...tuple, 'Producer-Seq': '1' })).status).toBe(204)
  const race = await Promise.all(Array.from({ length: 8 }, (_, i) => request(i % 2 ? old : current, 'producer', 'POST', 'race;', { ...tuple, 'Producer-Seq': '2' })))
  expect(race.filter((response) => response.status === 200)).toHaveLength(1)
  expect(race.filter((response) => response.status === 204)).toHaveLength(7)
  expect((await request(current, 'producer', 'POST', 'epoch;', { ...tuple, 'Producer-Epoch': '1' })).status).toBe(200)
  expect((await request(old, 'producer', 'POST', 'stale', { ...tuple, 'Producer-Seq': '3' })).status).toBe(403)
  for (const runtime of [old, current]) expect(await read(runtime, 'producer')).toBe('seed;old;new;race;epoch;')
})

qualify('old and new seal workers publish mutually readable chunks and retain producer sessions', async () => {
  expect((await request(current, 'sealing', 'PUT')).status).toBe(201)
  const parts = Array.from({ length: 20 }, (_, i) => `part-${i.toString().padStart(2, '0')};`)
  for (let i = 0; i < parts.length; i++) expect((await request(i % 2 ? old : current, 'sealing', 'POST', parts[i], { 'Producer-Id': 'sealer', 'Producer-Epoch': '0', 'Producer-Seq': String(i) })).status).toBe(200)
  await Promise.all([maintain(old), maintain(current)])
  expect(Number((await pool.query(`SELECT count(*) FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id=a.stream_id WHERE s.path='sealing' AND a.payload IS NOT NULL`)).rows[0].count)).toBe(0)
  for (const runtime of [old, current]) {
    expect(await read(runtime, 'sealing')).toBe(parts.join(''))
    expect((await request(runtime, 'sealing', 'POST', 'duplicate', { 'Producer-Id': 'sealer', 'Producer-Epoch': '0', 'Producer-Seq': '19' })).status).toBe(204)
  }
})

qualify('cross-version forks preserve exact-number JSON and inherited sealed ranges', async () => {
  const json = '[9007199254740993,1e400,-0]'
  expect((await request(old, 'json', 'PUT', json, { 'Content-Type': 'application/json' })).status).toBe(201)
  await maintain(current)
  expect((await request(current, 'json-child', 'PUT', undefined, { 'Content-Type': 'application/json', 'Stream-Forked-From': '/v1/stream/json' })).status).toBe(201)
  expect((await request(old, 'json-grandchild', 'PUT', undefined, { 'Content-Type': 'application/json', 'Stream-Forked-From': '/v1/stream/json-child' })).status).toBe(201)
  expect((await request(current, 'json-partial', 'PUT', undefined, { 'Content-Type': 'application/json', 'Stream-Forked-From': '/v1/stream/json',
    'Stream-Fork-Offset': '0000000000000000_0000000000000000', 'Stream-Fork-Sub-Offset': '2' })).status).toBe(201)
  await maintain(old)
  expect(await read(old, 'json-partial')).toBe('[9007199254740993,1e400]')
  expect((await request(old, 'json-subpartial', 'PUT', undefined, { 'Content-Type': 'application/json', 'Stream-Forked-From': '/v1/stream/json-partial',
    'Stream-Fork-Offset': '0000000000000000_0000000000000000', 'Stream-Fork-Sub-Offset': '1' })).status).toBe(201)
  expect(await read(current, 'json-subpartial')).toBe('[9007199254740993]')
  expect((await request(old, 'json-child', 'POST', '[42]', { 'Content-Type': 'application/json' })).status).toBe(204)
  expect(await read(current, 'json-grandchild')).toBe(json)
  expect(await read(old, 'json-child')).toBe('[9007199254740993,1e400,-0,42]')
  expect((await request(current, 'json', 'DELETE')).status).toBe(204)
  expect((await request(old, 'json')).status).toBe(410)
  expect(await read(current, 'json-grandchild')).toBe(json)
})

qualify('subscription registration, dispatch, claims, and scoped acks interoperate across versions', async () => {
  await request(old, 'watched', 'PUT')
  await request(current, 'wake', 'PUT', '[]', { 'Content-Type': 'application/json' })
  expect((await request(old, '__ds/subscriptions/mixed', 'PUT', JSON.stringify({ type: 'pull-wake', streams: ['watched'], wake_stream: 'wake', lease_ttl_ms: 600_000 }), { 'Content-Type': 'application/json' })).status).toBe(201)
  const appended = await request(current, 'watched', 'POST', 'first;'); expect(appended.status).toBe(204)
  await maintain(old)
  const claimed = await request(current, '__ds/subscriptions/mixed/claim', 'POST', '{"worker":"current"}', { 'Content-Type': 'application/json' })
  expect(claimed.status).toBe(200)
  const claim = await claimed.json() as { token: string; wake_id: string; generation: number }; mask(claim.token)
  const ack = JSON.stringify({ wake_id: claim.wake_id, generation: claim.generation, done: true, acks: [{ stream: 'watched', offset: appended.headers.get('Stream-Next-Offset') }] })
  expect((await request(old, '__ds/subscriptions/mixed/ack', 'POST', ack, { Authorization: `Bearer ${claim.token}`, 'Content-Type': 'application/json' })).status).toBe(200)
  expect((await request(current, '__ds/subscriptions/mixed/ack', 'POST', ack, { Authorization: `Bearer ${claim.token}`, 'Content-Type': 'application/json' })).status).toBe(409)
  expect((await request(old, 'watched', 'POST', 'second;')).status).toBe(204)
  await maintain(current)
  const next = await request(old, '__ds/subscriptions/mixed/claim', 'POST', '{"worker":"baseline"}', { 'Content-Type': 'application/json' })
  expect(next.status).toBe(200)
  const fresh = await next.json() as { token: string; wake_id: string; generation: number }; mask(fresh.token)
  expect(fresh.generation).toBeGreaterThan(claim.generation)
  expect((await request(current, '__ds/subscriptions/mixed/release', 'POST', JSON.stringify({ wake_id: fresh.wake_id, generation: fresh.generation }), { Authorization: `Bearer ${fresh.token}`, 'Content-Type': 'application/json' })).status).toBe(204)
})

qualify('both Realtime implementations deliver the other version’s final append before EOF', async () => {
  for (const [readerRuntime, writer, path] of [[old, current, 'old-live'], [current, old, 'new-live']] as const) {
    await request(writer, path, 'PUT')
    const live = await open(readerRuntime, path)
    try {
      expect((await request(writer, path, 'POST', 'final;', { 'Stream-Closed': 'true' })).status).toBe(204)
      const content = await live.until('"streamClosed":true')
      expect(content).toContain('data:final;')
      expect(content.indexOf('final;')).toBeLessThan(content.indexOf('"streamClosed":true'))
    } finally { await live.close() }
  }
}, 30_000)

qualify('installation-wide admission bounds both versions and releases canceled readers', async () => {
  await request(old, 'capacity', 'PUT')
  const one = await open(old, 'capacity'), two = await open(current, 'capacity')
  try {
    expect((await request(current, 'capacity?live=sse&offset=now')).status).toBe(429)
    await one.close()
    await expect.poll(async () => Number((await pool.query(`SELECT count(*) FROM "${schema}".live_leases WHERE expires_at>clock_timestamp()`)).rows[0].count), { timeout: 5000, interval: 100 }).toBeLessThan(2)
    const replacement = await open(current, 'capacity'); await replacement.close()
  } finally { await one.close(); await two.close() }
  await expect.poll(async () => Number((await pool.query(`SELECT count(*) FROM "${schema}".live_leases WHERE expires_at>clock_timestamp()`)).rows[0].count), { timeout: 5000, interval: 100 }).toBe(0)
}, 30_000)

qualify('tenant and generation isolation survive cross-version path deletion and recreation', async () => {
  await request(old, 'generation', 'PUT', 'old;')
  expect((await request(current, 'generation', 'GET', undefined, { Authorization: `Bearer ${userB}` })).status).toBe(404)
  const live = await open(current, 'generation')
  try {
    expect((await request(old, 'generation', 'DELETE')).status).toBe(204)
    expect((await request(old, 'generation', 'PUT', 'replacement;')).status).toBe(201)
    let content = ''
    for (;;) { const chunk = await live.reader.read(); if (chunk.done) break; content += new TextDecoder().decode(chunk.value) }
    expect(content).not.toContain('replacement;')
  } finally { await live.close() }
  expect(await read(current, 'generation')).toBe('replacement;')
}, 20_000)

qualify('recovery fences both running binaries and fresh old/current artifacts can resume the installation', async () => {
  await Promise.all([maintain(old), maintain(current)])
  const identity = await inspectInstallation(pool, { schema })
  const recovery = await beginRecovery(pool, { schema, operationId: randomUUID(), expectedInstallationId: identity.id,
    expectedServingEpoch: identity.serving_epoch, environmentId: branchId, storageIdentity: objects.identity })
  for (const runtime of [old, current]) expect((await request(runtime, 'producer', 'POST', 'fenced;')).status).toBe(503)
  while (!(await verifyRecovery(pool, { schema, token: recovery.token, objects, keyring, maxChunks: 8 })).complete) {}
  await finishRecovery(pool, { schema, token: recovery.token })
  for (const runtime of [old, current]) expect((await request(runtime, 'producer', 'POST', 'still fenced;')).status).toBe(503)
  const freshCurrent = await deploy('current', 2), freshOld = await deploy('baseline', 3)
  for (const runtime of [freshCurrent, freshOld]) expect(await read(runtime, 'producer')).toBe('seed;old;new;race;epoch;')
  expect((await request(freshCurrent, 'producer', 'POST', 'after recovery;', { 'Producer-Id': 'mixed', 'Producer-Epoch': '1', 'Producer-Seq': '1' })).status).toBe(200)
  expect((await request(freshOld, 'producer', 'POST', 'duplicate', { 'Producer-Id': 'mixed', 'Producer-Epoch': '1', 'Producer-Seq': '1' })).status).toBe(204)
  expect(await read(freshOld, 'producer')).toBe('seed;old;new;race;epoch;after recovery;')
}, 480_000)
