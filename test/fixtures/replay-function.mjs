// Private benchmark consumer. Instrumentation lives outside the library API.
import { AsyncLocalStorage } from 'node:async_hooks'
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto'
import { attachDatabasePool } from '@neon/functions'
import pg from 'pg'
import { createDurableStreams } from '@samwillis/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@samwillis/durable-streams-neon/neon'
import { createS3ChunkStorage } from '@samwillis/durable-streams-neon/s3'
import { parseSubscriptionKeyring } from '@samwillis/durable-streams-neon/subscriptions'
import { replayProfiles, replayFrame } from './replay-data.mjs'

const required = (name) => {
  if (!process.env[name]) throw new Error(`Missing ${name}`)
  return process.env[name]
}
const schema = required('DS_SCHEMA')
if (!/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Replay fixture requires a test schema')
const hash = (value) => createHash('sha256').update(value).digest()
const key = hash(required('DS_BENCHMARK_KEY'))
const authorized = (request) => {
  const token = request.headers.get('authorization')
  return !!token && token.length < 1024 && timingSafeEqual(hash(token), key)
}
const settings = JSON.parse(required('DS_BENCHMARK_SETTINGS'))
const instanceId = randomUUID()
const context = new AsyncLocalStorage()
const pool = new pg.Pool({ connectionString: required('DATABASE_URL'), max: 5, connectionTimeoutMillis: 10000, query_timeout: 15000 })
attachDatabasePool(pool)
pool.on('connect', (client) => {
  const original = client.query.bind(client)
  client.query = (...args) => {
    const metrics = context.getStore()
    if (!metrics) return original(...args)
    const sql = typeof args[0] === 'string' ? args[0] : args[0].text ?? ''
    metrics.sqlCalls++
    if (/^BEGIN/.test(sql)) metrics.transactions++
    if (/INSERT INTO .*replay_pins/.test(sql)) metrics.pinInsertCalls++
    if (/DELETE FROM .*replay_pins/.test(sql)) metrics.pinDeleteCalls++
    const started = performance.now()
    const finish = () => { metrics.sqlMs += performance.now() - started }
    const callback = args.at(-1)
    if (typeof callback === 'function') {
      args[args.length - 1] = (...result) => { finish(); callback(...result) }
      return original(...args)
    }
    return original(...args).finally(finish)
  }
})
const notifier = createNeonRealtimeNotifier({ url: required('DS_REALTIME_URL'), secret: required('DS_REALTIME_SECRET'),
  database: new URL(required('DATABASE_URL')).pathname.slice(1) })
const rawObjects = createS3ChunkStorage({ endpoint: required('AWS_ENDPOINT_URL_S3'), region: required('AWS_REGION'), bucket: required('DS_BUCKET') })
async function objectRead(method, args) {
  const metrics = context.getStore()
  const started = performance.now()
  if (metrics) { metrics.objectGets++; if (method === 'getRange') metrics.objectRangeGets++ }
  try {
    const bytes = await rawObjects[method](...args)
    if (metrics) metrics.objectBytes += bytes.byteLength
    return bytes
  } finally { if (metrics) metrics.objectMs += performance.now() - started }
}
async function rangeRead(key, range, consume, signal) {
  const metrics = context.getStore(), started = performance.now()
  if (metrics) { metrics.objectGets++; metrics.objectRangeGets++ }
  try { return await rawObjects.readRange(key, range, async (bytes) => {
    if (metrics) metrics.objectBytes += bytes.byteLength
    await consume(bytes)
  }, signal) } finally { if (metrics) metrics.objectMs += performance.now() - started }
}
const objects = { ...rawObjects, readRange: rangeRead, get: (...args) => objectRead('get', args), getRange: (...args) => objectRead('getRange', args) }
function server(cacheBytes = settings.cacheBytes, pageBytes) {
  return createDurableStreams({ postgres: { pool, schema, environmentId: required('DS_ENVIRONMENT_ID') },
    auth: { authenticate: () => ({ tenantId: 'replay', subject: 'benchmark' }), authorize: () => true },
    storage: { objects, targetChunkBytes: settings.targetChunkBytes, maxChunkFrames: settings.maxChunkFrames, blockBytes: settings.blockBytes, readAhead: settings.readAhead ?? 1, cacheBytes },
    ...(pageBytes ? { limits: { maxPageBytes: pageBytes, maxStoredBytes: pageBytes - 2 } } : {}),
    subscriptions: { keyring: parseSubscriptionKeyring(required('DS_KEYRING_JSON')) },
    live: { notifier, maxReaders: 64, maxSseDurationMs: 10000, heartbeatMs: 1000 },
  })
}
const shared = server()
const request = (handler, path, method = 'GET', body, headers = {}) => handler.fetch(new Request(`https://benchmark.invalid/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'application/octet-stream', ...headers }, ...(body === undefined ? {} : { body }),
}))

async function seed(input) {
  const profile = replayProfiles[input.profile]
  if (!profile || !/^replay-[a-f0-9-]{36}$/.test(input.run) || !Number.isSafeInteger(input.frames) || input.frames < 1 || input.frames > profile.frames) throw new Error('Invalid replay dataset')
  const path = `${input.run}/${input.profile}`
  const created = await request(shared, path, 'PUT', undefined, { 'Content-Type': profile.contentType })
  if (created.status !== 201) throw new Error('Benchmark stream already exists')
  const offsets = [created.headers.get('Stream-Next-Offset')]
  for (let index = 0; index < input.frames; index++) {
    const response = await request(shared, path, 'POST', replayFrame(input.profile, index), { 'Content-Type': profile.contentType })
    await response.arrayBuffer()
    if (response.status !== 204) throw new Error(`Benchmark append failed at ${index} with ${response.status}`)
    offsets.push(response.headers.get('Stream-Next-Offset'))
  }
  return { path, offsets, profile: input.profile, frames: input.frames }
}

async function handle(request) {
  if (!authorized(request)) return new Response(null, { status: 401 })
  const url = new URL(request.url)
  if (url.pathname === '/_benchmark/health') return Response.json({ instanceId, settings, build: required('DS_BUILD_HASH'), sourceCommit: required('DS_SOURCE_COMMIT'), ready: (await shared.inspectHealth()).ready })
  if (url.pathname === '/_benchmark/seed' && request.method === 'POST') return Response.json(await seed(await request.json()))
  if (url.pathname === '/_benchmark/maintain' && request.method === 'POST') return Response.json(await shared.maintain({ forceHot: true, maxChunks: 128, timeBudgetMs: 40000 }))
  if (url.pathname === '/_benchmark/storage') {
    const relations = await pool.query(`SELECT relname,pg_relation_size(c.oid) AS heap_bytes,pg_indexes_size(c.oid) AS index_bytes,
      pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname=$1 AND c.relkind='r' ORDER BY relname`, [schema])
    const counts = await pool.query(`SELECT (SELECT count(*) FROM ${schema}.appends) AS appends,
      (SELECT count(*) FROM ${schema}.chunks WHERE state='frozen') AS frozen,
      (SELECT count(*) FROM ${schema}.chunks WHERE state='sealed') AS sealed,
      (SELECT sum(encoded_bytes) FROM ${schema}.chunks WHERE state='sealed') AS object_bytes,
      (SELECT count(*) FROM ${schema}.replay_pins) AS replay_pins`)
    return Response.json({ relations: relations.rows, counts: counts.rows[0] })
  }
  if (!url.pathname.startsWith('/v1/stream/replay-')) return new Response(null, { status: 404 })
  const cold = request.headers.get('x-replay-cache') === 'cold'
  const firstBlock = request.headers.get('x-replay-first-block') === '1'
  const isolated = cold || firstBlock
  const handler = isolated ? server(0, firstBlock ? settings.blockBytes : undefined) : shared
  const metrics = { sqlCalls: 0, transactions: 0, pinInsertCalls: 0, pinDeleteCalls: 0, sqlMs: 0, objectGets: 0, objectRangeGets: 0, objectBytes: 0, objectMs: 0 }
  const started = performance.now()
  const before = process.memoryUsage()
  let response
  try { response = await context.run(metrics, () => handler.fetch(request)) }
  catch (error) { if (isolated) await handler.close(); throw error }
  if (isolated && !response.body) await handler.close()
  const headers = new Headers(response.headers)
  headers.set('x-replay-metrics', JSON.stringify({ ...metrics, handlerMs: performance.now() - started,
    instanceId, cache: cold ? 'disabled' : 'shared', memoryBefore: before, memoryAfter: process.memoryUsage(),
    pool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount } }))
  let body = response.body
  if (isolated && body) {
    const reader = body.getReader()
    body = new ReadableStream({
      async pull(controller) {
        try {
          const next = await reader.read()
          if (next.done) { controller.close(); await handler.close() }
          else controller.enqueue(next.value)
        } catch (error) { controller.error(error); await handler.close() }
      },
      async cancel(reason) { try { await reader.cancel(reason) } finally { await handler.close() } },
    }, { highWaterMark: 0 })
  }
  return new Response(body, { status: response.status, headers })
}

process.once('SIGINT', () => { void shared.close().finally(async () => { notifier.close(); rawObjects.close(); await pool.end() }) })
export default { fetch: handle }
