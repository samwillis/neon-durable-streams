import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, afterEach, beforeAll, expect, test, vi } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import type { AppendUpdate, HeadWatch, StreamNotifier } from '../../src/neon.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { PostgresStreamStore } from '../../src/store.js'
import { beginRecovery, inspectInstallation, verifyRecovery, finishRecovery } from '../../src/operations.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.NEON_REALTIME_URL || !env.NEON_REALTIME_SECRET
  || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Payload qualification requires real Neon services')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 8, connectionTimeoutMillis: 120_000 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {}); direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const errors: unknown[] = []
const native = createNeonRealtimeNotifier({ url: env.NEON_REALTIME_URL, secret: env.NEON_REALTIME_SECRET, database: new URL(env.TEST_DATABASE_URL).pathname.slice(1), onError: (error) => errors.push(error) })
const watches: { after: string; ready: Promise<void>; listener: (update: AppendUpdate) => void }[] = []
const heads: HeadWatch[] = []
let beforeSubscribe: (() => Promise<void>) | undefined
let holdInvalidations = false
const notifier: StreamNotifier = { watch(input, listener) { const watch = native.watch(input, listener); heads.push(watch); return watch },
  watchAppends(input, listener) {
    const deliver = (update: AppendUpdate) => { if (!holdInvalidations || update.type !== 'invalidate') listener(update) }
    const hook = beforeSubscribe; beforeSubscribe = undefined
    if (!hook) { const watch = native.watchAppends!(input, deliver); watches.push({ after: input.after, ready: watch.ready, listener }); return watch }
    let inner: HeadWatch | undefined, closed = false
    const ready = (async () => { await hook(); inner = native.watchAppends!(input, deliver); if (closed) inner.close(); await inner.ready })()
    watches.push({ after: input.after, ready, listener })
    return { ready, close() { closed = true; inner?.close() } }
  } }
const servers: DurableStreamsServer[] = []
const server = (options: { frames?: number; windowMs?: number; validationMs?: number; authMs?: number; environmentId?: string } = {}) => {
  const value = createDurableStreams({ postgres: { pool, schema, ...postgresTestLimits, ...(options.environmentId ? { environmentId: options.environmentId } : {}) },
    auth: { authenticate: () => ({ tenantId: 'payload', subject: 'fixture', ...(options.authMs ? { expiresAt: Date.now() + options.authMs } : {}) }), authorize: () => true },
    storage: { objects, targetChunkBytes: 32, maxChunkFrames: 8 },
    live: { notifier, fallbackIntervalMs: 60_000, validationIntervalMs: options.validationMs ?? 60_000,
      maxBufferedFrames: options.frames ?? 256, payloadWindowMs: options.windowMs ?? 10_000 }, onError: (error) => errors.push(error) })
  servers.push(value); return value
}
const request = (handler: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
async function open(handler: DurableStreamsServer, path: string, offset = 'now') {
  const start = watches.length, headStart = heads.length
  const response = await request(handler, `${path}?offset=${offset}&live=sse`)
  expect(response.status).toBe(200)
  const reader = response.body!.getReader()
  let content = ''
  const readUntil = async (match: string) => {
    while (!content.includes(match)) {
      const next = await reader.read()
      if (next.done) throw new Error(`SSE ended before ${match}`)
      content += new TextDecoder().decode(next.value)
    }
    return content
  }
  await readUntil('event: control')
  await Promise.all([...heads.slice(headStart).map((watch) => watch.ready), ...watches.slice(start).map((watch) => watch.ready)])
  return { reader, readUntil, content: () => content, close: () => reader.cancel(), watchIndex: start }
}
beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }) })
afterEach(async () => { beforeSubscribe = undefined; holdInvalidations = false; await Promise.all(servers.splice(0).map((value) => value.close())); vi.restoreAllMocks() })
afterAll(async () => {
  native.close(); objects.close(); await direct.query(`DROP SCHEMA "${schema}" CASCADE`); await pool.end(); await direct.end()
  expect(errors).toEqual([])
})

test('committed payload rows flow through Realtime without replaying SQL for every append', async () => {
  const handler = server()
  await request(handler, 'fast', 'PUT')
  const replay = vi.spyOn(PostgresStreamStore.prototype, 'readGeneration')
  const live = await open(handler, 'fast')
  try {
    for (let i = 0; i < 6; i++) await request(handler, 'fast', 'POST', `${i};`)
    const content = await live.readUntil('5;')
    for (let i = 0; i < 6; i++) expect(content).toContain(`${i};`)
    expect(replay.mock.calls.length).toBeLessThan(4)
    expect(watches[live.watchIndex]!.after).toBe('0000000000000000_0000000000000000')
  } finally { await live.close() }
}, 20_000)

test('an append committed between authorization/catch-up and raw subscription is replayed exactly once', async () => {
  const handler = server()
  await request(handler, 'setup-race', 'PUT')
  beforeSubscribe = async () => { expect((await request(handler, 'setup-race', 'POST', 'during-setup-')).status).toBe(204) }
  const live = await open(handler, 'setup-race')
  try {
    await request(handler, 'setup-race', 'POST', 'after-ready-')
    const content = await live.readUntil('after-ready-')
    expect(content.match(/during-setup-/g)).toHaveLength(1)
    expect(content.match(/after-ready-/g)).toHaveLength(1)
  } finally { await live.close() }
})

test('sealing or raw snapshot replacement recovers exact bytes through durable replay', async () => {
  const handler = server()
  await request(handler, 'sealed', 'PUT')
  const live = await open(handler, 'sealed')
  try {
    await request(handler, 'sealed', 'POST', 'one-')
    await request(handler, 'sealed', 'POST', 'two-')
    await request(handler, 'sealed', 'POST', 'three-')
    await handler.maintain({ maxChunks: 8, forceHot: true })
    watches[live.watchIndex]!.listener({ type: 'replace', rows: [], removed: [] })
    const content = await live.readUntil('three-')
    expect(content.match(/one-/g)).toHaveLength(1)
    expect(content.match(/two-/g)).toHaveLength(1)
    expect(content.match(/three-/g)).toHaveLength(1)
    expect(content.indexOf('one-')).toBeLessThan(content.indexOf('three-'))
  } finally { await live.close() }
})

test('buffer overflow and bounded window replacement recover from the durable cursor', async () => {
  const handler = server({ frames: 1, windowMs: 1500 })
  await request(handler, 'bounded', 'PUT')
  const live = await open(handler, 'bounded')
  try {
    const results = await Promise.all(['A-', 'B-', 'C-'].map((body) => request(handler, 'bounded', 'POST', body)))
    const tail = results.map((response) => response.headers.get('Stream-Next-Offset')!).sort().at(-1)!
    for (const part of ['A-', 'B-', 'C-']) await live.readUntil(part)
    await expect.poll(() => watches.slice(live.watchIndex + 1).some((watch) => watch.after === tail), { timeout: 5000, interval: 50 }).toBe(true)
  } finally { await live.close() }
})

test('fork replay catches up inherited data before forwarding only child appends', async () => {
  const handler = server()
  await request(handler, 'parent', 'PUT', 'inherited-')
  await handler.maintain({ forceHot: true })
  await request(handler, 'fork', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/parent' })
  const live = await open(handler, 'fork', '-1')
  try {
    await request(handler, 'parent', 'POST', 'parent-only-')
    await request(handler, 'fork', 'POST', 'child-only-')
    const content = await live.readUntil('child-only-')
    expect(content).toContain('inherited-'); expect(content).not.toContain('parent-only-')
  } finally { await live.close() }
})

test('JSON payload forwarding preserves large numbers and JSON append boundaries', async () => {
  const handler = server()
  await request(handler, 'json', 'PUT', '[]', { 'Content-Type': 'application/json' })
  const live = await open(handler, 'json')
  try {
    await request(handler, 'json', 'POST', '[9007199254740993,1e400,-0]', { 'Content-Type': 'application/json' })
    expect(await live.readUntil('9007199254740993')).toContain('[9007199254740993,1e400,-0]')
  } finally { await live.close() }
})

test('final close waits for its contiguous bytes and delete/recreate never switches UUID', async () => {
  const handler = server()
  await request(handler, 'close', 'PUT')
  const live = await open(handler, 'close')
  try {
    await request(handler, 'close', 'POST', 'final-', { 'Stream-Closed': 'true' })
    const content = await live.readUntil('"streamClosed":true')
    expect(content).toContain('data:final-')
    expect(content.indexOf('final-')).toBeLessThan(content.indexOf('"streamClosed":true'))
  } finally { await live.close() }
  await request(handler, 'replace', 'PUT')
  const old = await open(handler, 'replace')
  try {
    await request(handler, 'replace', 'DELETE')
    await request(handler, 'replace', 'PUT', 'replacement-private')
    let content = ''
    for (;;) { const next = await old.reader.read(); if (next.done) break; content += new TextDecoder().decode(next.value) }
    expect(content).not.toContain('replacement-private')
  } finally { await old.close() }
}, 25_000)

test('authentication expiry closes a raw payload session', async () => {
  const handler = server({ authMs: 5000 })
  await request(handler, 'auth', 'PUT')
  const live = await open(handler, 'auth')
  try {
    for (;;) { if ((await live.reader.read()).done) break }
    expect(live.content()).not.toContain('event: data')
  } finally { await live.close() }
}, 15_000)

test('a paused reader survives payload overflow and sealing before final closure', async () => {
  const handler = server({ frames: 2, windowMs: 60_000 })
  await request(handler, 'slow', 'PUT')
  const live = await open(handler, 'slow')
  try {
    const parts = Array.from({ length: 12 }, (_, i) => `record-${i.toString().padStart(2, '0')};`)
    // No reads while the response queue fills and the raw buffer overflows.
    for (const part of parts) expect((await request(handler, 'slow', 'POST', part)).status).toBe(204)
    await handler.maintain({ maxChunks: 16 })
    expect((await request(handler, 'slow', 'POST', undefined, { 'Stream-Closed': 'true' })).status).toBe(204)
    const content = await live.readUntil('"streamClosed":true')
    for (let i = 0; i < parts.length; i++) {
      expect(content.split(parts[i]!).length - 1).toBe(1)
      if (i) expect(content.indexOf(parts[i - 1]!)).toBeLessThan(content.indexOf(parts[i]!))
    }
  } finally { await live.close() }
}, 45_000) // Includes remote SQL setup/sealing; remains below the 60s fallback.

test.each(['ttl', 'absolute'])('%s expiry stops an idle payload session without extending its lifetime', async (mode) => {
  const handler = server()
  const path = `expiry-${mode}`
  // Leave time for bounded request/subscription setup before testing idle
  // expiry. Periodic validation and fallback remain at 60 seconds.
  const headers: Record<string, string> = mode === 'ttl' ? { 'Stream-TTL': '5' } : { 'Stream-Expires-At': new Date(Date.now() + 5000).toISOString() }
  expect((await request(handler, path, 'PUT', undefined, headers)).status).toBe(201)
  const live = await open(handler, path)
  try {
    for (;;) { if ((await live.reader.read()).done) break }
    expect((await request(handler, path, 'HEAD')).status).toBe(404)
  } finally { await live.close() }
}, 15_000)

test('serving-epoch validation fences a raw session before recovery can activate', async () => {
  const handler = server({ validationMs: 60_000, windowMs: 60_000 })
  await request(handler, 'epoch', 'PUT')
  const live = await open(handler, 'epoch')
  // Delay both notification and SQL validation across cutover. An initial
  // subscription reconciliation may still be scheduled when open() returns.
  let resumeReplay!: () => void
  const delayed = new Promise<void>((resolve) => { resumeReplay = resolve })
  const read = PostgresStreamStore.prototype.readGeneration
  vi.spyOn(PostgresStreamStore.prototype, 'readGeneration').mockImplementation(async function (this: PostgresStreamStore, ...args) {
    await delayed
    return read.apply(this, args)
  })
  holdInvalidations = true
  const identity = await inspectInstallation(pool, { schema })
  const { token } = await beginRecovery(direct, { schema, operationId: randomUUID(), expectedInstallationId: identity.id,
    expectedServingEpoch: identity.serving_epoch, environmentId: 'recovered-payload', storageIdentity: objects.identity })
  try {
    await verifyRecovery(pool, { schema, token, objects })
    await expect(finishRecovery(pool, { schema, token })).rejects.toThrow('Drain active live readers')
    // A reconnect/reset must validate the serving epoch before any more bytes.
    holdInvalidations = false
    resumeReplay()
    watches[live.watchIndex]!.listener({ type: 'invalidate' })
    for (;;) { if ((await live.reader.read()).done) break }
    await handler.close()
    await finishRecovery(pool, { schema, token })
    expect((await request(handler, 'epoch', 'POST', 'stale')).status).toBe(503)
    expect((await request(server({ environmentId: 'recovered-payload' }), 'epoch')).status).toBe(200)
  } finally { resumeReplay(); await live.close() }
}, 30_000)

test('a Realtime installation with the wrong serving epoch cannot supply payload rows', async () => {
  const installation = await inspectInstallation(pool, { schema })
  const handler = server(installation.environment_id ? { environmentId: installation.environment_id } : {})
  expect((await request(handler, 'wrong-epoch', 'PUT')).status).toBe(201)
  const stream = (await pool.query<{ id: string }>(`SELECT id FROM "${schema}".streams WHERE path = $1`, ['wrong-epoch'])).rows[0]
  expect(stream).toBeDefined()
  const rejected: unknown[] = [], updates: AppendUpdate[] = []
  const isolated = createNeonRealtimeNotifier({ url: env.NEON_REALTIME_URL!, secret: env.NEON_REALTIME_SECRET!, database: new URL(env.TEST_DATABASE_URL!).pathname.slice(1), onError: (error) => rejected.push(error) })
  const watch = isolated.watchAppends!({ schema, tenantId: 'payload', streamId: stream!.id,
    after: '0000000000000000_0000000000000000', installation: { ...installation, serving_epoch: randomUUID() } }, (update) => updates.push(update))
  try {
    await request(handler, 'wrong-epoch', 'POST', 'must-not-relay')
    await expect(watch.ready).rejects.toThrow('Realtime installation identity differs')
    expect(updates).toEqual([])
    expect(rejected.length).toBeGreaterThan(0)
  } finally { watch.close(); isolated.close() }
}, 30_000)

test('an interrupted SSE data event resumes from its last completed checkpoint', async () => {
  const installation = await inspectInstallation(pool, { schema })
  const value = server(installation.environment_id ? { environmentId: installation.environment_id } : {})
  const data = 'durable;'.repeat(32768)
  const created = await request(value, 'interrupted-control', 'PUT', data, { 'Stream-Closed': 'true' })
  expect(created.status).toBe(201)
  const checkpoint = created.headers.get('Stream-Next-Offset')!
  await value.maintain({ forceHot: true, maxChunks: 1 })
  const start = '0000000000000000_0000000000000000'
  const partial = await request(value, `interrupted-control?live=sse&offset=${start}`)
  expect(partial.status).toBe(200)
  const reader = partial.body!.getReader()
  let unfinished = Buffer.from((await reader.read()).value!).toString()
  unfinished += Buffer.from((await reader.read()).value!).toString()
  expect(unfinished).toContain('event: data')
  expect(unfinished).not.toContain('event: control')
  await reader.cancel()
  const replay = await request(value, `interrupted-control?live=sse&offset=${start}`)
  const complete = await replay.text()
  const events = complete.split('\n\n').filter(Boolean)
  expect(events).toHaveLength(2)
  expect(events[0]!.replace(/^event: data\ndata:/, '').replace(/\ndata:/g, '\n')).toBe(data)
  const control = JSON.parse(events[1]!.slice('event: control\ndata:'.length))
  expect(control.streamNextOffset).toBe(checkpoint)
  expect(control.streamClosed).toBe(true)
})
