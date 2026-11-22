import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import type { HeadWatch } from '../../src/neon.js'

const { TEST_DATABASE_URL, TEST_DATABASE_URL_UNPOOLED, NEON_REALTIME_URL, NEON_REALTIME_SECRET } = process.env
if (!TEST_DATABASE_URL || !TEST_DATABASE_URL_UNPOOLED || !NEON_REALTIME_URL || !NEON_REALTIME_SECRET) throw new Error('Real Neon credentials required for live-read qualification')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 6 })
const direct = new pg.Pool({ connectionString: TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
direct.on('error', () => {})
const errors: unknown[] = []
const notifier = createNeonRealtimeNotifier({ url: NEON_REALTIME_URL, secret: NEON_REALTIME_SECRET, database: new URL(TEST_DATABASE_URL).pathname.slice(1), onError: (error) => errors.push(error) })
let registered: ((watch: HeadWatch) => void) | undefined
const handler = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema }, auth: {
  authenticate: () => ({ tenantId: 'live', subject: 'fixture' }), authorize: () => true,
}, live: {
  notifier: { watch(input, listener) { const watch = notifier.watch(input, listener); registered?.(watch); return watch } },
  // No timeout or polling can deliver the live appends in these tests.
  longPollTimeoutMs: 30_000, fallbackIntervalMs: 60_000, validationIntervalMs: 60_000,
}, onError: (error) => errors.push(error) })
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>, signal?: AbortSignal) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}), ...(signal ? { signal } : {}),
}))
const watching = () => new Promise<HeadWatch>((resolve) => { registered = resolve })

beforeAll(async () => { await migrate(direct, { schema }) })
afterAll(async () => {
  await handler.close()
  notifier.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
  expect(errors).toEqual([])
})

test('long-poll is woken by actual Realtime, including a final append with closure', async () => {
  const created = await request('longpoll', 'PUT', 'history')
  const ready = watching()
  const pending = request(`longpoll?live=long-poll&offset=${created.headers.get('Stream-Next-Offset')}`)
  const watch = await ready
  await watch.ready
  const append = await request('longpoll', 'POST', 'final', { 'Stream-Closed': 'true' })
  const response = await pending
  expect(response.status).toBe(200)
  expect(await response.text()).toBe('final')
  expect(response.headers.get('Stream-Next-Offset')).toBe(append.headers.get('Stream-Next-Offset'))
  expect(response.headers.get('Stream-Closed')).toBe('true')
}, 15_000)

test('SSE delivers final bytes before EOF and closes without polling', async () => {
  await request('sse', 'PUT', 'history')
  const ready = watching()
  const response = await request('sse?live=sse&offset=now')
  const watch = await ready
  await watch.ready
  const reader = response.body!.getReader()
  try {
    const initial = new TextDecoder().decode((await reader.read()).value)
    expect(initial).toContain('"upToDate":true')
    expect(initial).not.toContain('history')
    await request('sse', 'POST', 'final', { 'Stream-Closed': 'true' })
    let content = ''
    for (;;) {
      const chunk = await reader.read()
      if (chunk.done) break
      content += new TextDecoder().decode(chunk.value)
    }
    expect(content).toContain('event: data\ndata:final\n\nevent: control')
    expect(content).toContain('"streamClosed":true')
  } finally { await reader.cancel(); reader.releaseLock() }
}, 15_000)

test('delete and recreate never attaches an existing live reader to the new UUID', async () => {
  await request('generation', 'PUT')
  const ready = watching()
  const pending = request('generation?live=long-poll&offset=now')
  await (await ready).ready
  await request('generation', 'DELETE')
  await request('generation', 'PUT', 'private replacement')
  const response = await pending
  expect(response.status).toBe(404)
  expect(await response.text()).not.toContain('private replacement')
}, 15_000)

test('canceling an idle reader releases its subscription promptly', async () => {
  await request('cancel', 'PUT')
  const ready = watching()
  const controller = new AbortController()
  const pending = request('cancel?live=long-poll&offset=now', 'GET', undefined, undefined, controller.signal)
  await (await ready).ready
  controller.abort()
  expect((await pending).status).toBe(408)
}, 15_000)
