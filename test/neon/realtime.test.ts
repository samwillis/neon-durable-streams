import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import type { StreamHead } from '../../src/neon.js'
import { migrate } from '../../src/migrations.js'
import { createDurableStreams } from '../../src/index.js'

const { TEST_DATABASE_URL, TEST_DATABASE_URL_UNPOOLED, NEON_REALTIME_URL, NEON_REALTIME_SECRET } = process.env
if (!TEST_DATABASE_URL || !TEST_DATABASE_URL_UNPOOLED || !NEON_REALTIME_URL || !NEON_REALTIME_SECRET) throw new Error('Real Neon Postgres and Realtime credentials are required; this test cannot be skipped')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 3, connectionTimeoutMillis: 15000 })
const migrationPool = new pg.Pool({ connectionString: TEST_DATABASE_URL_UNPOOLED, max: 1, connectionTimeoutMillis: 15000 })
pool.on('error', () => {})
migrationPool.on('error', () => {})
const errors: unknown[] = []
const notifier = createNeonRealtimeNotifier({ url: NEON_REALTIME_URL, secret: NEON_REALTIME_SECRET, database: new URL(TEST_DATABASE_URL).pathname.slice(1), onError: (error) => errors.push(error) })
const server = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema }, auth: {
  authenticate: () => ({ tenantId: 'realtime-test', subject: 'fixture' }), authorize: () => true,
} })
const request = (method: string, body?: string, headers?: Record<string, string>) => server.fetch(new Request('https://example.test/v1/stream/live', {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))

beforeAll(async () => {
  const version = await pool.query("SELECT current_setting('server_version_num')::integer AS version")
  expect(version.rows[0].version).toBeGreaterThanOrEqual(180000)
  expect(new URL(TEST_DATABASE_URL).hostname).toContain('.neon.tech')
  await migrate(migrationPool, { schema })
  expect((await request('PUT', 'before')).status).toBe(201)
})
afterAll(async () => {
  notifier.close()
  await server.close()
  await migrationPool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
  await pool.end()
  await migrationPool.end()
})

test('real Neon Realtime observes committed appends and closure across independent connections', async () => {
  const id = (await pool.query(`SELECT id FROM "${schema}".streams WHERE path = 'live'`)).rows[0].id as string
  let latest: StreamHead | null = null
  const watch = notifier.watch({ schema, tenantId: 'realtime-test', streamId: id }, (head) => { latest = head })
  const observe = (predicate: (head: StreamHead) => boolean) => {
    let timer: ReturnType<typeof setTimeout>
    let resolve: (head: StreamHead) => void
    const event = new Promise<StreamHead>((accept, reject) => {
      resolve = accept
      timer = setTimeout(() => reject(new Error('Realtime did not deliver the committed head')), 20_000)
    })
    const reference = notifier.watch({ schema, tenantId: 'realtime-test', streamId: id }, (head) => {
      if (head && predicate(head)) resolve(head)
    })
    return event.finally(() => { clearTimeout(timer); reference.close() })
  }
  try {
    await watch.ready
    const initial = latest!
    expect(initial.closed).toBe(false)
    const event = observe((head) => head.offset !== initial.offset)
    const response = await request('POST', ' after')
    expect(response.status).toBe(204)
    const appended = await event
    expect(appended.offset).toBe(response.headers.get('Stream-Next-Offset'))
    expect(await (await request('GET')).text()).toBe('before after')
    const closing = observe((head) => head.closed)
    expect((await request('POST', undefined, { 'Stream-Closed': 'true' })).status).toBe(204)
    const closed = await closing
    expect(closed.offset).toBe(appended.offset)
    expect(BigInt(closed.version)).toBeGreaterThan(BigInt(appended.version))
    expect(errors).toEqual([])
  } finally { watch.close() }
}, 45000)

test('a tenant-filtered live query does not expose another tenant head', async () => {
  const id = (await pool.query(`SELECT id FROM "${schema}".streams WHERE path = 'live'`)).rows[0].id as string
  const heads: unknown[] = []
  const watch = notifier.watch({ schema, tenantId: 'another-tenant', streamId: id }, (head) => heads.push(head))
  try { await watch.ready; expect(heads).toEqual([null]) } finally { watch.close() }
}, 45000)
