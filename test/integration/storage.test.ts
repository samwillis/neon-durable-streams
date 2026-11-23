import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomUUID } from 'node:crypto'
import { fork } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'
import pg from 'pg'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { DurableStreamsServer } from '../../src/index.js'
import { MIGRATION_IDS, migrate } from '../../src/migrations.js'
import { transaction } from '../../src/transaction.js'

const database = process.env.TEST_DATABASE_URL
if (!database) throw new Error('Set TEST_DATABASE_URL to a disposable Postgres 18 database; see .env.example')
const pool = new pg.Pool({ connectionString: database, max: 8, connectionTimeoutMillis: 120_000 })
const migrationPool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED ?? database, max: 6, connectionTimeoutMillis: 15000 })
pool.on('error', () => {})
migrationPool.on('error', () => {})
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const errors: unknown[] = []
const config = { postgres: { ...postgresTestLimits, pool, schema }, auth: {
  authenticate(request: Request) {
    const identities = new Map([['Bearer a', 'tenant-a'], ['Bearer b', 'tenant-b']])
    const tenantId = identities.get(request.headers.get('authorization') ?? '')
    return tenantId ? { tenantId, subject: 'test-user' } : null
  }, authorize: () => true,
}, onError: (error: unknown) => errors.push(error) }
let server: DurableStreamsServer

async function request(path: string, method = 'GET', body?: string | Uint8Array, headers: Record<string, string> = {}) {
  const requestHeaders = new Headers({ Authorization: 'Bearer a', ...headers })
  if ((body !== undefined || method === 'PUT') && !requestHeaders.has('content-type')) requestHeaders.set('content-type', 'text/plain')
  const init: RequestInit = { method, headers: requestHeaders }
  if (body !== undefined) init.body = typeof body === 'string' ? body : new Uint8Array(body)
  return server.fetch(new Request(`https://example.test/v1/stream/${path}`, init))
}

async function readAll(path: string) {
  let offset = '-1'
  const parts: string[] = []
  for (let i = 0; i < 100; i++) {
    const response = await request(`${path}?offset=${offset}`)
    expect(response.status).toBe(200)
    parts.push(await response.text())
    if (response.headers.get('stream-up-to-date') === 'true') return parts.join('')
    offset = response.headers.get('stream-next-offset')!
  }
  throw new Error('Replay failed to converge')
}

beforeAll(async () => {
  const version = await pool.query<{ version: string }>("SELECT current_setting('server_version_num') AS version")
  expect(Number(version.rows[0]!.version)).toBeGreaterThanOrEqual(180000)
  expect(await migrate(migrationPool, { schema })).toEqual([...MIGRATION_IDS])
  expect(await migrate(migrationPool, { schema })).toEqual([])
  server = createDurableStreams(config)
})

afterAll(async () => {
  await server?.close()
  if (!schema.startsWith('ds_test_')) throw new Error('Refusing to drop a non-test schema')
  await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await migrationPool.end()
  expect(errors).toEqual([])
})

describe('transaction and migration guarantees', () => {
  test('concurrent migrators initialize an installation once', async () => {
    const other = `${schema}_migration`
    try {
      const outcomes = await Promise.all(Array.from({ length: 6 }, () => migrate(migrationPool, { schema: other })))
      expect(outcomes.filter((value) => value.length)).toHaveLength(1)
      expect((await pool.query(`SELECT count(*) FROM "${other}".installation`)).rows[0].count).toBe('1')
    } finally { await pool.query(`DROP SCHEMA IF EXISTS "${other}" CASCADE`) }
  })
  test('local durability and timeout settings override inherited sessions and revert', async () => {
    const one = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED ?? database!, max: 1 })
    try {
      await one.query("SET synchronous_commit = off; SET statement_timeout = 0")
      await transaction(one, {}, async (client) => {
        const result = await client.query<{ synchronous_commit: string; statement_timeout: string }>('SHOW synchronous_commit; SHOW statement_timeout')
        const results = result as unknown as pg.QueryResult[]
        expect(results[0]!.rows[0]).toEqual({ synchronous_commit: 'on' })
        expect(results[1]!.rows[0]).toEqual({ statement_timeout: '10s' })
      })
      expect((await one.query('SHOW synchronous_commit')).rows[0]).toEqual({ synchronous_commit: 'off' })
    } finally { await one.end() }
  })
  test('failed work rolls back the entire transaction', async () => {
    await expect(transaction(pool, {}, async (client) => {
      await client.query(`INSERT INTO "${schema}".paths (tenant_id, path) VALUES ('test', 'rolled-back')`)
      throw new Error('simulated interruption')
    })).rejects.toThrow('simulated interruption')
    expect((await pool.query(`SELECT 1 FROM "${schema}".paths WHERE path = 'rolled-back'`)).rowCount).toBe(0)
  })
  test('unknown migration history blocks migration and preserves existing data', async () => {
    await pool.query(`INSERT INTO "${schema}".migrations (id, checksum) VALUES ('9999_future', 'unknown')`)
    await expect(migrate(pool, { schema })).rejects.toThrow('Unknown or modified')
    await pool.query(`DELETE FROM "${schema}".migrations WHERE id = '9999_future'`)
  })
  test('an existing installation identity cannot be silently regenerated', async () => {
    const prior = (await pool.query(`SELECT id FROM "${schema}".installation`)).rows[0].id as string
    await pool.query(`DELETE FROM "${schema}".installation`)
    try { await expect(migrate(pool, { schema })).rejects.toThrow('explicit recovery') }
    finally { await pool.query(`INSERT INTO "${schema}".installation (id, storage_version) VALUES ($1, 1)`, [prior]) }
  })
})

describe('durable HTTP hot log', () => {
  test('concurrent creates establish exactly one stream identity', async () => {
    const responses = await Promise.all(Array.from({ length: 24 }, () => request('create-race', 'PUT')))
    expect(responses.filter((r) => r.status === 201)).toHaveLength(1)
    expect(responses.filter((r) => r.status === 200)).toHaveLength(23)
    expect((await pool.query(`SELECT count(*) FROM "${schema}".streams WHERE path = 'create-race'`)).rows[0].count).toBe('1')
  })
  test('concurrent duplicate producer retries append exactly once and create one change', async () => {
    await request('dedup', 'PUT')
    const headers = { 'Producer-Id': 'writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
    const responses = await Promise.all(Array.from({ length: 24 }, () => request('dedup', 'POST', 'once', headers)))
    expect(responses.filter((r) => r.status === 200)).toHaveLength(1)
    expect(responses.filter((r) => r.status === 204)).toHaveLength(23)
    expect(await readAll('dedup')).toBe('once')
    const outbox = await pool.query(`SELECT c.* FROM "${schema}".stream_changes c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = 'dedup' ORDER BY version`)
    expect(outbox.rows.map((r) => r.kind)).toEqual(['created', 'appended'])
  })
  test('sequence gaps and stale epochs do not mutate data', async () => {
    await request('fencing', 'PUT')
    const base = { 'Producer-Id': 'writer', 'Producer-Epoch': '2', 'Producer-Seq': '0' }
    expect((await request('fencing', 'POST', 'a', base)).status).toBe(200)
    const gap = await request('fencing', 'POST', 'bad', { ...base, 'Producer-Seq': '2' })
    expect(gap.status).toBe(409)
    expect(gap.headers.get('producer-expected-seq')).toBe('1')
    expect((await request('fencing', 'POST', 'bad', { ...base, 'Producer-Epoch': '1' })).status).toBe(403)
    expect(await readAll('fencing')).toBe('a')
  })
  test('JSON failure does not advance producer state and numbers retain precision', async () => {
    await request('json', 'PUT', '[]', { 'Content-Type': 'application/json' })
    const headers = { 'Content-Type': 'application/json', 'Producer-Id': 'json-writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
    expect((await request('json', 'POST', '{bad', headers)).status).toBe(400)
    expect((await request('json', 'POST', '[9007199254740993,[1,2]]', headers)).status).toBe(200)
    expect(await readAll('json')).toBe('[9007199254740993,[1,2]]')
  })
  test('replay survives a new pool and server instance', async () => {
    await request('restart', 'PUT', 'persisted')
    const freshPool = new pg.Pool({ connectionString: database! })
    const fresh = createDurableStreams({ ...config, postgres: { ...postgresTestLimits, pool: freshPool, schema } })
    try {
      const response = await fresh.fetch(new Request('https://example.test/v1/stream/restart', { headers: { Authorization: 'Bearer a' } }))
      expect(response.status).toBe(200)
      expect(await response.text()).toBe('persisted')
      await fresh.close()
      expect((await freshPool.query('SELECT 1 AS alive')).rows[0].alive).toBe(1)
    } finally { await fresh.close(); await freshPool.end() }
  })
  test.each(['committed', 'uncommitted'])('SIGKILL of a writer preserves the %s transaction outcome', async (mode) => {
    const child = fork(fileURLToPath(new URL('../fixtures/crash-worker.mjs', import.meta.url)), [mode], {
      execArgv: [], env: { ...process.env, TEST_SCHEMA: schema }, stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    })
    let stderr = ''
    child.stderr?.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
    try {
      const message = await Promise.race([
        once(child, 'message').then(([value]) => value as { ready: boolean }),
        once(child, 'exit').then(() => { throw new Error(`Writer exited before test barrier: ${stderr}`) }),
      ])
      expect(message.ready).toBe(true)
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = once(child, 'exit')
        child.kill('SIGKILL')
        await exited
      }
    }
    if (mode === 'committed') expect(await readAll('crash-committed')).toBe('acknowledged bytes')
    else {
      expect((await pool.query(`SELECT 1 FROM "${schema}".paths WHERE path = 'crash-uncommitted'`)).rowCount).toBe(0)
      expect((await request('crash-uncommitted', 'PUT', 'new transaction')).status).toBe(201)
    }
  })
  test('pages across complete frames and resumes without gaps', async () => {
    await request('pages', 'PUT')
    const frames = Array.from({ length: 40 }, (_, i) => `frame-${i}|`)
    for (const frame of frames) expect((await request('pages', 'POST', frame)).status).toBe(204)
    expect(await readAll('pages')).toBe(frames.join(''))
  })
  test('closing changes the ETag and deduplicates its producer tuple', async () => {
    await request('closed', 'PUT', 'start')
    const before = await request('closed')
    const headers = { 'Producer-Id': 'closer', 'Producer-Epoch': '0', 'Producer-Seq': '0', 'Stream-Closed': 'true' }
    expect((await request('closed', 'POST', 'end', headers)).status).toBe(200)
    expect((await request('closed', 'POST', 'different body', headers)).status).toBe(204)
    const closed = await request('closed', 'GET', undefined, { 'If-None-Match': before.headers.get('etag')! })
    expect(closed.status).toBe(200)
    expect(closed.headers.get('stream-closed')).toBe('true')
    expect(await closed.text()).toBe('startend')
    expect((await request('closed', 'GET', undefined, { 'If-None-Match': closed.headers.get('etag')! })).status).toBe(304)
    expect((await request('closed', 'POST', 'rejected')).status).toBe(409)
  })
  test('delete/recreate isolates producer state, bytes, ETags, and identities', async () => {
    await request('recreate', 'PUT', 'old')
    const old = await request('recreate')
    expect((await request('recreate', 'DELETE')).status).toBe(204)
    expect((await request('recreate', 'PUT', 'new')).status).toBe(201)
    const fresh = await request('recreate')
    expect(await fresh.text()).toBe('new')
    expect(fresh.headers.get('etag')).not.toBe(old.headers.get('etag'))
    expect((await pool.query(`SELECT count(*) FROM "${schema}".streams WHERE path = 'recreate'`)).rows[0].count).toBe('2')
  })
  test('expiry commits its lifecycle change; HEAD never touches access time', async () => {
    await request('ttl', 'PUT', undefined, { 'Stream-TTL': '60' })
    const initial = await pool.query(`SELECT accessed_at FROM "${schema}".streams WHERE path = 'ttl'`)
    expect((await request('ttl', 'HEAD')).status).toBe(200)
    expect((await pool.query(`SELECT accessed_at FROM "${schema}".streams WHERE path = 'ttl'`)).rows[0]).toEqual(initial.rows[0])
    await pool.query(`UPDATE "${schema}".streams SET accessed_at = clock_timestamp() - interval '61 seconds' WHERE path = 'ttl'`)
    expect((await request('ttl')).status).toBe(404)
    expect((await pool.query(`SELECT active_stream_id FROM "${schema}".paths WHERE path = 'ttl'`)).rows[0].active_stream_id).toBeNull()
    expect((await request('ttl', 'PUT')).status).toBe(201)
  })
  test('tenant identity isolates a shared URL and authentication precedes body parsing', async () => {
    await request('private', 'PUT', 'a')
    expect((await request('private', 'GET', undefined, { Authorization: 'Bearer b' })).status).toBe(404)
    expect((await request('private', 'POST', 'x', { Authorization: 'bad', 'Content-Length': '999999999' })).status).toBe(401)
  })
  test('unsupported reserved features fail explicitly and never create streams', async () => {
    expect((await request('__ds/subscriptions/sub', 'PUT', '{}')).status).toBe(501)
    expect((await request('fork', 'PUT', undefined, { 'Stream-Fork-Sub-Offset': '0' })).status).toBe(400)
    expect((await request('private?offset=-1&live=sse')).status).toBe(503)
    expect((await request('fork', 'HEAD')).status).toBe(404)
  })
  test('unknown methods cannot fall through to reads and invalid calendar expiry is rejected', async () => {
    const unknown = await server.fetch(new Request('https://example.test/v1/stream/private', { method: 'constructor', headers: { Authorization: 'Bearer a' } }))
    expect(unknown.status).toBe(405)
    expect((await request('invalid-date', 'PUT', undefined, { 'Stream-Expires-At': '2027-02-30T00:00:00Z' })).status).toBe(400)
    expect((await request('invalid-date', 'HEAD')).status).toBe(404)
  })
  test('streamed bodies are byte-limited even when Content-Length understates their size', async () => {
    const limited = createDurableStreams({ ...config, limits: { maxAppendBytes: 16 } })
    try {
      const body = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new Uint8Array(32)); controller.close() } })
      const init: RequestInit & { duplex: 'half' } = { method: 'PUT', headers: { Authorization: 'Bearer a', 'Content-Length': '1' }, body, duplex: 'half' }
      expect((await limited.fetch(new Request('https://example.test/v1/stream/oversized', init))).status).toBe(413)
      expect((await request('oversized', 'HEAD')).status).toBe(404)
    } finally { await limited.close() }
  })
  test('a stalled body is canceled and never creates a stream', async () => {
    let canceled = false
    const limited = createDurableStreams({ ...config, limits: { bodyTimeoutMs: 10 } })
    try {
      const body = new ReadableStream<Uint8Array>({ cancel() { canceled = true } })
      const init: RequestInit & { duplex: 'half' } = { method: 'PUT', headers: { Authorization: 'Bearer a' }, body, duplex: 'half' }
      expect((await limited.fetch(new Request('https://example.test/v1/stream/stalled', init))).status).toBe(408)
      expect(canceled).toBe(true)
      expect((await request('stalled', 'HEAD')).status).toBe(404)
    } finally { await limited.close() }
  })
})
