import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import pg from 'pg'
import { afterAll, beforeAll } from 'vitest'
import { runConformanceTests } from '@durable-streams/server-conformance-tests'
import { createDurableStreams } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'

if (!process.env.TEST_DATABASE_URL) throw new Error('Set TEST_DATABASE_URL to disposable Postgres 18')
if (!process.env.NEON_REALTIME_URL || !process.env.NEON_REALTIME_SECRET) throw new Error('Full conformance requires real Neon Realtime credentials')
const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL, max: 10, connectionTimeoutMillis: 5000 })
const migrationPool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED ?? process.env.TEST_DATABASE_URL, max: 1, connectionTimeoutMillis: 15000 })
pool.on('error', () => {})
migrationPool.on('error', () => {})
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const notifier = createNeonRealtimeNotifier({ url: process.env.NEON_REALTIME_URL, secret: process.env.NEON_REALTIME_SECRET,
  database: new URL(process.env.TEST_DATABASE_URL).pathname.slice(1) })
const handler = createDurableStreams({ postgres: { pool, schema },
  live: { notifier, longPollTimeoutMs: 1000 },
  allowedOrigins: ['https://example.com'],
  // Explicit anonymous access is confined to the disposable test fixture.
  auth: { authenticate: () => ({ tenantId: 'conformance', subject: 'fixture' }), authorize: () => true },
})
const config = { baseUrl: '', subscriptions: true, longPollTimeoutMs: 1000 }
const http = createServer(async (incoming, outgoing) => {
  const controller = new AbortController()
  incoming.once('aborted', () => controller.abort())
  outgoing.once('close', () => { if (!outgoing.writableFinished) controller.abort() })
  try {
    const headers = new Headers()
    for (const [name, value] of Object.entries(incoming.headers)) if (value !== undefined) headers.set(name, Array.isArray(value) ? value.join(', ') : value)
    const init: RequestInit & { duplex?: 'half' } = { method: incoming.method ?? 'GET', headers, signal: controller.signal }
    if (!['GET', 'HEAD'].includes(init.method!)) {
      init.body = Readable.toWeb(incoming) as ReadableStream<Uint8Array>
      init.duplex = 'half'
    }
    const response = await handler.fetch(new Request(`${config.baseUrl}${incoming.url}`, init))
    outgoing.writeHead(response.status, Object.fromEntries(response.headers))
    if (response.body) {
      const reader = response.body.getReader()
      try {
        for (;;) {
          const { done, value } = await reader.read()
          if (done) break
          if (!outgoing.write(value)) await once(outgoing, 'drain', { signal: controller.signal })
        }
      } finally { await reader.cancel(); reader.releaseLock() }
    }
    outgoing.end()
  } catch {
    if (!outgoing.headersSent) outgoing.writeHead(500)
    outgoing.end()
  }
})

beforeAll(async () => {
  await migrate(migrationPool, { schema })
  http.listen(0, '127.0.0.1')
  await once(http, 'listening')
  const address = http.address()
  if (!address || typeof address === 'string') throw new Error('Missing HTTP test address')
  config.baseUrl = `http://127.0.0.1:${address.port}`
})

afterAll(async () => {
  http.closeAllConnections()
  if (http.listening) await new Promise<void>((resolve, reject) => http.close((error) => error ? reject(error) : resolve()))
  await handler.close()
  notifier.close()
  await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await migrationPool.end()
})

runConformanceTests(config)
