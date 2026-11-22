// Test-only consumer. Bundled twice against separately installed npm tarballs.
import { createHash, timingSafeEqual } from 'node:crypto'
import { attachDatabasePool } from '@neon/functions'
import pg from 'pg'
import { createDurableStreams } from '@samwillis/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@samwillis/durable-streams-neon/neon'
import { createS3ChunkStorage } from '@samwillis/durable-streams-neon/s3'
import { parseSubscriptionKeyring } from '@samwillis/durable-streams-neon/subscriptions'

const required = (name: string) => { const value = process.env[name]; if (!value) throw new Error(`Missing ${name}`); return value }
const schema = required('DS_SCHEMA')
if (!/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Rolling fixture requires an isolated test schema')
const digest = (value: string) => createHash('sha256').update(value).digest()
const admin = digest(required('DS_ADMIN_KEY'))
const keys = [{ key: digest(required('DS_USER_A_KEY')), tenantId: 'rolling-a' }, { key: digest(required('DS_USER_B_KEY')), tenantId: 'rolling-b' }]
const pool = new pg.Pool({ connectionString: required('DATABASE_URL'), max: 4, connectionTimeoutMillis: 10_000, query_timeout: 15_000 })
attachDatabasePool(pool)
const notifier = createNeonRealtimeNotifier({ url: required('DS_REALTIME_URL'), secret: required('DS_REALTIME_SECRET'),
  database: new URL(required('DATABASE_URL')).pathname.slice(1), onError: () => console.error('Rolling fixture Realtime degraded') })
const objects = createS3ChunkStorage({ endpoint: required('AWS_ENDPOINT_URL_S3'), region: required('AWS_REGION'), bucket: required('DS_BUCKET') })
const streams = createDurableStreams({ postgres: { pool, schema, environmentId: required('DS_ENVIRONMENT_ID') },
  storage: { objects, targetChunkBytes: 64, maxChunkFrames: 4 },
  subscriptions: { keyring: parseSubscriptionKeyring(required('DS_KEYRING_JSON')) },
  live: { notifier, maxReaders: 8, longPollTimeoutMs: 2500, maxSseDurationMs: 5000, heartbeatMs: 1000, fallbackIntervalMs: 60_000, validationIntervalMs: 60_000 },
  auth: { authenticate(request) {
    const token = request.headers.get('authorization')
    if (!token?.startsWith('Bearer ')) return null
    const match = keys.find((candidate) => timingSafeEqual(candidate.key, digest(token.slice(7))))
    return match ? { tenantId: match.tenantId, subject: 'rolling-fixture' } : null
  }, authorize: () => true }, onError: (error) => console.error('Rolling fixture failure', error instanceof Error ? error.name : 'unknown') })
process.once('SIGINT', () => {
  void streams.close().finally(async () => { notifier.close(); objects.close(); await pool.end() })
})
export default { async fetch(request: Request) {
  const path = new URL(request.url).pathname
  if (path === '/_health' || path === '/_maintain') {
    const key = request.headers.get('x-ds-admin-key')
    if (!key || !timingSafeEqual(admin, digest(key))) return new Response(null, { status: 401 })
    if (path === '/_maintain') {
      if (request.method !== 'POST') return new Response(null, { status: 405 })
      return Response.json(await streams.maintain({ maxChunks: 8, maxSubscriptionWork: 16 }))
    }
    const health = await streams.inspectHealth()
    return Response.json({ ...health, revision: required('DS_REVISION'), build: required('DS_BUILD_HASH'), instance: required('DS_INSTANCE'),
      runtimeNode: process.version, architecture: process.arch }, { status: health.ready ? 200 : 503 })
  }
  return streams.fetch(request)
} }
