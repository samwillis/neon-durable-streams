import { createHash, randomUUID, timingSafeEqual } from 'node:crypto'
import { attachDatabasePool, waitUntil } from '@neon/functions'
import { isScheduleTriggerInvocation, parseTriggerDelivery } from '@neon/functions/triggers'
import pg from 'pg'
import { createDurableStreams } from '@samwillis/durable-streams-neon'
import type { Principal } from '@samwillis/durable-streams-neon'
import { createNeonRealtimeNotifier } from '@samwillis/durable-streams-neon/neon'
import { createS3ChunkStorage } from '@samwillis/durable-streams-neon/s3'
import { parseSubscriptionKeyring } from '@samwillis/durable-streams-neon/subscriptions'

function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`Missing Function configuration: ${name}`)
  return value
}
const digest = (text: string) => createHash('sha256').update(text).digest()
const keys = JSON.parse(required('DS_AUTH_JSON')) as unknown
if (!Array.isArray(keys) || !keys.length || keys.some((item) => !item || typeof item !== 'object'
  || typeof item.key !== 'string' || item.key.length < 32 || typeof item.tenantId !== 'string' || !item.tenantId
  || typeof item.subject !== 'string' || !item.subject)) throw new Error('Invalid DS_AUTH_JSON')
const identities: { digest: Buffer; principal: Principal }[] = keys.map((item) => ({ digest: digest(item.key), principal: { tenantId: item.tenantId, subject: item.subject } }))
function authenticate(request: Request): Principal | null {
  const authorization = request.headers.get('authorization')
  if (!authorization?.startsWith('Bearer ') || authorization.length > 1024) return null
  const supplied = digest(authorization.slice(7))
  return identities.find((identity) => timingSafeEqual(identity.digest, supplied))?.principal ?? null
}
const maintenanceKey = digest(required('DS_MAINTENANCE_KEY'))
function maintenanceAuthorized(request: Request): boolean {
  const key = request.headers.get('x-ds-maintenance-key')
  return !!key && key.length <= 1024 && timingSafeEqual(digest(key), maintenanceKey)
}
const origins: unknown = JSON.parse(process.env.DS_ALLOWED_ORIGINS_JSON ?? '[]')
if (!Array.isArray(origins) || origins.some((origin) => typeof origin !== 'string')) throw new Error('Invalid DS_ALLOWED_ORIGINS_JSON')
const pool = new pg.Pool({ connectionString: required('DATABASE_URL'), max: 5, connectionTimeoutMillis: 10_000, query_timeout: 15_000 })
attachDatabasePool(pool)
const instanceId = randomUUID()
const notifier = createNeonRealtimeNotifier({ url: required('DS_REALTIME_URL'), secret: required('DS_REALTIME_SECRET'),
  database: new URL(required('DATABASE_URL')).pathname.slice(1), onError: () => console.error('Realtime delivery degraded') })
const objects = createS3ChunkStorage({ endpoint: required('AWS_ENDPOINT_URL_S3'), region: required('AWS_REGION'), bucket: required('DS_BUCKET') })
const streams = createDurableStreams({
  postgres: { pool, schema: required('DS_SCHEMA'), environmentId: required('DS_ENVIRONMENT_ID') },
  auth: { authenticate, authorize: () => true },
  allowedOrigins: origins,
  ...(process.env.DS_PUBLIC_BASE_URL ? { publicBaseUrl: process.env.DS_PUBLIC_BASE_URL } : {}),
  storage: { objects },
  subscriptions: { keyring: parseSubscriptionKeyring(required('DS_KEYRING_JSON')) },
  live: { notifier, longPollTimeoutMs: 25_000, fallbackIntervalMs: Number(process.env.DS_LIVE_FALLBACK_MS ?? '5000'),
    validationIntervalMs: Number(process.env.DS_LIVE_VALIDATION_MS ?? '1000') },
  onError: (error) => console.error('Durable Streams request failed', error instanceof Error ? error.name : 'unknown'),
})
const hintMode = process.env.DS_MAINTENANCE_HINTS ?? 'on'
if (!['on', 'off'].includes(hintMode)) throw new Error('DS_MAINTENANCE_HINTS must be on or off')
const batch = (name: string, fallback: number) => {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isSafeInteger(value) || value < 1 || value > 128) throw new Error(`Invalid ${name}`)
  return value
}
const cronChunks = batch('DS_CRON_MAX_CHUNKS', 64)
const cronSubscriptions = batch('DS_CRON_SUBSCRIPTION_WORK', 64)
let hintWork: Promise<void> | undefined
let hintRequested = false
function hintMaintenance(): void {
  hintRequested = true
  if (hintWork) return
  hintWork = (async () => {
    for (let pass = 0; pass < 2; pass++) {
      hintRequested = false
      await streams.maintain({ maxChunks: 2, maxSubscriptionWork: 16, freezeHot: false, timeBudgetMs: 1000 })
      if (!hintRequested) break
    }
  })().catch(() => console.error('Opportunistic maintenance failed; scheduled recovery remains pending'))
    .finally(() => { hintWork = undefined })
  // Only the invocation starting this coalesced pass retains it. Attaching the
  // same promise to every append would consume unnecessary invocation slots.
  waitUntil(hintWork)
}

process.once('SIGINT', () => {
  void streams.close().finally(async () => { notifier.close(); objects.close(); await pool.end() })
})
export default {
  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname
    if (path === '/_trigger') {
      // Neon strips client-supplied x-neon-* headers at deployed ingress.
      // Keep this platform-authenticated route outside application API-key auth.
      if (request.method !== 'POST' || !request.headers.has('x-neon-trigger-invocation-id')) return new Response(null, { status: 401 })
      const parsed = await parseTriggerDelivery(request)
      if (!parsed.ok || !isScheduleTriggerInvocation(parsed.invocation) || parsed.invocation.trigger.name !== required('DS_MAINTENANCE_TRIGGER_NAME')) return new Response(null, { status: 401 })
      return Response.json(await streams.maintain({ maxChunks: cronChunks, maxSubscriptionWork: cronSubscriptions, timeBudgetMs: 40_000 }))
    }
    if (path === '/_maintenance') {
      if (request.method !== 'POST' || !maintenanceAuthorized(request)) return new Response(null, { status: 401 })
      return Response.json(await streams.maintain({ maxChunks: 8, timeBudgetMs: 10_000 }))
    }
    if (path === '/health') {
      if (!authenticate(request)) return new Response(null, { status: 401 })
      const health = await streams.inspectHealth()
      return Response.json({ ...health, build: required('DS_BUILD_HASH'), deploymentId: process.env.DS_DEPLOYMENT_ID ?? required('DS_BUILD_HASH'),
        ...(maintenanceAuthorized(request) ? { runtime: { instanceId, processUptimeSeconds: process.uptime(), memory: process.memoryUsage(),
          pool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount } } } : {}) },
        { status: health.ready ? 200 : 503, headers: { 'Cache-Control': 'no-store' } })
    }
    const response = await streams.fetch(request)
    if (hintMode === 'on' && response.ok && ['PUT', 'POST', 'DELETE'].includes(request.method)) hintMaintenance()
    return response
  },
}
