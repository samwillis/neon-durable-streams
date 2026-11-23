import { observe } from './observer.js'
import type { Pool } from 'pg'
import { StreamError } from './errors.js'
import { normalizeBasePath, streamPath } from './path.js'
import { PostgresStreamStore } from './store.js'
import type { Producer, StreamMetadata } from './store.js'
import { LiveReads } from './live.js'
import { ownedResponseBody } from './response-body.js'
import type { LiveOptions } from './live.js'
import type { StorageOptions } from './storage.js'
import { PostgresSubscriptions } from './subscription-store.js'
import { subscriptionRoute } from './subscription-protocol.js'
import type { SubscriptionConfig } from './subscription-protocol.js'
import type { SubscriptionOptions } from './subscriptions.js'

export type Operation = 'create' | 'append' | 'read' | 'head' | 'delete' | 'subscribe' | 'claim'
export interface Principal {
  readonly tenantId: string
  readonly subject: string
  /** Unix milliseconds. Live modes will require explicit renewal semantics. */
  readonly expiresAt?: number
}
export interface AuthorizationContext {
  readonly principal: Principal
  readonly operation: Operation
  readonly path: string
  readonly request: Request
  readonly selector?: { readonly pattern?: string; readonly streams: readonly string[] }
  /** Normalized configuration, including delivery destinations, for subscribe. */
  readonly subscription?: SubscriptionConfig
}
export interface ServerOptions {
  readonly postgres: { readonly pool: Pool; readonly schema?: string; readonly environmentId?: string
    readonly transactionTimeoutMs?: number; readonly lockTimeoutMs?: number }
  readonly auth: {
    readonly authenticate: (request: Request) => Principal | null | Promise<Principal | null>
    readonly authorize: (context: AuthorizationContext) => boolean | Promise<boolean>
  }
  readonly basePath?: string
  readonly publicBaseUrl?: string
  readonly allowedOrigins?: readonly string[]
  readonly live?: LiveOptions
  readonly storage?: StorageOptions
  readonly subscriptions?: SubscriptionOptions
  readonly limits?: { readonly maxAppendBytes?: number; readonly maxStoredBytes?: number; readonly maxPageBytes?: number; readonly maxPageFrames?: number; readonly bodyTimeoutMs?: number; readonly replayTimeoutMs?: number
    readonly maxReplayMemoryBytes?: number; readonly responseTimeoutMs?: number }
  readonly onError?: (error: unknown) => void
  /** Contains no bodies, credentials, paths, or subscription tokens. */
  readonly onEvent?: (event: ServerEvent) => void
}
export interface MaintenanceResult { readonly frozen: number; readonly sealed: number; readonly retired: number; readonly changes: number; readonly deliveries: number; readonly links: number
  readonly reclaimedChunks: number; readonly deletedObjects: number; readonly prunedRows: number; readonly inventoriedObjects: number }
export interface MaintenanceOptions {
  readonly maxChunks?: number
  readonly maxSubscriptionWork?: number
  /** Finish the current bounded round, then stop claiming work at this budget. */
  readonly timeBudgetMs?: number
  /** Administrative drain only. Normal maintenance seals size/count/age-due chunks. */
  readonly forceHot?: boolean
}
export type ServerEvent =
  | { readonly type: 'request'; readonly method: string; readonly status: number; readonly durationMs: number }
  | { readonly type: 'maintenance'; readonly success: boolean; readonly durationMs: number; readonly result?: MaintenanceResult }
export interface DurableStreamsServer {
  readonly fetch: (request: Request) => Promise<Response>
  readonly inspectHealth: () => Promise<{ ready: boolean; mode: 'hot-log' | 'tiered'; features: readonly string[] }>
  readonly maintain: (options?: MaintenanceOptions) => Promise<MaintenanceResult>
  readonly close: (options?: { readonly deadlineMs?: number }) => Promise<void>
}

const METHODS = new Map<string, Operation>([['PUT', 'create'], ['POST', 'append'], ['GET', 'read'], ['HEAD', 'head'], ['DELETE', 'delete']])
const EXPOSED = 'Stream-Next-Offset, Stream-Up-To-Date, Stream-Closed, Stream-Cursor, Stream-SSE-Data-Encoding, Stream-TTL, Stream-Expires-At, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, ETag, Location'

async function readBody(request: Request, limit: number, timeoutMs: number): Promise<Buffer> {
  const declared = request.headers.get('content-length')
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > limit)) throw new StreamError(413, 'PAYLOAD_TOO_LARGE', 'Body exceeds configured limit')
  if (request.signal.aborted) throw new StreamError(408, 'REQUEST_ABORTED', 'Request aborted')
  if (!request.body) return Buffer.alloc(0)
  const reader = request.body.getReader()
  let timedOut = false
  const cancel = () => { void reader.cancel().catch(() => {}) }
  const timer = setTimeout(() => { timedOut = true; cancel() }, timeoutMs)
  request.signal.addEventListener('abort', cancel, { once: true })
  const chunks: Uint8Array[] = []
  let size = 0
  try {
    for (;;) {
      const result = await reader.read()
      if (timedOut || request.signal.aborted) throw new StreamError(408, 'REQUEST_ABORTED', 'Body timed out or was aborted')
      if (result.done) break
      size += result.value.byteLength
      if (size > limit) { cancel(); throw new StreamError(413, 'PAYLOAD_TOO_LARGE', 'Body exceeds configured limit') }
      chunks.push(result.value)
    }
    return Buffer.concat(chunks, size)
  } finally {
    clearTimeout(timer)
    request.signal.removeEventListener('abort', cancel)
    reader.releaseLock()
  }
}

function producerHeaders(headers: Headers): Producer | undefined {
  const id = headers.get('producer-id')
  const epoch = headers.get('producer-epoch')
  const seq = headers.get('producer-seq')
  if (id === null && epoch === null && seq === null) return undefined
  if (!id || id.length > 512 || epoch === null || seq === null || !/^\d+$/.test(epoch) || !/^\d+$/.test(seq)
    || !Number.isSafeInteger(Number(epoch)) || !Number.isSafeInteger(Number(seq))) {
    throw new StreamError(400, 'INVALID_PRODUCER', 'Provide a producer ID and nonnegative safe integer epoch and sequence together')
  }
  return { id, epoch: Number(epoch), seq: Number(seq) }
}

function contentType(request: Request): string {
  const value = request.headers.get('content-type') ?? 'application/octet-stream'
  if (!/^[\w!#$%&'*+.^`|~-]+\/[\w!#$%&'*+.^`|~-]+(?:\s*;[^\r\n]*)?$/.test(value)) throw new StreamError(400, 'INVALID_CONTENT_TYPE', 'Invalid Content-Type')
  return value
}

function createExpiry(headers: Headers): { ttlSeconds?: number; expiresAt?: Date } {
  const ttl = headers.get('stream-ttl')
  const expires = headers.get('stream-expires-at')
  if (ttl !== null && expires !== null) throw new StreamError(400, 'CONFLICTING_EXPIRY', 'Choose TTL or absolute expiry')
  if (ttl !== null) {
    if (!/^(0|[1-9]\d*)$/.test(ttl) || Number(ttl) > 2_147_483_647) throw new StreamError(400, 'INVALID_TTL', 'Invalid Stream-TTL')
    return { ttlSeconds: Number(ttl) }
  }
  if (expires !== null) {
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i.exec(expires)
    if (!parts) throw new StreamError(400, 'INVALID_EXPIRY', 'Expected an RFC3339 timestamp')
    const year = Number(parts[1]), month = Number(parts[2]), day = Number(parts[3])
    const days = [31, year % 400 === 0 || (year % 4 === 0 && year % 100 !== 0) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if (month < 1 || month > 12 || day < 1 || day > days[month - 1]! || Number(parts[4]) > 23 || Number(parts[5]) > 59 || Number(parts[6]) > 59) throw new StreamError(400, 'INVALID_EXPIRY', 'Invalid calendar timestamp')
    const date = new Date(expires)
    if (!Number.isFinite(date.getTime())) throw new StreamError(400, 'INVALID_EXPIRY', 'Invalid expiry timestamp')
    return { expiresAt: date }
  }
  return {}
}

function metadataHeaders(stream: StreamMetadata): Headers {
  const headers = new Headers({ 'Content-Type': stream.content_type, 'Stream-Next-Offset': stream.tail_offset, 'Cache-Control': 'no-store' })
  if (stream.closed) headers.set('Stream-Closed', 'true')
  if (stream.ttl_seconds !== null) headers.set('Stream-TTL', String(stream.ttl_seconds))
  if (stream.expires_at) headers.set('Stream-Expires-At', stream.expires_at.toISOString())
  return headers
}

export function createDurableStreams(options: ServerOptions): DurableStreamsServer {
  if (typeof options.auth?.authenticate !== 'function' || typeof options.auth.authorize !== 'function') throw new TypeError('An explicit authentication and authorization policy is required')
  const basePath = normalizeBasePath(options.basePath ?? '/v1/stream')
  const maxAppendBytes = options.limits?.maxAppendBytes ?? 1024 * 1024
  const bodyTimeoutMs = options.limits?.bodyTimeoutMs ?? 30_000
  const responseTimeoutMs = options.limits?.responseTimeoutMs ?? 300_000
  if (!Number.isSafeInteger(maxAppendBytes) || maxAppendBytes < 1 || maxAppendBytes > 16 * 1024 * 1024
    || !Number.isSafeInteger(bodyTimeoutMs) || bodyTimeoutMs < 1 || bodyTimeoutMs > 300_000
    || !Number.isSafeInteger(responseTimeoutMs) || responseTimeoutMs < 1 || responseTimeoutMs > 300_000) throw new RangeError('Invalid request limits')
  const canonical = options.publicBaseUrl ? new URL(options.publicBaseUrl) : undefined
  if (canonical && (!['https:', 'http:'].includes(canonical.protocol) || canonical.username || canonical.password || canonical.search || canonical.hash || canonical.pathname !== basePath)) throw new TypeError('publicBaseUrl must match basePath without credentials/query/fragment')
  const store = new PostgresStreamStore(options.postgres.pool, options.postgres.schema ?? 'durable_streams', {
    maxStoredBytes: options.limits?.maxStoredBytes, maxPageBytes: options.limits?.maxPageBytes, maxPageFrames: options.limits?.maxPageFrames,
    replayTimeoutMs: options.limits?.replayTimeoutMs,
    maxReplayMemoryBytes: options.limits?.maxReplayMemoryBytes, consumesChanges: !!options.subscriptions,
    storage: options.storage, environmentId: options.postgres.environmentId,
    timeouts: { ...(options.postgres.transactionTimeoutMs !== undefined ? { timeoutMs: options.postgres.transactionTimeoutMs } : {}),
      ...(options.postgres.lockTimeoutMs !== undefined ? { lockTimeoutMs: options.postgres.lockTimeoutMs } : {}) },
  })
  const report = (error: unknown) => observe(options.onError, error)
  const live = options.live ? new LiveReads(store, options.postgres.schema ?? 'durable_streams', options.live, report) : undefined
  const subscriptions = options.subscriptions ? new PostgresSubscriptions(options.postgres.pool, options.postgres.schema ?? 'durable_streams', store, options.subscriptions, report) : undefined
  const pending = new Set<Promise<unknown>>()
  const shutdown = new AbortController()
  let closed = false

  async function handle(request: Request): Promise<Response> {
    const common = new Headers({ 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Cross-Origin-Resource-Policy': 'cross-origin' })
    const origin = request.headers.get('origin')
    if (origin && options.allowedOrigins?.includes(origin)) {
      common.set('Access-Control-Allow-Origin', origin)
      common.set('Vary', 'Origin')
      common.set('Access-Control-Expose-Headers', EXPOSED)
    }
    const respond = (body: BodyInit | null, status: number, headers?: HeadersInit) => {
      const merged = new Headers(common)
      new Headers(headers).forEach((v, k) => merged.set(k, v))
      return new Response(request.method === 'HEAD' ? null : body, { status, headers: merged })
    }
    try {
      if (closed) throw new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down')
      if (origin && !options.allowedOrigins?.includes(origin)) throw new StreamError(403, 'ORIGIN_REJECTED', 'Origin is not allowed')
      const url = new URL(request.url)
      const control = subscriptionRoute(url, basePath)
      const path = control?.path ?? streamPath(url, basePath)
      if (path === null) throw new StreamError(404, 'ROUTE_NOT_FOUND', 'Route not found')
      if (request.method === 'OPTIONS') return respond(null, 204, { 'Access-Control-Allow-Methods': 'PUT, POST, GET, HEAD, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset, Producer-Id, Producer-Epoch, Producer-Seq' })
      const operation = METHODS.get(request.method)
      if (!operation) return respond(null, 405, { Allow: 'PUT, POST, GET, HEAD, DELETE, OPTIONS' })
      if (control) {
        if (!subscriptions) throw new StreamError(501, 'SUBSCRIPTIONS_NOT_CONFIGURED', 'Configure subscription support for the control API')
        let response: Response | undefined
        if (control.action === 'jwks' && request.method === 'GET') response = await subscriptions.jwks()
        else if (['callback', 'ack', 'release'].includes(control.action)) response = await subscriptions.scoped(request, control, () => readBody(request, maxAppendBytes, bodyTimeoutMs))
        if (response) return respond(response.body, response.status, response.headers)
      }
      const principal = await options.auth.authenticate(request)
      if (!principal || (principal.expiresAt !== undefined && principal.expiresAt <= Date.now())) throw new StreamError(401, 'UNAUTHENTICATED', 'Authentication required')
      if (!principal.subject || !principal.tenantId || !principal.tenantId.isWellFormed() || !principal.subject.isWellFormed() || Buffer.byteLength(principal.tenantId) > 512
        || (principal.expiresAt !== undefined && !Number.isFinite(principal.expiresAt))) throw new Error('Authentication policy returned an invalid principal')
      if (control && subscriptions) {
        const response = await subscriptions.handle(request, control, principal, options.auth.authorize,
          () => readBody(request, maxAppendBytes, bodyTimeoutMs), canonical?.href ?? `${url.origin}${basePath}`)
        return respond(response.body, response.status, response.headers)
      }
      if (!await options.auth.authorize({ principal, path, operation, request })) throw new StreamError(403, 'FORBIDDEN', 'Operation is not authorized')
      if (path.split('/')[0] === '__ds') {
        throw new StreamError(404, 'CONTROL_ROUTE_NOT_FOUND', 'Reserved control route not found')
      }
      const sourceHeader = request.headers.get('stream-forked-from')
      const forkOffset = request.headers.get('stream-fork-offset')
      const subOffsetHeader = request.headers.get('stream-fork-sub-offset')
      if ((sourceHeader !== null || forkOffset !== null || subOffsetHeader !== null) && request.method !== 'PUT') throw new StreamError(400, 'INVALID_FORK_HEADERS', 'Fork headers require PUT')
      if (sourceHeader === null && (forkOffset !== null || subOffsetHeader !== null)) throw new StreamError(400, 'MISSING_FORK_SOURCE', 'Fork position requires Stream-Forked-From')
      if (request.method === 'PUT') {
        let sourcePath: string | undefined
        if (sourceHeader !== null) {
          if (!sourceHeader.startsWith('/') || sourceHeader.startsWith('//') || sourceHeader.includes('?') || sourceHeader.includes('#')) throw new StreamError(400, 'INVALID_FORK_SOURCE', 'Fork source must be a path on this server')
          sourcePath = streamPath(new URL(sourceHeader, url.origin), basePath) ?? undefined
          if (!sourcePath || sourcePath.split('/')[0] === '__ds') throw new StreamError(400, 'INVALID_FORK_SOURCE', 'Fork source must be an application stream')
          if (!await options.auth.authorize({ principal, path: sourcePath, operation: 'read', request })) throw new StreamError(403, 'FORBIDDEN', 'Source read is not authorized')
        }
        if (subOffsetHeader !== null && (!/^\d+$/.test(subOffsetHeader) || !Number.isSafeInteger(Number(subOffsetHeader)))) throw new StreamError(400, 'INVALID_SUB_OFFSET', 'Sub-offset must be a nonnegative safe integer')
        const body = await readBody(request, maxAppendBytes, bodyTimeoutMs)
        const config = { body, closed: request.headers.get('stream-closed')?.toLowerCase() === 'true', ...createExpiry(request.headers) }
        const result = sourcePath
          ? await store.createFork(principal.tenantId, path, { ...config, sourcePath, subOffset: Number(subOffsetHeader ?? '0'),
            ...(forkOffset !== null ? { offset: forkOffset } : {}), ...(request.headers.has('content-type') ? { contentType: contentType(request) } : {}) })
          : await store.create(principal.tenantId, path, { ...config, contentType: contentType(request) })
        const headers = metadataHeaders(result.stream)
        if (result.created) headers.set('Location', canonical ? `${canonical.href}/${url.pathname.slice(basePath.length + 1)}` : new URL(url.pathname, url.origin).href)
        return respond(null, result.created ? 201 : 200, headers)
      }
      if (request.method === 'POST') {
        const producer = producerHeaders(request.headers)
        const sequence = request.headers.get('stream-seq')
        if (sequence !== null && sequence.length > 4096) throw new StreamError(400, 'INVALID_SEQUENCE', 'Stream-Seq is too long')
        const body = await readBody(request, maxAppendBytes, bodyTimeoutMs)
        if (body.length && request.headers.has('content-type')) contentType(request)
        const result = await store.append(principal.tenantId, path, { body, contentType: request.headers.get('content-type'),
          close: request.headers.get('stream-closed')?.toLowerCase() === 'true', ...(producer ? { producer } : {}), ...(sequence !== null ? { sequence } : {}) })
        const headers = metadataHeaders(result.stream)
        if (result.producer) { headers.set('Producer-Epoch', String(result.producer.epoch)); headers.set('Producer-Seq', String(result.producer.seq)) }
        return respond(null, result.status, headers)
      }
      if (request.method === 'DELETE') { await store.delete(principal.tenantId, path); return respond(null, 204) }
      if (request.method === 'HEAD') return respond(null, 200, metadataHeaders(await store.head(principal.tenantId, path)))
      if (url.searchParams.getAll('offset').length > 1) throw new StreamError(400, 'MULTIPLE_OFFSETS', 'Specify one offset')
      const offset = url.searchParams.get('offset') ?? '-1'
      if (url.searchParams.has('live')) {
        if (url.searchParams.getAll('live').length !== 1 || !['long-poll', 'sse'].includes(url.searchParams.get('live')!)) throw new StreamError(400, 'INVALID_LIVE_MODE', 'Expected live=long-poll or live=sse')
        if (!url.searchParams.has('offset')) throw new StreamError(400, 'MISSING_OFFSET', 'Live reads require an offset')
        if (!live) throw new StreamError(503, 'LIVE_NOT_CONFIGURED', 'Live reads require a configured notifier')
        const liveRequest = new Request(request, { signal: AbortSignal.any([request.signal, shutdown.signal]) })
        const response = await live.serve(liveRequest, principal.tenantId, path, offset, principal.expiresAt)
        return respond(response.body, response.status, response.headers)
      }
      const release = store.reserveRead()
      let releaseBody: (() => void) | undefined
      const remainingAuth = principal.expiresAt === undefined ? 300000 : Math.max(1, Math.ceil(principal.expiresAt - Date.now()))
      const signal = AbortSignal.any([request.signal, shutdown.signal, AbortSignal.timeout(Math.min(responseTimeoutMs, remainingAuth))])
      try {
        const result = await store.read(principal.tenantId, path, offset, request.headers.get('if-none-match'), signal)
        const headers = metadataHeaders(result.stream)
        headers.set('Stream-Next-Offset', result.next)
        if (!result.upToDate) headers.delete('Stream-Closed')
        else headers.set('Stream-Up-To-Date', 'true')
        if (offset !== 'now') headers.set('ETag', result.etag!)
        if (result.notModified) { release(); return respond(null, 304, headers) }
        release()
        releaseBody = store.reserveBody(result.body.byteLength)
        headers.set('Content-Length', String(result.body.byteLength))
        return respond(ownedResponseBody(result.body, { signal, release: releaseBody, timeoutMs: responseTimeoutMs,
          ...(principal.expiresAt === undefined ? {} : { expiresAt: principal.expiresAt }) }), 200, headers)
      } catch (error) {
        release()
        releaseBody?.()
        throw error
      }
    } catch (error) {
      if (error instanceof StreamError) return respond(JSON.stringify({ error: { code: error.code, message: error.message } }), error.status,
        { 'Content-Type': 'application/json', ...error.headers })
      report(error)
      return respond(JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } }), 500, { 'Content-Type': 'application/json' })
    }
  }

  return {
    maintain(input) {
      if (closed) return Promise.reject(new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down'))
      const started = performance.now()
      const work = (async () => {
        const maximum = input?.maxSubscriptionWork ?? 16
        if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid subscription maintenance batch')
        const chunks = input?.maxChunks ?? 8
        if (!Number.isSafeInteger(chunks) || chunks < 1 || chunks > 128) throw new RangeError('Invalid chunk maintenance batch')
        if (input?.timeBudgetMs !== undefined && (!Number.isSafeInteger(input.timeBudgetMs) || input.timeBudgetMs < 1 || input.timeBudgetMs > 300_000)) throw new RangeError('Invalid maintenance time budget')
        if (input?.forceHot !== undefined && typeof input.forceHot !== 'boolean') throw new TypeError('Invalid hot chunk rotation policy')
        if (input && 'freezeHot' in input) throw new TypeError('freezeHot was replaced by age-based maintenance; use forceHot only for an explicit drain')
        const until = input?.timeBudgetMs === undefined ? Infinity : started + input.timeBudgetMs
        const total: Record<keyof MaintenanceResult, number> = { frozen: 0, sealed: 0, retired: 0, changes: 0, deliveries: 0, links: 0,
          reclaimedChunks: 0, deletedObjects: 0, prunedRows: 0, inventoriedObjects: 0 }
        // Every round gives each work class a chance. A continuously busy
        // outbox must not consume the entire deadline before delivery/sealing.
        for (let i = 0; i < Math.max(chunks, maximum) && !closed; i++) {
          const current = {
            ...(subscriptions && i < maximum ? await subscriptions.maintain(1) : { changes: 0, deliveries: 0, links: 0, worked: false }),
            ...(i < chunks ? await store.maintain({ maxChunks: 1, forceHot: input?.forceHot ?? false }) : { frozen: 0, sealed: 0, retired: 0,
              reclaimedChunks: 0, deletedObjects: 0, prunedRows: 0, inventoriedObjects: 0, reclamationWorked: false }),
          }
          for (const key of Object.keys(total) as (keyof MaintenanceResult)[]) total[key] += current[key]
          if (!Object.values(current).some(Boolean) || performance.now() >= until) break
        }
        return total
      })()
      pending.add(work)
      void work.finally(() => pending.delete(work)).catch(() => {})
      void work.then((result) => observe(options.onEvent, { type: 'maintenance', success: true, durationMs: performance.now() - started, result: { ...result } }, report),
        () => observe(options.onEvent, { type: 'maintenance', success: false, durationMs: performance.now() - started }, report))
      return work
    },
    fetch(request) {
      const started = performance.now()
      const response = handle(request)
      pending.add(response)
      void response.finally(() => pending.delete(response)).catch(() => {})
      void response.then((result) => observe(options.onEvent, { type: 'request', method: request.method, status: result.status, durationMs: performance.now() - started }, report), report)
      return response
    },
    async inspectHealth() { await store.assertReady(); await store.guard.check(); await subscriptions?.assertReady(); return { ready: !closed, mode: options.storage ? 'tiered' : 'hot-log', features: ['create', 'append', 'catch-up', 'head', 'close', 'delete', 'json', 'idempotent-producers', 'ttl', 'forks', ...(live ? ['long-poll', 'sse'] : []), ...(options.storage ? ['sealed-storage'] : []), ...(subscriptions ? ['subscriptions'] : [])] } },
    async close({ deadlineMs = 4000 } = {}) {
      if (!Number.isSafeInteger(deadlineMs) || deadlineMs < 0 || deadlineMs > 300_000) throw new RangeError('Invalid shutdown deadline')
      closed = true
      shutdown.abort(new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down'))
      live?.close()
      subscriptions?.close()
      let timer: ReturnType<typeof setTimeout> | undefined
      try { await Promise.race([(async () => { await Promise.allSettled([...pending]); await live?.drain() })(), new Promise<void>((resolve) => { timer = setTimeout(resolve, deadlineMs) })]) }
      finally { clearTimeout(timer); store.close() }
    },
  }
}
