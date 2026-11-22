import { createHash } from 'node:crypto'
import type { Pool } from 'pg'
import { StreamError } from './errors.js'
import { normalizeBasePath, streamPath } from './path.js'
import { PostgresStreamStore } from './store.js'
import type { Producer, StreamMetadata } from './store.js'
import { LiveReads } from './live.js'
import type { LiveOptions } from './live.js'
import type { StorageOptions } from './storage.js'

export type Operation = 'create' | 'append' | 'read' | 'head' | 'delete'
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
}
export interface ServerOptions {
  readonly postgres: { readonly pool: Pool; readonly schema?: string }
  readonly auth: {
    readonly authenticate: (request: Request) => Principal | null | Promise<Principal | null>
    readonly authorize: (context: AuthorizationContext) => boolean | Promise<boolean>
  }
  readonly basePath?: string
  readonly publicBaseUrl?: string
  readonly allowedOrigins?: readonly string[]
  readonly live?: LiveOptions
  readonly storage?: StorageOptions
  readonly limits?: { readonly maxAppendBytes?: number; readonly maxStoredBytes?: number; readonly maxPageBytes?: number; readonly bodyTimeoutMs?: number }
  readonly onError?: (error: unknown) => void
}
export interface DurableStreamsServer {
  readonly fetch: (request: Request) => Promise<Response>
  readonly inspectHealth: () => Promise<{ ready: boolean; mode: 'hot-log'; features: readonly string[] }>
  readonly maintain: (options?: { readonly maxChunks?: number }) => Promise<{ frozen: number; sealed: number }>
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
  if (!Number.isSafeInteger(maxAppendBytes) || maxAppendBytes < 1 || maxAppendBytes > 16 * 1024 * 1024
    || !Number.isSafeInteger(bodyTimeoutMs) || bodyTimeoutMs < 1 || bodyTimeoutMs > 300_000) throw new RangeError('Invalid request limits')
  const canonical = options.publicBaseUrl ? new URL(options.publicBaseUrl) : undefined
  if (canonical && (!['https:', 'http:'].includes(canonical.protocol) || canonical.username || canonical.password || canonical.search || canonical.hash || canonical.pathname !== basePath)) throw new TypeError('publicBaseUrl must match basePath without credentials/query/fragment')
  const store = new PostgresStreamStore(options.postgres.pool, options.postgres.schema ?? 'durable_streams', options.limits?.maxStoredBytes, options.limits?.maxPageBytes, options.storage)
  const report = (error: unknown) => { try { options.onError?.(error) } catch { /* observer only */ } }
  const live = options.live ? new LiveReads(store, options.postgres.schema ?? 'durable_streams', options.live, report) : undefined
  const pending = new Set<Promise<unknown>>()
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
      const path = streamPath(url, basePath)
      if (path === null) throw new StreamError(404, 'ROUTE_NOT_FOUND', 'Route not found')
      if (request.method === 'OPTIONS') return respond(null, 204, { 'Access-Control-Allow-Methods': 'PUT, POST, GET, HEAD, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset, Producer-Id, Producer-Epoch, Producer-Seq' })
      const operation = METHODS.get(request.method)
      if (!operation) return respond(null, 405, { Allow: 'PUT, POST, GET, HEAD, DELETE, OPTIONS' })
      const principal = await options.auth.authenticate(request)
      if (!principal || (principal.expiresAt !== undefined && principal.expiresAt <= Date.now())) throw new StreamError(401, 'UNAUTHENTICATED', 'Authentication required')
      if (!principal.subject || !principal.tenantId || Buffer.byteLength(principal.tenantId) > 512
        || (principal.expiresAt !== undefined && !Number.isFinite(principal.expiresAt))) throw new Error('Authentication policy returned an invalid principal')
      if (!await options.auth.authorize({ principal, path, operation, request })) throw new StreamError(403, 'FORBIDDEN', 'Operation is not authorized')
      if (path.split('/')[0] === '__ds'
        || ['stream-forked-from', 'stream-fork-offset', 'stream-fork-sub-offset'].some((h) => request.headers.has(h))) {
        throw new StreamError(501, 'FEATURE_NOT_IMPLEMENTED', 'Forks and subscriptions are not implemented yet')
      }
      if (request.method === 'PUT') {
        const body = await readBody(request, maxAppendBytes, bodyTimeoutMs)
        const result = await store.create(principal.tenantId, path, { body, contentType: contentType(request), closed: request.headers.get('stream-closed')?.toLowerCase() === 'true', ...createExpiry(request.headers) })
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
        const response = await live.serve(request, principal.tenantId, path, offset, principal.expiresAt)
        return respond(response.body, response.status, response.headers)
      }
      const result = await store.read(principal.tenantId, path, offset)
      const headers = metadataHeaders(result.stream)
      headers.set('Stream-Next-Offset', result.next)
      if (!result.upToDate) headers.delete('Stream-Closed')
      else headers.set('Stream-Up-To-Date', 'true')
      if (offset !== 'now') {
        const etag = `"${createHash('sha256').update(`${result.stream.id}:${offset}:${result.next}:${result.stream.closed}`).update(result.body).digest('hex')}"`
        headers.set('ETag', etag)
        const matches = request.headers.get('if-none-match')?.split(',').map((v) => v.trim()) ?? []
        if (matches.some((v) => v === '*' || v.replace(/^W\//, '') === etag)) return respond(null, 304, headers)
      }
      return respond(new Uint8Array(result.body), 200, headers)
    } catch (error) {
      if (error instanceof StreamError) return respond(JSON.stringify({ error: { code: error.code, message: error.message } }), error.status,
        { 'Content-Type': 'application/json', ...error.headers })
      try { options.onError?.(error) } catch { /* observer failures must not affect outcomes */ }
      return respond(JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } }), 500, { 'Content-Type': 'application/json' })
    }
  }

  return {
    maintain(input) {
      if (closed) return Promise.reject(new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down'))
      const work = store.maintain(input)
      pending.add(work)
      void work.finally(() => pending.delete(work)).catch(() => {})
      return work
    },
    fetch(request) {
      const response = handle(request)
      pending.add(response)
      void response.finally(() => pending.delete(response)).catch(() => {})
      return response
    },
    async inspectHealth() { await store.assertReady(); return { ready: !closed, mode: 'hot-log', features: ['create', 'append', 'catch-up', 'head', 'close', 'delete', 'json', 'idempotent-producers', 'ttl', ...(live ? ['long-poll', 'sse'] : [])] } },
    async close({ deadlineMs = 4000 } = {}) {
      if (!Number.isSafeInteger(deadlineMs) || deadlineMs < 0 || deadlineMs > 300_000) throw new RangeError('Invalid shutdown deadline')
      closed = true
      live?.close()
      let timer: ReturnType<typeof setTimeout> | undefined
      try { await Promise.race([Promise.allSettled([...pending]), new Promise<void>((resolve) => { timer = setTimeout(resolve, deadlineMs) })]) }
      finally { clearTimeout(timer) }
    },
  }
}
