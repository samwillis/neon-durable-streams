import { admitLive } from './admission.js'
import { randomInt } from 'node:crypto'
import { StreamError } from './errors.js'
import type { HeadWatch, StreamNotifier, StreamHead } from './neon.js'
import { mediaType, formatPayloads } from './payload.js'
import type { PostgresStreamStore, ReadResult } from './store.js'
import { PayloadWindow } from './live-payload.js'

export interface LiveOptions {
  /** Consumer-owned notifier. Production Neon servers use createNeonRealtimeNotifier. */
  readonly notifier: StreamNotifier
  readonly longPollTimeoutMs?: number
  readonly fallbackIntervalMs?: number
  readonly heartbeatMs?: number
  readonly maxSseDurationMs?: number
  /** Per instance, additionally bounded by installation-wide database admission. */
  readonly maxReaders?: number
  readonly maxBufferedBytes?: number
  readonly maxBufferedFrames?: number
  readonly payloadWindowMs?: number
  readonly validationIntervalMs?: number
}

export function responseCursor(previous: string | null, now = Date.now()): string {
  const current = BigInt(Math.max(0, Math.floor((now - Date.UTC(2024, 9, 9)) / 20_000)))
  // Bound untrusted BigInt input while preserving monotonicity for valid cursors.
  if (previous && /^\d{1,128}$/.test(previous) && BigInt(previous) >= current) {
    return String(BigInt(previous) + BigInt(Math.ceil(randomInt(1, 3601) / 20)))
  }
  return String(current)
}

interface Channel {
  version: string | null
  revision: number
  references: number
  disposed: boolean
  watch?: HeadWatch
  timer?: ReturnType<typeof setTimeout>
  waiters: Set<() => void>
  head: StreamHead | null | undefined
  validation: Pick<ReadResult, 'stream' | 'validatedAt' | 'validUntil'>
  validating?: Promise<Pick<ReadResult, 'stream' | 'validatedAt' | 'validUntil'>>
}

/** Authorized durable replay plus bounded, contiguous committed Realtime payloads. */
export class LiveReads {
  private readonly channels = new Map<string, Channel>()
  private readonly active = new Set<AbortController>()
  private readonly releases = new Set<Promise<void>>()
  private readonly timeout: number
  private readonly fallback: number
  private readonly heartbeat: number
  private readonly duration: number
  private readonly maximum: number
  private readonly bufferBytes: number
  private readonly bufferFrames: number
  private readonly windowMs: number
  private readonly validationMs: number
  private closed = false

  constructor(private readonly store: PostgresStreamStore, private readonly schema: string,
    private readonly options: LiveOptions, private readonly report: (error: unknown) => void) {
    this.timeout = options.longPollTimeoutMs ?? 25_000
    this.fallback = options.fallbackIntervalMs ?? 5000
    this.heartbeat = options.heartbeatMs ?? 15_000
    this.duration = options.maxSseDurationMs ?? 60_000
    this.maximum = options.maxReaders ?? 80
    this.bufferBytes = options.maxBufferedBytes ?? 2 * 1024 * 1024
    this.bufferFrames = options.maxBufferedFrames ?? 256
    this.windowMs = options.payloadWindowMs ?? 10_000
    this.validationMs = options.validationIntervalMs ?? 1000
    for (const [value, min, max] of [[this.timeout, 1, 300_000], [this.fallback, 100, 60_000],
      [this.heartbeat, 100, 60_000], [this.duration, 100, 300_000], [this.maximum, 1, 100_000],
      [this.bufferBytes, 1, 64 * 1024 * 1024], [this.bufferFrames, 1, 4096], [this.windowMs, 100, 60_000], [this.validationMs, 100, 60_000]]) {
      if (!Number.isSafeInteger(value) || value! < min! || value! > max!) throw new RangeError('Invalid live read limits')
    }
    if (typeof options.notifier?.watch !== 'function') throw new TypeError('Live reads require a notifier')
  }

  private channel(tenant: string, id: string, initial: ReadResult) {
    const key = JSON.stringify([tenant, id])
    let channel = this.channels.get(key)
    if (!channel) {
      const current: Channel = { version: initial.stream.change_version, revision: 0, references: 0, disposed: false, waiters: new Set(), head: undefined, validation: initial }
      this.channels.set(key, current)
      const update = (next: string | null) => {
        if (current.disposed || next === current.version) return
        // Delayed authoritative snapshots can trail a more recent SQL recheck.
        if (next !== null && current.version !== null && BigInt(next) < BigInt(current.version)) return
        current.version = next
        current.revision++
        for (const wake of [...current.waiters]) wake()
      }
      try {
        current.watch = this.options.notifier.watch({ schema: this.schema, tenantId: tenant, streamId: id },
          (head) => {
            if (head && current.version !== null && BigInt(head.version) < BigInt(current.version)) return
            current.head = head
            update(head && !head.deleted ? head.version : null)
          })
        void current.watch.ready.catch(this.report)
      } catch (error) { this.report(error) }
      const poll = async () => {
        try { update(await this.store.headVersion(tenant, id)) } catch (error) {
          if (error instanceof StreamError && error.code === 'INSTALLATION_FENCED') update(null)
          else this.report(error)
        }
        finally { if (!current.disposed) current.timer = setTimeout(() => { void poll() }, this.fallback).unref() }
      }
      current.timer = setTimeout(() => { void poll() }, this.fallback).unref()
      channel = current
    }
    const watched = channel
    watched.references++
    return {
      revision: () => watched.revision,
      head: () => watched.head,
      wake: () => { if (!watched.disposed) { watched.revision++; for (const wake of [...watched.waiters]) wake() } },
      observe: (value: Pick<ReadResult, 'stream' | 'validatedAt' | 'validUntil'>) => {
        if (value.validatedAt >= watched.validation.validatedAt) watched.validation = value
      },
      validate: async () => {
        const previous = watched.validation
        if (performance.now() - previous.validatedAt < this.validationMs && performance.now() < previous.validUntil) return previous
        watched.validating ??= this.store.validateGeneration(tenant, id).then((value) => { watched.validation = value; return value })
          .finally(() => { delete watched.validating })
        return watched.validating
      },
      wait: (revision: number, timeoutMs: number, signal: AbortSignal) => new Promise<void>((resolve, reject) => {
        if (signal.aborted) { reject(signal.reason); return }
        if (watched.revision !== revision || watched.disposed) { resolve(); return }
        const cleanup = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); watched.waiters.delete(wake) }
        const wake = () => { cleanup(); resolve() }
        const abort = () => { cleanup(); reject(signal.reason) }
        const timer = setTimeout(wake, Math.max(0, timeoutMs))
        watched.waiters.add(wake)
        signal.addEventListener('abort', abort, { once: true })
      }),
      release: () => {
        if (--watched.references) return
        watched.disposed = true
        clearTimeout(watched.timer)
        watched.watch?.close()
        for (const wake of [...watched.waiters]) wake()
        this.channels.delete(key)
      },
    }
  }

  async serve(request: Request, tenant: string, path: string, offset: string, expiresAt?: number): Promise<Response> {
    if (this.closed) throw new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down')
    if (this.active.size >= this.maximum) throw new StreamError(429, 'LIVE_LIMIT', 'Live reader capacity reached', { 'Retry-After': '1' })
    const abort = new AbortController()
    this.active.add(abort)
    const canceled = () => abort.abort(new StreamError(408, 'REQUEST_ABORTED', 'Request aborted'))
    request.signal.addEventListener('abort', canceled, { once: true })
    if (request.signal.aborted) canceled()
    const expiry = expiresAt === undefined ? undefined : setTimeout(() => abort.abort(new StreamError(401, 'AUTH_EXPIRED', 'Authentication expired')), Math.max(0, Math.min(2_147_483_647, expiresAt - Date.now())))
    // The hard local deadline precedes the database lease expiry. A suspended
    // runtime must observe abort before replaying after the deadline.
    const lifetime = Math.max(this.timeout, this.duration) + 15_000
    const hardDeadline = performance.now() + lifetime
    const deadlineTimer = setTimeout(() => abort.abort(), lifetime).unref()
    const assertActive = () => {
      // Timer callbacks can run after promise continuations when a suspended
      // isolate resumes. Check monotonic elapsed time before any new emission.
      if (performance.now() >= hardDeadline) abort.abort()
      abort.signal.throwIfAborted()
    }
    let releaseAdmission: (() => Promise<void>) | undefined
    let subscription: ReturnType<LiveReads['channel']> | undefined
    let payload: PayloadWindow | undefined
    let released = false
    const inFlight = new Set<Promise<unknown>>()
    const track = <T>(work: Promise<T>): Promise<T> => {
      const pending = work.finally(() => { inFlight.delete(pending) })
      inFlight.add(pending)
      return pending
    }
    const release = () => {
      if (released) return
      released = true
      clearTimeout(expiry)
      clearTimeout(deadlineTimer)
      // Stop notifications immediately, but retain capacity while an already
      // issued SQL/object read settles. Cancellation storms must not admit an
      // unbounded number of detached replay operations.
      request.signal.removeEventListener('abort', canceled)
      payload?.close()
      subscription?.release()
      const work = (async () => {
        await Promise.allSettled([...inFlight])
        await releaseAdmission?.()
      })().catch((error: unknown) => {
        if (!(error instanceof StreamError && error.code === 'INSTALLATION_FENCED')) this.report(error)
      }).finally(() => { this.active.delete(abort) })
      this.releases.add(work)
      void work.finally(() => this.releases.delete(work))
    }
    try {
      assertActive()
      releaseAdmission = await admitLive(this.store.guard, this.schema, lifetime + 5000)
      assertActive()
      const initial = await this.store.read(tenant, path, offset)
      assertActive()
      const id = initial.stream.id
      const isSse = new URL(request.url).searchParams.get('live') === 'sse'
      let cursor = new URL(request.url).searchParams.get('cursor')
      const headers = new Headers({ 'Stream-Next-Offset': initial.next, 'Content-Type': initial.stream.content_type, 'Cache-Control': 'no-store' })
      if (initial.upToDate) headers.set('Stream-Up-To-Date', 'true')
      if (initial.stream.closed && initial.upToDate) headers.set('Stream-Closed', 'true')
      if (initial.stream.ttl_seconds !== null) headers.set('Stream-TTL', String(initial.stream.ttl_seconds))
      if (initial.stream.expires_at) headers.set('Stream-Expires-At', initial.stream.expires_at.toISOString())
      if (!isSse && (initial.hasData || initial.stream.closed)) {
        headers.set('Stream-Cursor', responseCursor(cursor))
        release()
        return new Response(initial.hasData ? new Uint8Array(initial.body.buffer, initial.body.byteOffset, initial.body.byteLength) : null, { status: initial.hasData ? 200 : 204, headers })
      }
      subscription = this.channel(tenant, id, initial)
      const channel = subscription
      channel.observe(initial)
      if (this.options.notifier.watchAppends) payload = new PayloadWindow({ notifier: this.options.notifier, schema: this.schema, tenantId: tenant, streamId: id,
        installation: await this.store.guard.snapshot(), maxBytes: this.bufferBytes, maxFrames: this.bufferFrames, windowMs: this.windowMs, wake: channel.wake, report: this.report }, initial)
      const replay = async (at: string) => {
        const revision = payload?.revision ?? -1
        const result = await this.store.readGeneration(tenant, id, at)
        channel.observe(result); payload?.reconcile(result, revision)
        return result
      }
      const page = async (at: string): Promise<ReadResult> => {
        if (!payload || payload.needsReplay || !payload.verified) return replay(at)
        const head = channel.head()
        if (head === null || head?.deleted) return replay(at)
        const validation = await channel.validate()
        const metadata = head && BigInt(head.version) >= BigInt(validation.stream.change_version)
          ? { ...validation.stream, tail_offset: head.offset, change_version: head.version, closed: head.closed } : validation.stream
        const batch = payload.take(at, this.store.replayLimits.maxBytes, this.store.replayLimits.maxFrames, metadata.tail_offset)
        if (!batch && payload.needsReplay) return replay(at)
        const next = batch?.next ?? at
        if (metadata.closed && next < metadata.tail_offset && !batch?.frames.length) return replay(at)
        return { stream: metadata, body: formatPayloads((batch?.frames ?? []).map((frame) => frame.payload), metadata.content_type),
          next, upToDate: !batch?.ahead && next >= metadata.tail_offset, hasData: Boolean(batch?.frames.length), validatedAt: validation.validatedAt, validUntil: validation.validUntil }
      }
      const waitForPage = async (at: string, deadline: number): Promise<ReadResult> => {
        let awaitingPayload: number | undefined
        for (;;) {
          assertActive()
          const revision = channel.revision()
          const result = await track(page(at))
          assertActive()
          if (result.hasData || result.stream.closed || Date.now() >= deadline) return result
          if (!result.upToDate) {
            awaitingPayload ??= performance.now()
            if (performance.now() - awaitingPayload >= 50) return track(replay(at))
          } else awaitingPayload = undefined
          const wait = Math.min(deadline - Date.now(), payload ? this.validationMs : Infinity, Math.max(1, result.validUntil - performance.now()),
            awaitingPayload === undefined ? Infinity : Math.max(1, 50 - (performance.now() - awaitingPayload)))
          await channel.wait(revision, wait, abort.signal)
        }
      }
      if (!isSse) {
        const deadline = Date.now() + this.timeout
        const result = await waitForPage(initial.next, deadline)
        assertActive()
        headers.set('Stream-Next-Offset', result.next)
        headers.set('Stream-Cursor', responseCursor(cursor))
        if (result.upToDate) headers.set('Stream-Up-To-Date', 'true')
        else headers.delete('Stream-Up-To-Date')
        if (result.stream.closed && result.upToDate) headers.set('Stream-Closed', 'true')
        release()
        return new Response(result.hasData ? new Uint8Array(result.body.buffer, result.body.byteOffset, result.body.byteLength) : null, { status: result.hasData ? 200 : 204, headers })
      }

      const binary = !mediaType(initial.stream.content_type).startsWith('text/') && mediaType(initial.stream.content_type) !== 'application/json'
      headers.set('Content-Type', 'text/event-stream')
      headers.set('Cache-Control', 'no-cache, no-store, no-transform')
      headers.set('X-Accel-Buffering', 'no')
      if (binary) headers.set('Stream-SSE-Data-Encoding', 'base64')
      let next = initial.next
      let first: ReadResult | undefined = initial
      let ending = false
      let rotation: ReturnType<typeof setTimeout>
      const thisDuration = this.duration
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          const end = () => {
            if (ending) return
            ending = true
            clearTimeout(rotation)
            abort.signal.removeEventListener('abort', end)
            controller.close()
            release()
          }
          abort.signal.addEventListener('abort', end, { once: true })
          rotation = setTimeout(() => abort.abort(), thisDuration)
          if (abort.signal.aborted) end()
        },
        pull: async (controller) => {
          try {
            assertActive()
            let result = first
            first = undefined
            if (!result) {
              result = await waitForPage(next, Date.now() + this.heartbeat)
              if (ending) return
              assertActive()
              if (!result.hasData && !result.stream.closed) { controller.enqueue(Buffer.from(': heartbeat\n\n')); return }
            }
            if (ending) return
            assertActive()
            next = result.next
            const eof = result.stream.closed && result.upToDate
            cursor = responseCursor(cursor)
            let frame = ''
            if (result.hasData) {
              const data = binary ? result.body.toString('base64') : result.body.toString('utf8').replace(/\r\n|\r/g, '\n')
              frame += `event: data\n${data.split('\n').map((line) => `data:${line}`).join('\n')}\n\n`
            }
            frame += `event: control\ndata:${JSON.stringify({ streamNextOffset: next, ...(eof ? { streamClosed: true } : { streamCursor: cursor }), ...(result.upToDate ? { upToDate: true } : {}) })}\n\n`
            // Data and its checkpoint occupy a single bounded queue element.
            controller.enqueue(Buffer.from(frame))
            if (eof) abort.abort()
          } catch (error) {
            if (!ending) { if (!(error instanceof StreamError)) this.report(error); abort.abort(error) }
          }
        },
        cancel() {
          // The stream is already canceled before this callback runs. Prevent
          // the abort listener from trying to close its controller a second time.
          ending = true
          clearTimeout(rotation)
          abort.abort()
          release()
        },
      }, { highWaterMark: 1 })
      return new Response(body, { status: 200, headers })
    } catch (error) { release(); throw error }
  }

  close(): void {
    this.closed = true
    for (const abort of this.active) abort.abort(new StreamError(503, 'SERVER_CLOSED', 'Server is shutting down'))
  }

  async drain(): Promise<void> { await Promise.allSettled([...this.releases]) }
}
