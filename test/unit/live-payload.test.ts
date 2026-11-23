import { expect, test, vi } from 'vitest'
import fc from 'fast-check'
import { AppendBuffer, PayloadWindow } from '../../src/live-payload.js'
import type { AppendUpdate, RealtimeAppend } from '../../src/live-payload.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import type { StreamNotifier } from '../../src/neon.js'
import { createDurableStreams } from '../../src/index.js'

const streamId = '00000000-0000-4000-8000-000000000002'
function frame(start: string, text: string, rowId = text): RealtimeAppend {
  const payload = Buffer.from(text)
  return { rowId, streamId, start, end: advanceOffset(start, payload.length), payload, messageEnds: null }
}
const a = frame(ZERO_OFFSET, 'a'), b = frame(a.end, 'b'), c = frame(b.end, 'c')
test.each([false, true])('live subscriptions and SQL never precede authentication/authorization (authenticated=%s)', async (authenticated) => {
  const query = vi.fn(() => { throw new Error('Unauthorized database access') })
  const watch = vi.fn(() => { throw new Error('Unauthorized Realtime access') })
  const handler = createDurableStreams({ postgres: { pool: { query } as never },
    auth: { authenticate: () => authenticated ? { tenantId: 'denied', subject: 'fixture' } : null, authorize: () => false },
    live: { notifier: { watch, watchAppends: watch } } })
  try {
    const response = await handler.fetch(new Request('https://fixture.test/v1/stream/private?offset=now&live=sse'))
    expect(response.status).toBe(authenticated ? 403 : 401)
    expect(query).not.toHaveBeenCalled(); expect(watch).not.toHaveBeenCalled()
  } finally { await handler.close() }
})
const update = (...rows: RealtimeAppend[]): AppendUpdate => ({ type: 'changes', rows, removed: [] })
function buffer(bytes = 1024, frames = 16) {
  const value = new AppendBuffer(streamId, bytes, frames)
  value.reconcile(ZERO_OFFSET, value.revision)
  return value
}
test('unordered duplicate batches produce only contiguous bytes and discard acknowledged rows', () => {
  const value = buffer()
  expect(value.ingest(update(c, a, b, a), ZERO_OFFSET)).toBe(true)
  const page = value.take(ZERO_OFFSET, 1024, 2)!
  expect(Buffer.concat(page.frames.map((row) => row.payload)).toString()).toBe('ab')
  expect(page.next).toBe(b.end); expect(page.ahead).toBe(true)
  value.ingest(update(a, b), b.end)
  expect(value.take(b.end, 1024, 2)!.frames.map((row) => row.payload.toString())).toEqual(['c'])
})
test('a committed head watermark gates bytes that might belong to an atomic close', () => {
  const value = buffer(); value.ingest(update(a, b), ZERO_OFFSET)
  expect(value.take(ZERO_OFFSET, 1024, 16, ZERO_OFFSET)).toMatchObject({ frames: [], ahead: true })
  expect(value.needsReplay).toBe(false)
  expect(value.take(ZERO_OFFSET, 1024, 16, b.end)!.frames).toHaveLength(2)
})
test('gaps cannot advance a checkpoint and force authoritative replay', () => {
  const value = buffer(); value.ingest(update(b, c), ZERO_OFFSET)
  expect(value.take(ZERO_OFFSET, 1024, 16)).toBeUndefined()
  expect(value.needsReplay).toBe(true)
})
test.each(['null', 'remove', 'reset', 'conflict', 'overflow', 'wrong-stream', 'bad-offset', 'overlap'])('%s invalidates pending payloads without returning a partial batch', (kind) => {
  const value = buffer(2, 2)
  value.ingest(update(a), ZERO_OFFSET)
  if (kind === 'null') value.ingest(update({ ...b, payload: null }), ZERO_OFFSET)
  if (kind === 'remove') value.ingest({ type: 'changes', rows: [], removed: ['a'] }, ZERO_OFFSET)
  if (kind === 'reset') value.ingest({ type: 'replace', rows: [c], removed: [] }, ZERO_OFFSET)
  if (kind === 'conflict') value.ingest(update({ ...a, payload: Buffer.from('z') }), ZERO_OFFSET)
  if (kind === 'overflow') value.ingest(update(b, c), ZERO_OFFSET)
  if (kind === 'wrong-stream') value.ingest(update({ ...b, streamId: 'other' }), ZERO_OFFSET)
  if (kind === 'bad-offset') value.ingest(update({ ...b, end: c.end }), ZERO_OFFSET)
  if (kind === 'overlap') value.ingest(update({ ...a, start: a.start, end: b.end, payload: Buffer.from('xxxxxxx') }), ZERO_OFFSET)
  expect(value.needsReplay).toBe(true)
  expect(value.take(ZERO_OFFSET, 1024, 16)).toBeUndefined()
})
test('sealing already emitted bytes cannot invalidate the next contiguous append', () => {
  const value = buffer(); value.ingest(update(a), ZERO_OFFSET); value.take(ZERO_OFFSET, 1024, 16)
  expect(value.ingest(update({ ...a, payload: null }, b), a.end)).toBe(true)
  expect(value.take(a.end, 1024, 16)!.next).toBe(b.end)
})
test('snapshot replacement fences an in-flight SQL reconciliation token', () => {
  const value = buffer(), revision = value.revision
  value.ingest({ type: 'replace', rows: [b], removed: [] }, ZERO_OFFSET)
  expect(value.reconcile(a.end, revision)).toBe(false)
  expect(value.reconcile(a.end, value.revision)).toBe(true)
  expect(value.take(a.end, 1024, 16)!.next).toBe(b.end)
})
test('a raw window covers setup races, replaces snapshots, bounds its lifetime, and advances its lower cursor', async () => {
  vi.useFakeTimers()
  const listeners: ((update: AppendUpdate) => void)[] = [], offsets: string[] = []
  const closes = vi.fn(), wake = vi.fn(), errors = vi.fn()
  const notifier: StreamNotifier = { watch() { throw new Error('unused') }, watchAppends(input, listener) {
    offsets.push(input.after); listeners.push(listener); return { ready: Promise.resolve(), close: closes }
  } }
  const initial = { next: ZERO_OFFSET, upToDate: true, stream: { closed: false } }
  const window = new PayloadWindow({ notifier, schema: 'durable_streams', tenantId: 'tenant', streamId,
    installation: { id: streamId, serving_epoch: streamId, serving_state: 'active', environment_id: null, storage_identity: null },
    maxBytes: 1024, maxFrames: 16, windowMs: 1000, wake, report: errors }, initial)
  try {
    listeners[0]!({ type: 'replace', rows: [b, a], removed: [] })
    await Promise.resolve()
    expect(window.needsReplay).toBe(true)
    window.reconcile({ ...initial, next: a.end }, window.revision)
    expect(window.take(a.end, 1024, 16, b.end)!.next).toBe(b.end)
    listeners[0]!({ type: 'replace', rows: [c], removed: [] })
    expect(closes).toHaveBeenCalledTimes(1)
    window.reconcile({ ...initial, next: b.end }, window.revision)
    expect(offsets).toEqual([ZERO_OFFSET, b.end])
    await vi.advanceTimersByTimeAsync(1000)
    expect(window.needsReplay).toBe(true)
    expect(closes).toHaveBeenCalledTimes(2)
    expect(errors).not.toHaveBeenCalled()
  } finally { window.close(); vi.useRealTimers() }
})

test('synchronous invalidation during subscribe closes the returned handle without retaining a timer', async () => {
  vi.useFakeTimers()
  const close = vi.fn(), wake = vi.fn()
  const value = new PayloadWindow({ notifier: { watch() { throw new Error('unused') }, watchAppends(_input, listener) {
    listener({ type: 'invalidate' }); return { ready: Promise.resolve(), close }
  } }, schema: 'fixture', tenantId: 'tenant', streamId,
  installation: { id: streamId, serving_epoch: streamId, serving_state: 'active', environment_id: null, storage_identity: null },
  maxBytes: 1024, maxFrames: 16, windowMs: 1000, wake, report: () => {} }, { next: ZERO_OFFSET, upToDate: true, stream: { closed: false } })
  try { await Promise.resolve(); expect(close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0) }
  finally { value.close(); vi.useRealTimers() }
})

test('generated late events cannot revive a closed payload window or leak a watch', async () => {
  vi.useFakeTimers()
  try {
    await fc.assert(fc.asyncProperty(fc.array(fc.record({ type: fc.constantFrom('ready', 'fail', 'snapshot', 'invalidate', 'replay', 'timer', 'close'), index: fc.nat(20) }), { maxLength: 80 }), async (events) => {
      const handles: { ready: ReturnType<typeof Promise.withResolvers<void>>; listener: (event: AppendUpdate) => void; closes: number }[] = []
      let wakes = 0, reports = 0, closed = false
      const initial = { next: ZERO_OFFSET, upToDate: true, stream: { closed: false } }
      const value = new PayloadWindow({ notifier: { watch() { throw new Error('unused') }, watchAppends(_input, listener) {
        const handle = { ready: Promise.withResolvers<void>(), listener, closes: 0 }; handles.push(handle)
        return { ready: handle.ready.promise, close() { handle.closes++ } }
      } }, schema: 'fixture', tenantId: 'tenant', streamId,
      installation: { id: streamId, serving_epoch: streamId, serving_state: 'active', environment_id: null, storage_identity: null },
      maxBytes: 1024, maxFrames: 16, windowMs: 1000, wake() { wakes++ }, report() { reports++ } }, initial)
      try {
        for (const event of [...events, { type: 'close', index: 0 }, { type: 'replay', index: 0 }]) {
          const before = { count: handles.length, wakes, reports }, handle = handles[event.index % handles.length]!
          if (event.type === 'ready') handle.ready.resolve()
          if (event.type === 'fail') handle.ready.reject(new Error('delayed subscription failure'))
          if (event.type === 'snapshot') handle.listener({ type: 'replace', rows: [], removed: [] })
          if (event.type === 'invalidate') handle.listener({ type: 'invalidate' })
          if (event.type === 'replay') value.reconcile(initial, value.revision)
          if (event.type === 'timer') await vi.advanceTimersByTimeAsync(1000)
          if (event.type === 'close') value.close()
          await Promise.resolve(); await Promise.resolve()
          expect(handles.every((item) => item.closes <= 1)).toBe(true)
          expect(handles.filter((item) => item.closes === 0).length).toBeLessThanOrEqual(1)
          if (closed) expect({ count: handles.length, wakes, reports }).toEqual(before)
          if (event.type === 'close') closed = true
          if (closed) { expect(handles.every((item) => item.closes === 1)).toBe(true); expect(value.verified).toBe(false) }
        }
      } finally { value.close(); vi.clearAllTimers() }
    }), { numRuns: 150 })
  } finally { vi.useRealTimers() }
})

test('subscription readiness and a stale replay cannot skip the required reconciliation', async () => {
  const ready = Promise.withResolvers<void>()
  const initial = { next: ZERO_OFFSET, upToDate: true, stream: { closed: false } }
  const value = new PayloadWindow({ notifier: { watch() { throw new Error('unused') }, watchAppends() { return { ready: ready.promise, close() {} } } },
    schema: 'fixture', tenantId: 'tenant', streamId,
    installation: { id: streamId, serving_epoch: streamId, serving_state: 'active', environment_id: null, storage_identity: null },
    maxBytes: 1024, maxFrames: 16, windowMs: 1000, wake() {}, report() {} }, initial)
  try {
    const staleRevision = value.revision
    value.reconcile(initial, staleRevision)
    expect(value.verified).toBe(false)
    ready.resolve(); await Promise.resolve()
    value.reconcile(initial, staleRevision)
    expect(value.verified).toBe(false)
    value.reconcile(initial, value.revision)
    expect(value.verified).toBe(true)
  } finally { value.close() }
})

test('failed subscription backoff cannot be bypassed by replay or redundant invalidations', async () => {
  const now = vi.spyOn(performance, 'now').mockReturnValue(0), subscribe = vi.fn(() => ({ ready: Promise.reject(new Error('offline')), close() {} }))
  const initial = { next: ZERO_OFFSET, upToDate: true, stream: { closed: false } }
  const value = new PayloadWindow({ notifier: { watch() { throw new Error('unused') }, watchAppends: subscribe }, schema: 'fixture', tenantId: 'tenant', streamId,
    installation: { id: streamId, serving_epoch: streamId, serving_state: 'active', environment_id: null, storage_identity: null },
    maxBytes: 1024, maxFrames: 16, windowMs: 1000, wake() {}, report() {} }, initial)
  try {
    await Promise.resolve()
    for (let i = 0; i < 5; i++) { value.invalidate(); value.reconcile(initial, value.revision) }
    expect(subscribe).toHaveBeenCalledOnce()
    now.mockReturnValue(1001)
    value.reconcile(initial, value.revision)
    expect(subscribe).toHaveBeenCalledTimes(2)
  } finally { value.close(); now.mockRestore(); await Promise.resolve() }
})
