import { expect, test, vi } from 'vitest'
import { LiveReads, responseCursor } from '../../src/live.js'
import type { PostgresStreamStore, ReadResult } from '../../src/store.js'

test('CDN cursors advance past echoed future intervals without losing integer precision', () => {
  const future = '9999999999999999999999999999999999999999'
  expect(BigInt(responseCursor(future))).toBeGreaterThan(BigInt(future))
  const current = responseCursor(null)
  expect(BigInt(responseCursor(current))).toBeGreaterThan(BigInt(current))
  expect(responseCursor('invalid', Date.UTC(2024, 9, 9) + 20_000)).toBe('1')
})

test.each([false, true])('cancellation retains reader capacity until detached replay settles (failure=%s)', async (failure) => {
  const entered = Promise.withResolvers<void>(), resume = Promise.withResolvers<void>()
  const releaseLease = vi.fn(async () => {})
  const closeWatch = vi.fn(), closeAppends = vi.fn(), report = vi.fn()
  const watchAppends = vi.fn(() => ({ ready: Promise.resolve(), close: closeAppends }))
  const initial = { stream: { id: '00000000-0000-4000-8000-000000000001', change_version: '0', content_type: 'text/plain', closed: false, ttl_seconds: null, expires_at: null } as never,
    body: Buffer.alloc(0), next: '0000000000000000_0000000000000000', upToDate: true, hasData: false, validatedAt: performance.now(), validUntil: Infinity } satisfies ReadResult
  const store = { reserveRead: () => () => {}, reserveBody: () => () => {}, guard: {
    async run(_options: unknown, work: (client: unknown) => Promise<unknown>) { return work({ query: async () => ({ rows: [{ max_readers: 1 }], rowCount: 1 }) }) },
    releaseLiveLease: releaseLease,
    snapshot: async () => ({}),
  }, read: async () => initial, async readGeneration() {
    entered.resolve(); await resume.promise
    if (failure) throw new Error('Replay unavailable after cancellation')
    return initial
  } } as unknown as PostgresStreamStore
  const live = new LiveReads(store, 'fixture', { maxReaders: 1, fallbackIntervalMs: 60_000,
    notifier: { watch: () => ({ ready: Promise.resolve(), close: closeWatch }), watchAppends } }, report)
  const request = () => new Request('https://fixture.test/v1/stream/a?live=sse&offset=now')
  const response = await live.serve(request(), 'tenant', 'a', 'now')
  const reader = response.body!.getReader()
  try {
    await reader.read(); await entered.promise
    await reader.cancel()
    expect(closeWatch).toHaveBeenCalledOnce()
    expect(closeAppends).toHaveBeenCalledOnce()
    expect(releaseLease).not.toHaveBeenCalled()
    await expect(live.serve(request(), 'tenant', 'a', 'now')).rejects.toMatchObject({ code: 'LIVE_LIMIT' })
    resume.resolve(); await live.drain()
    expect(releaseLease).toHaveBeenCalledOnce()
    expect(watchAppends).toHaveBeenCalledOnce()
    expect(closeAppends).toHaveBeenCalledOnce()
    const next = await live.serve(request(), 'tenant', 'a', 'now')
    await next.body!.cancel()
    await live.drain()
    expect(report).not.toHaveBeenCalled()
  } finally { resume.resolve(); await reader.cancel(); live.close(); await live.drain() }
})
