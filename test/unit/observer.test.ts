import { expect, test } from 'vitest'
import { observe } from '../../src/observer.js'
import pg from 'pg'
import { createDurableStreams } from '../../src/index.js'

test('throwing and rejecting observers cannot affect results or escape unhandled', async () => {
  const errors: unknown[] = []
  observe(() => { throw new Error('sync') }, null, (error) => { errors.push(error) })
  observe(async () => { throw new Error('async') }, null, async (error) => { errors.push(error); throw new Error('reporter failed') })
  observe(async () => { throw new Error('unreported') }, null)
  await new Promise<void>((resolve) => setImmediate(resolve))
  expect(errors.map((error) => (error as Error).message)).toEqual(['sync', 'async'])
})

test('request telemetry reports bounded metadata and rejecting hooks preserve the response', async () => {
  const pool = new pg.Pool()
  const events: unknown[] = []
  const server = createDurableStreams({ postgres: { pool }, auth: { authenticate: () => null, authorize: () => false },
    async onEvent(event) { events.push(event); throw new Error('observer unavailable') }, async onError() { throw new Error('reporter unavailable') } })
  try {
    const response = await server.fetch(new Request('https://example.test/v1/stream/private-name', { headers: { Authorization: 'Bearer private-key' } }))
    expect(response.status).toBe(401)
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(events).toEqual([{ type: 'request', method: 'GET', status: 401, durationMs: expect.any(Number) }])
  } finally { await server.close(); await pool.end() }
})
