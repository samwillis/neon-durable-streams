import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, expect, test } from 'vitest'

const { TEST_FUNCTION_URL, TEST_FUNCTION_API_KEY, TEST_FUNCTION_OTHER_KEY, TEST_FUNCTION_MAINTENANCE_KEY, TEST_FUNCTION_SCHEMA, TEST_FUNCTION_BUNDLE_SHA, TEST_DATABASE_URL } = process.env
if (!TEST_FUNCTION_URL || !TEST_FUNCTION_API_KEY || !TEST_FUNCTION_OTHER_KEY || !TEST_FUNCTION_MAINTENANCE_KEY || !TEST_FUNCTION_SCHEMA || !TEST_FUNCTION_BUNDLE_SHA || !TEST_DATABASE_URL) throw new Error('Deployed Neon Function credentials required; ingress qualification cannot skip')
if (!/^ds_test_[a-f0-9]{32}$/.test(TEST_FUNCTION_SCHEMA) || !new URL(TEST_FUNCTION_URL).hostname.includes('.compute.')) throw new Error('Only the provisioned isolated Neon Function can be tested')
const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 2 })
pool.on('error', () => {})
afterAll(() => pool.end())
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>, signal?: AbortSignal) => fetch(new URL(path, TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${TEST_FUNCTION_API_KEY}`, 'Content-Type': 'text/plain', ...headers },
  ...(body !== undefined ? { body } : {}), signal: signal ?? AbortSignal.timeout(15_000),
})

test('authenticated health identifies the deployed bundle and unauthorized calls fail before application work', async () => {
  const health = await request('health')
  expect(health.status).toBe(200)
  expect(await health.json()).toMatchObject({ ready: true, build: TEST_FUNCTION_BUNDLE_SHA })
  expect((await request('health', 'GET', undefined, { Authorization: 'Bearer invalid' })).status).toBe(401)
  expect((await request(`v1/stream/unauthorized-${randomUUID()}`, 'PUT', '{bad json', { Authorization: 'Bearer invalid', 'Content-Type': 'application/json' })).status).toBe(401)
  expect((await request('_maintenance', 'POST')).status).toBe(401)
})

test('a caller cannot impersonate a Neon Function Trigger using forged platform headers', async () => {
  const id = randomUUID()
  const response = await request('_trigger', 'POST', JSON.stringify({ version: 1, invocation_id: id, type: 'schedule',
    trigger: { type: 'schedule', id: 'forged', name: 'durable-streams-maintenance' }, data: { scheduled_at: new Date().toISOString() } }),
  { 'x-neon-trigger-invocation-id': id, 'Content-Type': 'application/json' })
  expect(response.status).toBe(401)
})

test('the same path is isolated between independently authenticated tenants at real Function ingress', async () => {
  const path = `v1/stream/tenant-${randomUUID()}`
  expect((await request(path, 'PUT', 'tenant-a bytes')).status).toBe(201)
  const other = { Authorization: `Bearer ${TEST_FUNCTION_OTHER_KEY}` }
  expect((await request(path, 'GET', undefined, other)).status).toBe(404)
  expect((await request(path, 'PUT', 'tenant-b bytes', other)).status).toBe(201)
  expect(await (await request(path)).text()).toBe('tenant-a bytes')
  expect(await (await request(path, 'GET', undefined, other)).text()).toBe('tenant-b bytes')
})

test('SSE flushes its initial checkpoint and delivers a live final append before EOF through Neon ingress', async () => {
  const path = `v1/stream/sse-${randomUUID()}`
  const created = await request(path, 'PUT', 'history')
  expect(created.status).toBe(201)
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 12_000)
  const response = await request(`${path}?live=sse&offset=${created.headers.get('Stream-Next-Offset')}`, 'GET', undefined, undefined, controller.signal)
  expect(response.status).toBe(200)
  expect(response.headers.get('Content-Type')).toBe('text/event-stream')
  const reader = response.body!.getReader()
  try {
    let initial = ''
    while (!initial.includes('event: control')) {
      const part = await reader.read()
      if (part.done) throw new Error('SSE ended before its initial checkpoint')
      initial += new TextDecoder().decode(part.value)
    }
    expect(initial).not.toContain('history')
    expect((await request(path, 'POST', 'final bytes', { 'Stream-Closed': 'true' })).status).toBe(204)
    let content = ''
    for (;;) {
      const part = await reader.read()
      if (part.done) break
      content += new TextDecoder().decode(part.value)
    }
    expect(content).toContain('event: data\ndata:final bytes\n\nevent: control')
    expect(content).toContain('"streamClosed":true')
  } finally { clearTimeout(timer); controller.abort(); await reader.cancel(); reader.releaseLock() }
}, 15_000)

test('authenticated maintenance publishes to Neon Object Storage and the Function replays sealed data', async () => {
  const path = `sealed-${randomUUID()}`
  expect((await request(`v1/stream/${path}`, 'PUT', 'before')).status).toBe(201)
  const tuple = { 'Producer-Id': 'ingress-writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  expect((await request(`v1/stream/${path}`, 'POST', ' after', tuple)).status).toBe(200)
  const maintenance = await request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': TEST_FUNCTION_MAINTENANCE_KEY })
  expect(maintenance.status).toBe(200)
  await maintenance.json() // The schedule may already have sealed this batch.
  const remote = await pool.query(`SELECT bool_and(a.payload IS NULL) AS sealed FROM "${TEST_FUNCTION_SCHEMA}".appends a JOIN "${TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE s.tenant_id = 'function-a' AND s.path = $1`, [path])
  expect(remote.rows[0].sealed).toBe(true)
  expect(await (await request(`v1/stream/${path}`)).text()).toBe('before after')
  expect((await request(`v1/stream/${path}`, 'POST', 'a retried body', tuple)).status).toBe(204)
  expect(await (await request(`v1/stream/${path}`)).text()).toBe('before after')
})

test('a real Neon schedule trigger seals idle data without an application maintenance call', async () => {
  const path = `scheduled-${randomUUID()}`
  expect((await request(`v1/stream/${path}`, 'PUT', 'idle bytes awaiting scheduled recovery')).status).toBe(201)
  await expect.poll(async () => {
    const result = await pool.query(`SELECT bool_and(a.payload IS NULL) AS sealed FROM "${TEST_FUNCTION_SCHEMA}".appends a
      JOIN "${TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE s.tenant_id = 'function-a' AND s.path = $1`, [path])
    return result.rows[0].sealed
  }, { timeout: 90_000, interval: 1000 }).toBe(true)
  expect(await (await request(`v1/stream/${path}`)).text()).toBe('idle bytes awaiting scheduled recovery')
}, 120_000)
