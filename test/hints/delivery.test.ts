import { randomUUID, createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'

const env = process.env
if (!env.TEST_FUNCTION_URL || !env.TEST_FUNCTION_API_KEY || !env.TEST_FUNCTION_SCHEMA || !env.TEST_DATABASE_URL || !env.NEON_API_KEY || !env.NEON_PROJECT_ID
  || !env.NEON_TEST_BRANCH_ID || env.TEST_FUNCTION_BRANCH_ID !== env.NEON_TEST_BRANCH_ID
  || env.TEST_FUNCTION_MAINTENANCE_HINTS !== 'on' || env.TEST_FUNCTION_HAS_CRON !== 'no'
  || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA)) throw new Error('Hint qualification requires a recorded isolated Function with hints enabled and no cron')
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8')) as { projectId: string; branchId: string; name: string }
if (state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.NEON_TEST_BRANCH_ID || !state.name.startsWith('ds-test-')) throw new Error('Unrecorded hint qualification branch')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 2, query_timeout: 10_000 })
pool.on('error', () => {})
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true })
const request = (path: string, method = 'GET', body?: BodyInit, headers?: Record<string, string>, signal = AbortSignal.timeout(20_000)) => fetch(new URL(path, env.TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, 'Content-Type': 'application/octet-stream', ...headers }, ...(body !== undefined ? { body } : {}), signal,
})
beforeAll(async () => {
  const triggers = await neon.triggers.list({ projectId: env.NEON_PROJECT_ID!, branchId: env.NEON_TEST_BRANCH_ID! })
  expect(triggers).toHaveLength(0)
})
afterAll(async () => {
  try {
    const names = await pool.query<{ name: string }>(`SELECT name FROM "${env.TEST_FUNCTION_SCHEMA}".subscriptions WHERE tenant_id='function-a' AND name LIKE 'hint-%' AND deleted_at IS NULL`)
    for (const row of names.rows) await request(`v1/stream/__ds/subscriptions/${encodeURIComponent(row.name)}`, 'DELETE')
  } finally { await pool.end() }
})

test('an append hint publishes a pull wake through actual Realtime with no cron or manual worker', async () => {
  const name = `hint-${randomUUID()}`, path = `${name}/events`, wake = `${name}-wake`
  expect((await request(`v1/stream/${path}`, 'PUT')).status).toBe(201)
  expect((await request(`v1/stream/${wake}`, 'PUT', '[]', { 'Content-Type': 'application/json' })).status).toBe(201)
  expect((await request(`v1/stream/__ds/subscriptions/${name}`, 'PUT', JSON.stringify({ type: 'pull-wake', streams: [path], wake_stream: wake }), { 'Content-Type': 'application/json' })).status).toBe(201)
  const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 12_000)
  const response = await request(`v1/stream/${wake}?live=sse&offset=now`, 'GET', undefined, undefined, controller.signal)
  expect(response.status).toBe(200)
  const reader = response.body!.getReader()
  try {
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('event: control')
    expect((await request(`v1/stream/${path}`, 'POST', 'hinted append')).status).toBe(204)
    let content = ''
    while (!content.includes('"type":"wake"')) {
      const next = await reader.read()
      if (next.done) throw new Error('Wake SSE ended before hint publication')
      content += new TextDecoder().decode(next.value)
    }
    expect(content).toContain(`"subscription_id":"${name}"`)
  } finally {
    clearTimeout(timer); await reader.cancel().catch(() => {}); controller.abort(); reader.releaseLock()
    await request(`v1/stream/__ds/subscriptions/${name}`, 'DELETE')
  }
}, 20_000)

test('hints seal a full chunk while small hot tails remain local, without a scheduled worker', async () => {
  const path = `hint-storage-${randomUUID()}`, payload = new Uint8Array(256 * 1024).fill(97)
  expect((await request(`v1/stream/${path}`, 'PUT')).status).toBe(201)
  for (let i = 0; i < 8; i++) expect((await request(`v1/stream/${path}`, 'POST', payload)).status).toBe(204)
  const pending = await pool.query(`SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".chunks c JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id=c.stream_id WHERE s.path=$1`, [path])
  expect(pending.rows[0].count).toBe('0')
  for (let i = 0; i < 8; i++) expect((await request(`v1/stream/${path}`, 'POST', payload)).status).toBe(204)
  await expect.poll(async () => (await pool.query(`SELECT bool_and(a.payload IS NULL) AS sealed FROM "${env.TEST_FUNCTION_SCHEMA}".appends a JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id=a.stream_id WHERE s.path=$1`, [path])).rows[0].sealed,
  { timeout: 15_000, interval: 250 }).toBe(true)
  const hash = createHash('sha256')
  let bytes = 0, offset = '-1'
  for (let i = 0; i < 20; i++) {
    const page = await request(`v1/stream/${path}?offset=${encodeURIComponent(offset)}`)
    expect(page.status).toBe(200)
    const part = Buffer.from(await page.arrayBuffer()); bytes += part.length; hash.update(part)
    if (page.headers.get('Stream-Up-To-Date')) break
    offset = page.headers.get('Stream-Next-Offset')!
  }
  expect(bytes).toBe(4 * 1024 * 1024)
  expect(hash.digest('hex')).toBe(createHash('sha256').update(Buffer.alloc(4 * 1024 * 1024, 97)).digest('hex'))
})
