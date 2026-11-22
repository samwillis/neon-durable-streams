import { randomUUID, createPublicKey, verify } from 'node:crypto'
import type { JsonWebKey } from 'node:crypto'
import pg from 'pg'
import { afterAll, expect, test } from 'vitest'

const env = process.env
if (!env.TEST_FUNCTION_URL || !env.TEST_FUNCTION_API_KEY || !env.TEST_FUNCTION_MAINTENANCE_KEY || !env.TEST_FUNCTION_SCHEMA
  || !env.TEST_WEBHOOK_RECEIVER_URL || !env.TEST_WEBHOOK_RECEIVER_KEY || !env.TEST_DATABASE_URL) throw new Error('Deployed Neon source and public HTTPS receiver required')
if (!/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA) || !new URL(env.TEST_WEBHOOK_RECEIVER_URL).hostname.includes('.compute.')) throw new Error('Webhook qualification requires recorded Neon Functions')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 2 })
pool.on('error', () => {})
afterAll(() => pool.end())
const request = (path: string, method = 'GET', body?: object | string, headers?: Record<string, string>) => fetch(new URL(path, env.TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, 'Content-Type': typeof body === 'object' ? 'application/json' : 'text/plain', ...headers },
  ...(body !== undefined ? { body: typeof body === 'string' ? body : JSON.stringify(body) } : {}), signal: AbortSignal.timeout(20_000),
})
interface Delivery {
  wake_id: string; raw_body: string; signature: string; attempts: number
  payload: { subscription_id: string; tenant_id: string; wake_id: string; generation: number; callback_url: string; callback_token: string;
    streams: { path: string; tail_offset: string }[] }
}
async function received(name: string): Promise<Delivery[]> {
  const url = new URL('deliveries', env.TEST_WEBHOOK_RECEIVER_URL); url.searchParams.set('subscription_id', name)
  const response = await fetch(url, { headers: { Authorization: env.TEST_WEBHOOK_RECEIVER_KEY! }, signal: AbortSignal.timeout(10_000) })
  expect(response.status).toBe(200)
  const deliveries = await response.json() as Delivery[]
  if (env.GITHUB_ACTIONS === 'true') for (const delivery of deliveries) console.log(`::add-mask::${delivery.payload.callback_token}`)
  return deliveries
}
const maintain = () => request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY! })
async function setup(mode: string) {
  const name = `https-${mode}-${randomUUID()}`, path = `webhook/${name}`
  expect((await request(`v1/stream/${path}`, 'PUT', 'before')).status).toBe(201)
  const registration = await request(`v1/stream/__ds/subscriptions/${name}`, 'PUT', { type: 'webhook', streams: [path],
    webhook: { url: new URL(mode, env.TEST_WEBHOOK_RECEIVER_URL).href }, lease_ttl_ms: 30_000 })
  expect(registration.status).toBe(201)
  const appended = await request(`v1/stream/${path}`, 'POST', ' new HTTPS data')
  expect(appended.status).toBe(204)
  return { name, path, tail: appended.headers.get('Stream-Next-Offset')! }
}
async function acknowledged(name: string, tail: string) {
  const result = await pool.query(`SELECT m.acked_offset FROM "${env.TEST_FUNCTION_SCHEMA}".subscription_members m JOIN "${env.TEST_FUNCTION_SCHEMA}".subscriptions s ON s.id = m.subscription_id WHERE s.tenant_id = 'function-a' AND s.name = $1`, [name])
  return result.rows[0]?.acked_offset === tail
}

test('deployed Function signs real public HTTPS wakes and synchronous done acknowledges their snapshot', async () => {
  expect((await fetch(new URL('sync', env.TEST_WEBHOOK_RECEIVER_URL), { method: 'POST', body: '{}', signal: AbortSignal.timeout(10_000) })).status).toBe(401)
  const { name, path, tail } = await setup('sync')
  try {
    expect((await maintain()).status).toBe(200)
    await expect.poll(async () => (await received(name)).length, { timeout: 15_000, interval: 250 }).toBe(1)
    const [delivery] = await received(name)
    expect(delivery!.payload.subscription_id).toBe(name)
    expect(delivery!.payload.tenant_id).toBe('function-a')
    expect(delivery!.payload.streams[0]!.path).toBe(path)
    expect(delivery!.payload.streams[0]!.tail_offset).toBe(tail)
    const fields = Object.fromEntries(delivery!.signature.split(',').map((field) => field.split('=')))
    const jwks = await (await request('v1/stream/__ds/jwks.json')).json() as { keys: (JsonWebKey & { kid: string })[] }
    const key = jwks.keys.find((key) => key.kid === fields.kid)!
    expect(verify(null, Buffer.concat([Buffer.from(`${fields.t}.`), Buffer.from(delivery!.raw_body, 'base64')]), createPublicKey({ key, format: 'jwk' }), Buffer.from(fields.ed25519!, 'base64url'))).toBe(true)
    await expect.poll(() => acknowledged(name, tail), { timeout: 5000, interval: 100 }).toBe(true)
  } finally { await request(`v1/stream/__ds/subscriptions/${name}`, 'DELETE') }
})

test('public HTTPS asynchronous callback uses its scoped token and fences duplicate completion', async () => {
  const { name, path, tail } = await setup('async')
  try {
    expect((await maintain()).status).toBe(200)
    await expect.poll(async () => (await received(name)).length, { timeout: 15_000, interval: 250 }).toBe(1)
    const { payload } = (await received(name))[0]!
    expect(payload.callback_url).toBe(new URL(`v1/stream/__ds/subscriptions/${name}/callback`, env.TEST_FUNCTION_URL).href)
    expect((await request(`v1/stream/${path}`, 'GET', undefined, { Authorization: `Bearer ${payload.callback_token}` })).status).toBe(401)
    expect(await acknowledged(name, tail)).toBe(false)
    const callback = () => request(`v1/stream/__ds/subscriptions/${name}/callback`, 'POST', {
      wake_id: payload.wake_id, generation: payload.generation, done: true, acks: [{ stream: path, offset: tail }],
    }, { Authorization: `Bearer ${payload.callback_token}` })
    expect((await callback()).status).toBe(200)
    expect(await acknowledged(name, tail)).toBe(true)
    expect((await callback()).status).toBe(409)
  } finally { await request(`v1/stream/__ds/subscriptions/${name}`, 'DELETE') }
})

test('public HTTPS 500 responses persist retry state and retry the same wake', async () => {
  const { name, tail } = await setup('retry')
  try {
    await maintain()
    await expect.poll(async () => {
      await maintain()
      return (await received(name))[0]?.attempts ?? 0
    }, { timeout: 40_000, interval: 1000 }).toBeGreaterThanOrEqual(2)
    const deliveries = await received(name)
    expect(deliveries).toHaveLength(1)
    await expect.poll(() => acknowledged(name, tail), { timeout: 5000, interval: 100 }).toBe(true)
  } finally { await request(`v1/stream/__ds/subscriptions/${name}`, 'DELETE') }
}, 55_000)
