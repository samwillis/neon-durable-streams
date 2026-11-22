import { postgresTestLimits } from '../fixtures/postgres-limits.js'
import { randomInt, randomUUID, createPublicKey, verify } from 'node:crypto'
import { createServer } from 'node:http'
import { once } from 'node:events'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { AuthorizationContext, DurableStreamsServer } from '../../src/index.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../../src/subscriptions.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { createNeonRealtimeNotifier } from '../../src/neon.js'
import { migrate } from '../../src/migrations.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'
import { transaction } from '../../src/transaction.js'

const env = process.env
if (!env.TEST_DATABASE_URL || !env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET || !env.DURABLE_STREAMS_KEYRING || !env.NEON_REALTIME_URL || !env.NEON_REALTIME_SECRET) throw new Error('Subscription qualification requires real Neon data services and wrapping keys')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 12 })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
direct.on('error', () => {})
const keyring = parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING)
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const reported: unknown[] = []
const notifier = createNeonRealtimeNotifier({ url: env.NEON_REALTIME_URL, secret: env.NEON_REALTIME_SECRET, database: new URL(env.TEST_DATABASE_URL).pathname.slice(1) })
const servers: DurableStreamsServer[] = []
const subscriptions = new Set<string>()
const server = (maxStreams = 1000, authorize: (context: AuthorizationContext) => boolean = () => true) => {
  const handler = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema }, storage: { objects, targetChunkBytes: 32, maxChunkFrames: 8 },
    live: { notifier, fallbackIntervalMs: 60_000 }, subscriptions: { keyring, allowLocalhostWebhooks: true, maxStreams },
    auth: { authenticate: (request) => { const key = request.headers.get('authorization'); return key === 'Bearer a' || key === 'Bearer b' ? { tenantId: key.slice(-1), subject: 'fixture' } : null }, authorize },
    onError: (error) => reported.push(error) })
  servers.push(handler)
  return handler
}
const handler = server()
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>, target = handler) => target.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { Authorization: 'Bearer a', 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
const subPath = (name: string, action = '') => `__ds/subscriptions/${name}${action ? `/${action}` : ''}`
async function register(name: string, config: object, target = handler) {
  const response = await request(subPath(name), 'PUT', JSON.stringify(config), { 'Content-Type': 'application/json' }, target)
  expect(response.status).toBe(201)
  subscriptions.add(name)
  return response.json() as Promise<{ webhook?: { signing: { kid: string; jwks_url: string } } }>
}
async function createPull(name: string, pattern: string, lease = 30_000, target = handler) {
  const wake = `wake/${name}`
  expect((await request(wake, 'PUT', '[]', { 'Content-Type': 'application/json' }, target)).status).toBe(201)
  await register(name, { type: 'pull-wake', pattern, wake_stream: wake, lease_ttl_ms: lease }, target)
  return wake
}
const maintain = (target = handler) => target.maintain({ maxChunks: 1, maxSubscriptionWork: 64 })
interface Claim { wake_id: string; generation: number; token: string; streams: { path: string; acked_offset: string; tail_offset: string; has_pending: boolean }[] }
async function claim(name: string, worker = 'worker', target = handler): Promise<Claim> {
  const response = await request(subPath(name, 'claim'), 'POST', JSON.stringify({ worker }), { 'Content-Type': 'application/json' }, target)
  expect(response.status).toBe(200)
  return response.json() as Promise<Claim>
}
const ack = (name: string, value: Claim, acks: readonly { stream: string; offset: string }[], done = true, target = handler) => request(subPath(name, 'ack'), 'POST', JSON.stringify({ wake_id: value.wake_id, generation: value.generation, acks, done }), { 'Content-Type': 'application/json', Authorization: `Bearer ${value.token}` }, target)
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>((accept) => { resolve = accept }); return { promise, resolve } }
async function poll(check: () => Promise<boolean>, timeout = 5000) {
  const deadline = Date.now() + timeout
  while (!await check()) { if (Date.now() >= deadline) throw new Error('Timed out at the SQL concurrency barrier'); await new Promise((resolve) => setTimeout(resolve, 25)) }
}
interface Notification { raw: Buffer; signature: string; body: { subscription_id: string; wake_id: string; generation: number; callback_token: string; callback_url: string; streams: { path: string; acked_offset: string; tail_offset: string }[] } }
async function receiver(response: (notification: Notification, index: number) => Promise<{ status?: number; done?: boolean }> = async () => ({ done: true })) {
  const received: Notification[] = []
  const http = createServer(async (request, outgoing) => {
    const chunks: Buffer[] = []
    for await (const chunk of request) chunks.push(Buffer.from(chunk as Uint8Array))
    const raw = Buffer.concat(chunks)
    const notification: Notification = { raw, signature: String(request.headers['webhook-signature']), body: JSON.parse(raw.toString('utf8')) as Notification['body'] }
    const index = received.push(notification) - 1
    const result = await response(notification, index)
    outgoing.writeHead(result.status ?? 200, { 'Content-Type': 'application/json' })
    outgoing.end(JSON.stringify({ done: result.done ?? false }))
  })
  http.listen(0, '127.0.0.1')
  await once(http, 'listening')
  const address = http.address()
  if (!address || typeof address === 'string') throw new Error('Missing webhook fixture address')
  return { url: `http://127.0.0.1:${address.port}/hook`, received,
    close: async () => { http.closeAllConnections(); await new Promise<void>((resolve, reject) => http.close((error) => error ? reject(error) : resolve())) } }
}
async function remove(name: string) {
  expect((await request(subPath(name), 'DELETE')).status).toBe(204)
  subscriptions.delete(name)
}

beforeAll(async () => { await migrate(direct, { schema, storageIdentity: objects.identity }); await initializeSubscriptions(direct, { schema, keyring }) })
afterAll(async () => {
  for (const name of subscriptions) await request(subPath(name), 'DELETE')
  await Promise.all(servers.map((value) => value.close()))
  notifier.close(); objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end(); await direct.end()
  expect(reported).toEqual([])
})

test('one bounded dispatch coalesces committed appends without consuming intent beyond its cutoff', async () => {
  const name = 'coalesced-prefix', path = 'coalesced/source'
  await request(path, 'PUT')
  await createPull(name, 'coalesced/*')
  await maintain()
  let tail = ''
  for (let i = 0; i < 12; i++) tail = (await request(path, 'POST', String(i))).headers.get('Stream-Next-Offset')!
  const key = randomInt(1, 2_000_000_000), gate = await direct.connect()
  await gate.query('SELECT pg_advisory_lock($1::bigint)', [key])
  await direct.query(`CREATE FUNCTION "${schema}".pause_coalesced() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name = 'coalesced-prefix' THEN PERFORM pg_advisory_xact_lock(${key}::bigint); END IF; RETURN NEW; END $$`)
  await direct.query(`CREATE TRIGGER pause_coalesced AFTER UPDATE ON "${schema}".subscriptions FOR EACH ROW EXECUTE FUNCTION "${schema}".pause_coalesced()`)
  const work = handler.maintain({ maxChunks: 1, maxSubscriptionWork: 1 })
  void work.catch(() => {})
  try {
    await poll(async () => Boolean((await pool.query(`SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::oid AND NOT granted`, [key])).rowCount))
    tail = (await request(path, 'POST', 'after cutoff')).headers.get('Stream-Next-Offset')!
  } finally { await gate.query('SELECT pg_advisory_unlock($1::bigint)', [key]); gate.release() }
  // Remove the barrier before the same bounded maintenance pass prepares a
  // wake. The unlocked trigger is harmless while the first pass finishes.
  const stats = await work
  await direct.query(`DROP TRIGGER pause_coalesced ON "${schema}".subscriptions`)
  await direct.query(`DROP FUNCTION "${schema}".pause_coalesced()`)
  expect(stats.changes).toBe(12)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".stream_changes c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = $1 AND NOT c.processed`, [path])).rows[0].count).toBe('1')
  await maintain()
  const value = await claim(name)
  expect(value.streams.find((stream) => stream.path === path)?.tail_offset).toBe(tail)
  expect((await ack(name, value, [{ stream: path, offset: tail }])).status).toBe(200)
  await remove(name)
})

test('resumed multi-batch fan-out preserves appends needed by a newly registered lower UUID', async () => {
  const name = 'fanout-cursor', path = 'fanout/source'
  await request(path, 'PUT')
  await createPull(name, 'fanout/*')
  await maintain()
  // Equivalent empty-tail registrations, seeded in one fixture transaction so
  // this test exercises cursor recovery rather than 17 repetitions of setup.
  await pool.query(`INSERT INTO "${schema}".subscriptions (id, tenant_id, name, config, config_hash, pattern_regex, public_base_url)
    SELECT gen_random_uuid(), s.tenant_id, s.name || '-' || n, s.config, s.config_hash, s.pattern_regex, s.public_base_url
    FROM "${schema}".subscriptions s CROSS JOIN generate_series(1,16) n WHERE s.name = $1`, [name])
  try {
    await request(path, 'POST', 'one')
    const baseline = (await request(path, 'POST', 'two')).headers.get('Stream-Next-Offset')!
    await handler.maintain({ maxChunks: 1, maxSubscriptionWork: 1 })
    const pending = () => pool.query<{ count: string }>(`SELECT count(*) FROM "${schema}".stream_changes c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = $1 AND NOT c.processed`, [path])
    expect((await pending()).rows[0]!.count).toBe('2')
    const lateId = '00000000-0000-4000-8000-000000000001'
    const registryKey = `durable-streams:subscription-registry:"${schema}":a`
    await transaction(pool, {}, async (client) => {
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [registryKey])
      await client.query(`INSERT INTO "${schema}".subscriptions (id, tenant_id, name, config, config_hash, pattern_regex, public_base_url, member_count)
        SELECT $2, tenant_id, name || '-late', config, config_hash, pattern_regex, public_base_url, 1 FROM "${schema}".subscriptions WHERE name = $1`, [name, lateId])
      await client.query(`INSERT INTO "${schema}".subscription_members (subscription_id,tenant_id,path,stream_id,glob_link,acked_offset)
        SELECT $1, tenant_id,path,id,true,$2 FROM "${schema}".streams WHERE path = $3`, [lateId, baseline, path])
    })
    const tail = (await request(path, 'POST', 'three')).headers.get('Stream-Next-Offset')!
    const fresh = server()
    await fresh.maintain({ maxChunks: 1, maxSubscriptionWork: 1 })
    // Completing the older partial fan-out must not consume either later
    // record: the late UUID was deliberately below the persisted cursor.
    expect((await pending()).rows[0]!.count).toBe('2')
    for (let i = 0; i < 5; i++) await fresh.maintain({ maxChunks: 1, maxSubscriptionWork: 1 })
    expect((await pending()).rows[0]!.count).toBe('0')
    const late = await pool.query(`SELECT m.acked_offset, s.next_work_at IS NOT NULL OR s.current_wake IS NOT NULL AS scheduled
      FROM "${schema}".subscriptions s JOIN "${schema}".subscription_members m ON m.subscription_id = s.id WHERE s.id = $1`, [lateId])
    expect(late.rows).toEqual([{ acked_offset: baseline, scheduled: true }])
    expect(tail).not.toBe(baseline)
  } finally {
    await pool.query(`UPDATE "${schema}".subscriptions SET deleted_at = clock_timestamp() WHERE name = $1 OR name LIKE $2`, [name, `${name}-%`])
    subscriptions.delete(name)
  }
}, 180_000)

test('registration backfill cannot be overtaken by dispatch when an append commits before registration does', async () => {
  const source = 'barrier/x', name = 'registration-barrier'
  await request(source, 'PUT', 'old')
  const before = (await request(source, 'HEAD')).headers.get('Stream-Next-Offset')!
  await maintain()
  const gateKey = randomInt(1, 2_000_000_000)
  const gate = await direct.connect()
  await gate.query('SELECT pg_advisory_lock($1::bigint)', [gateKey])
  await direct.query(`CREATE FUNCTION "${schema}".pause_backfill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.path = 'barrier/x' THEN PERFORM pg_advisory_xact_lock(${gateKey}::bigint); END IF; RETURN NEW; END $$`)
  await direct.query(`CREATE TRIGGER pause_backfill AFTER INSERT ON "${schema}".subscription_members FOR EACH ROW EXECUTE FUNCTION "${schema}".pause_backfill()`)
  let registration: Promise<unknown> | undefined, dispatch: Promise<unknown> | undefined
  try {
    registration = createPull(name, 'barrier/*')
    void registration.catch(() => {})
    await poll(async () => Boolean((await pool.query(`SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::oid AND NOT granted`, [gateKey])).rowCount))
    const append = await request(source, 'POST', ' new')
    expect(append.status).toBe(204)
    dispatch = maintain()
    void dispatch.catch(() => {})
    const registry = (await pool.query<{ key: string }>('SELECT hashtextextended($1,0) AS key', [`durable-streams:subscription-registry:"${schema}":a`])).rows[0]!.key
    const unsigned = BigInt.asUintN(64, BigInt(registry))
    await poll(async () => Boolean((await pool.query(`SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = $1::oid AND objid = $2::oid AND mode = 'ShareLock' AND NOT granted`, [String(unsigned >> 32n), String(unsigned & 0xffffffffn)])).rowCount))
    expect((await pool.query(`SELECT c.processed FROM "${schema}".stream_changes c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = $1 ORDER BY c.version DESC LIMIT 1`, [source])).rows[0].processed).toBe(false)
  } finally {
    await gate.query('SELECT pg_advisory_unlock($1::bigint)', [gateKey]); gate.release()
    await registration; await dispatch
    await direct.query(`DROP TRIGGER pause_backfill ON "${schema}".subscription_members`)
    await direct.query(`DROP FUNCTION "${schema}".pause_backfill()`)
  }
  await maintain()
  const value = await claim(name)
  expect(value.streams.find((stream) => stream.path === source)).toMatchObject({ acked_offset: before, has_pending: true })
  expect(await (await request(source)).text()).toBe('old new')
  await remove(name)
})

test('a late-committing earlier change is delivered after a later change was already processed', async () => {
  const name = 'commit-order'
  await request('ordering/a', 'PUT')
  await request('ordering/b', 'PUT')
  await createPull(name, 'ordering/*')
  await maintain()
  const entered = deferred(), resume = deferred()
  const late = transaction(pool, { timeoutMs: 60_000 }, async (client) => {
    const stream = (await client.query<{ id: string; tail_offset: string; change_version: string }>(`SELECT * FROM "${schema}".streams WHERE tenant_id = 'a' AND path = 'ordering/a' FOR NO KEY UPDATE`)).rows[0]!
    const payload = Buffer.from('late commit'), next = advanceOffset(stream.tail_offset, payload.length)
    await client.query(`INSERT INTO "${schema}".appends (stream_id,start_offset,end_offset,payload,payload_bytes) VALUES ($1,$2,$3,$4,$5)`, [stream.id, stream.tail_offset, next, payload, payload.length])
    await client.query(`UPDATE "${schema}".streams SET tail_offset = $2, change_version = change_version + 1, hot_frames = hot_frames + 1, hot_bytes = hot_bytes + $3 WHERE id = $1`, [stream.id, next, payload.length])
    await client.query(`INSERT INTO "${schema}".stream_changes (stream_id,version,before_offset,after_offset,kind) VALUES ($1,$2,$3,$4,'appended')`, [stream.id, String(BigInt(stream.change_version) + 1n), stream.tail_offset, next])
    entered.resolve(); await resume.promise
  })
  try {
    await entered.promise
    const later = await request('ordering/b', 'POST', 'commits first')
    expect(later.status).toBe(204)
    await maintain()
    const first = await claim(name)
    expect(first.streams.find((stream) => stream.path === 'ordering/a')?.has_pending).toBe(false)
    expect((await ack(name, first, [{ stream: 'ordering/b', offset: later.headers.get('Stream-Next-Offset')! }])).status).toBe(200)
  } finally { resume.resolve(); await late }
  await maintain()
  const second = await claim(name)
  expect(second.generation).toBeGreaterThan(1)
  expect(second.streams.find((stream) => stream.path === 'ordering/a')).toMatchObject({ has_pending: true, acked_offset: ZERO_OFFSET })
  await remove(name)
})

test('synchronous webhook completion acks only its snapshot and wakes again for a concurrent append', async () => {
  const name = 'snapshot-webhook', received = deferred(), release = deferred()
  const webhook = await receiver(async (_notification, index) => { if (!index) { received.resolve(); await release.promise } return { done: true } })
  await register(name, { type: 'webhook', pattern: 'snapshot-events/*', webhook: { url: webhook.url } })
  const first = await request('snapshot-events/a', 'PUT', 'first')
  const delivery = maintain()
  let final: Response
  try {
    await received.promise
    final = await request('snapshot-events/a', 'POST', ' second')
  } finally { release.resolve() }
  await delivery
  await maintain()
  expect(webhook.received).toHaveLength(2)
  const next = webhook.received[1]!.body
  expect(next.generation).toBeGreaterThan(webhook.received[0]!.body.generation)
  expect(next.streams.find((stream) => stream.path === 'snapshot-events/a')).toMatchObject({ acked_offset: first.headers.get('Stream-Next-Offset'), tail_offset: final!.headers.get('Stream-Next-Offset') })
  await remove(name); await webhook.close()
})

test('webhook retries honor a persisted deadline after a fresh handler and keep stable signing keys', async () => {
  const name = 'retry-state'
  const webhook = await receiver(async (_notification, index) => ({ status: index ? 200 : 500, done: Boolean(index) }))
  const created = await register(name, { type: 'webhook', pattern: 'retry-events/*', webhook: { url: webhook.url } })
  await request('retry-events/a', 'PUT', 'retry payload')
  await maintain()
  expect(webhook.received).toHaveLength(1)
  await pool.query(`UPDATE "${schema}".subscriptions SET retry_at = clock_timestamp() + interval '30 seconds', next_work_at = clock_timestamp() + interval '30 seconds' WHERE name = $1`, [name])
  const fresh = server()
  await maintain(fresh)
  expect(webhook.received).toHaveLength(1)
  const metadata = await (await request(subPath(name))).json() as { status: string }
  expect(metadata.status).toBe('failed')
  await pool.query(`UPDATE "${schema}".subscriptions SET retry_at = clock_timestamp() - interval '1 second', next_work_at = clock_timestamp() - interval '1 second' WHERE name = $1`, [name])
  await maintain(fresh)
  expect(webhook.received).toHaveLength(2)
  expect(webhook.received[1]!.body.callback_token).toBe(webhook.received[0]!.body.callback_token)
  expect(webhook.received[1]!.signature).toContain(`kid=${created.webhook!.signing.kid},`)
  const record = (await pool.query(`SELECT token_cipher FROM "${schema}".subscription_wakes w JOIN "${schema}".subscriptions s ON s.id = w.subscription_id WHERE s.name = $1`, [name])).rows[0]
  expect((record.token_cipher as Buffer).includes(Buffer.from(webhook.received[1]!.body.callback_token))).toBe(false)
  await remove(name); await webhook.close()
})

test('leases use database time, heartbeat, and generation fencing; invalid ack batches are atomic', async () => {
  const name = 'lease-fences'
  await createPull(name, 'leased/*', 5000)
  const first = await request('leased/a', 'PUT', 'a')
  await request('leased/b', 'PUT', 'b')
  await maintain()
  const value = await claim(name)
  expect((await request(subPath(name, 'ack'), 'POST', JSON.stringify({ wake_id: value.wake_id, generation: value.generation, done: true }), { 'Content-Type': 'application/json' })).status).toBe(401)
  expect((await request('leased/a', 'GET', undefined, { Authorization: `Bearer ${value.token}` })).status).toBe(401)
  expect((await ack(name, value, [{ stream: 'leased/a', offset: first.headers.get('Stream-Next-Offset')! }, { stream: 'leased/b', offset: '9999999999999999_9999999999999999' }])).status).toBe(409)
  const cursor = (await (await request(subPath(name))).json() as { streams: { acked_offset: string }[] }).streams
  expect(cursor.every((stream) => stream.acked_offset === ZERO_OFFSET)).toBe(true)
  const oldDeadline = (await pool.query(`SELECT lease_until FROM "${schema}".subscriptions WHERE name = $1`, [name])).rows[0].lease_until as Date
  expect((await ack(name, value, [], false)).status).toBe(200)
  const renewed = (await pool.query(`SELECT lease_until FROM "${schema}".subscriptions WHERE name = $1`, [name])).rows[0].lease_until as Date
  expect(renewed.getTime()).toBeGreaterThan(oldDeadline.getTime())
  await pool.query(`UPDATE "${schema}".subscriptions SET lease_until = clock_timestamp() - interval '1 second', next_work_at = clock_timestamp() - interval '1 second' WHERE name = $1`, [name])
  expect((await ack(name, value, [], true)).status).toBe(409)
  const replacement = await claim(name, 'replacement')
  expect(replacement.generation).toBeGreaterThan(value.generation)
  expect((await ack(name, value, [], true)).status).toBe(409)
  await remove(name)
  await register(name, { type: 'pull-wake', pattern: 'leased/*', wake_stream: `wake/${name}` })
  expect((await ack(name, replacement, [], true)).status).toBe(409)
  await remove(name)
})

test('quota-blocked glob discovery preserves the earliest cursor when capacity grows before the next append', async () => {
  const name = 'quota-recovery', limited = server(1)
  await createPull(name, 'quota-events/*', 30_000, limited)
  await request('quota-events/a', 'PUT', 'one')
  await maintain(limited)
  const first = await claim(name, 'first', limited)
  expect((await ack(name, first, first.streams.map((stream) => ({ stream: stream.path, offset: stream.tail_offset })), true, limited)).status).toBe(200)
  await request('quota-events/b', 'PUT', 'earliest')
  await maintain(limited)
  expect((await (await request(subPath(name), 'GET', undefined, undefined, limited)).json() as { status: string }).status).toBe('failed')
  await request('quota-events/b', 'POST', ' later')
  const expanded = server(2)
  await maintain(expanded)
  const recovered = await claim(name, 'expanded', expanded)
  expect(recovered.streams.find((stream) => stream.path === 'quota-events/b')).toMatchObject({ acked_offset: ZERO_OFFSET, has_pending: true })
  expect(await (await request('quota-events/b')).text()).toBe('earliest later')
  expect((await pool.query(`SELECT blocked_members FROM "${schema}".subscriptions WHERE name = $1`, [name])).rows[0].blocked_members).toBe(0)
  await remove(name)
})

test('pull-wake appends are observable through actual Neon Realtime', async () => {
  const name = 'realtime-wake'
  const wake = await createPull(name, 'rt-events/*')
  const response = await request(`${wake}?live=sse&offset=now`)
  expect(response.status).toBe(200)
  const reader = response.body!.getReader()
  try {
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('event: control')
    await request('rt-events/a', 'PUT', 'real wake')
    await maintain()
    let received = ''
    while (!received.includes('"type":"wake"')) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error('Wake SSE ended before the internal append')
      received += new TextDecoder().decode(chunk.value)
    }
    expect(received).toContain(`"subscription_id":"${name}"`)
  } finally { await reader.cancel(); reader.releaseLock() }
  await remove(name)
}, 45_000)

test('signing-key rotation preserves old JWKS verification while new deliveries use the new key', async () => {
  const name = 'signing-rotation'
  const webhook = await receiver()
  await register(name, { type: 'webhook', pattern: 'rotation-events/*', webhook: { url: webhook.url } })
  await request('rotation-events/a', 'PUT', 'first')
  await maintain()
  const old = webhook.received[0]!
  const next = await initializeSubscriptions(direct, { schema, keyring, rotateSigningKey: true })
  await request('rotation-events/a', 'POST', ' second')
  await maintain()
  expect(webhook.received[1]!.signature).toContain(`kid=${next.kid},`)
  const jwks = await (await request('__ds/jwks.json', 'GET', undefined, { Authorization: '' })).json() as { keys: { kid: string; kty: string; crv: string; x: string }[] }
  const parameters = Object.fromEntries(old.signature.split(',').map((part) => { const n = part.indexOf('='); return [part.slice(0, n), part.slice(n + 1)] }))
  const key = jwks.keys.find((key) => key.kid === parameters.kid)!
  expect(key).toBeDefined()
  expect(verify(null, Buffer.concat([Buffer.from(`${parameters.t}.`), old.raw]), createPublicKey({ key, format: 'jwk' }), Buffer.from(parameters.ed25519!, 'base64url'))).toBe(true)
  expect(jwks.keys.every((key) => !('d' in key))).toBe(true)
  await remove(name); await webhook.close()
})

test('acks validate inherited sealed frame boundaries before advancing a fork cursor', async () => {
  const name = 'ack-boundaries'
  await createPull(name, 'ack-child/*')
  const first = await request('ack-source', 'PUT', 'abc')
  await request('ack-source', 'POST', 'def')
  expect((await request('ack-child/a', 'PUT', undefined, { 'Stream-Forked-From': '/v1/stream/ack-source' })).status).toBe(201)
  await request('ack-source', 'DELETE')
  await maintain()
  const value = await claim(name)
  expect((await ack(name, value, [{ stream: 'ack-child/a', offset: advanceOffset(ZERO_OFFSET, 1) }])).status).toBe(409)
  expect((await ack(name, value, [{ stream: 'ack-child/a', offset: first.headers.get('Stream-Next-Offset')! }], false)).status).toBe(200)
  const body = await (await request(subPath(name))).json() as { streams: { acked_offset: string }[] }
  expect(body.streams[0]!.acked_offset).toBe(first.headers.get('Stream-Next-Offset'))
  await remove(name)
})

test('explicit and glob membership retain independent links and at-tail baselines', async () => {
  const name = 'membership-modes'
  const created = await request('member-events/a', 'PUT', 'history')
  await register(name, { type: 'webhook', pattern: 'member-events/*', streams: ['member-events/a'], webhook: { url: 'http://127.0.0.1:9/unused' } })
  expect((await request(subPath(name, `streams/${encodeURIComponent('member-events/a')}`), 'DELETE')).status).toBe(204)
  const glob = await (await request(subPath(name))).json() as { streams: { path: string; link_type: string; acked_offset: string }[] }
  expect(glob.streams).toEqual([{ path: 'member-events/a', link_type: 'glob', acked_offset: created.headers.get('Stream-Next-Offset') }])
  expect((await request(subPath(name, 'streams'), 'POST', JSON.stringify({ streams: ['member-events/a'] }), { 'Content-Type': 'application/json' })).status).toBe(204)
  const explicit = await (await request(subPath(name))).json() as { streams: { link_type: string; acked_offset: string }[] }
  expect(explicit.streams[0]).toMatchObject({ link_type: 'explicit', acked_offset: created.headers.get('Stream-Next-Offset') })
  expect((await request(subPath(name), 'GET', undefined, { Authorization: 'Bearer b' })).status).toBe(404)
  await remove(name)
})

test('authorization sees the normalized selector and destination and can deny wake-stream writes', async () => {
  let seen: AuthorizationContext | undefined
  const restricted = server(1000, (context) => {
    if (context.operation === 'subscribe') { seen = context; return context.subscription?.type !== 'webhook' }
    return context.operation !== 'append' || context.path !== 'wake/forbidden'
  })
  const denied = await request(subPath('denied-destination'), 'PUT', JSON.stringify({ type: 'webhook', pattern: 'private/*', webhook: { url: 'http://127.0.0.1:9/denied' } }), { 'Content-Type': 'application/json' }, restricted)
  expect(denied.status).toBe(403)
  expect(seen?.subscription).toMatchObject({ pattern: 'private/*', webhook: { url: 'http://127.0.0.1:9/denied' } })
  const noWrite = await request(subPath('denied-wake-write'), 'PUT', JSON.stringify({ type: 'pull-wake', pattern: 'private/*', wake_stream: 'wake/forbidden' }), { 'Content-Type': 'application/json' }, restricted)
  expect(noWrite.status).toBe(403)
  expect((await pool.query(`SELECT 1 FROM "${schema}".subscriptions WHERE name IN ('denied-destination','denied-wake-write')`)).rowCount).toBe(0)
})
