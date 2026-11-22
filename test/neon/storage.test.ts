import { randomUUID } from 'node:crypto'
import { fork } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createDurableStreams } from '../../src/index.js'
import type { ChunkStorage, DurableStreamsServer } from '../../src/index.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { migrate } from '../../src/migrations.js'

const { TEST_DATABASE_URL, TEST_DATABASE_URL_UNPOOLED, AWS_ENDPOINT_URL_S3, AWS_REGION, NEON_TEST_BUCKET } = process.env
if (!TEST_DATABASE_URL || !TEST_DATABASE_URL_UNPOOLED || !AWS_ENDPOINT_URL_S3 || !AWS_REGION || !NEON_TEST_BUCKET) throw new Error('Real Neon Postgres and Object Storage credentials are required')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 6 })
const direct = new pg.Pool({ connectionString: TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
direct.on('error', () => {})
const objects = createS3ChunkStorage({ endpoint: AWS_ENDPOINT_URL_S3, region: AWS_REGION, bucket: NEON_TEST_BUCKET })
const servers: DurableStreamsServer[] = []
const server = (storage: ChunkStorage = objects, onError?: (error: unknown) => void) => {
  const handler = createDurableStreams({ postgres: { pool, schema }, storage: { objects: storage, targetChunkBytes: 8, maxChunkFrames: 4 },
    auth: { authenticate: () => ({ tenantId: 'storage', subject: 'fixture' }), authorize: () => true }, ...(onError ? { onError } : {}) })
  servers.push(handler)
  return handler
}
const request = (handler: DurableStreamsServer, path: string, method = 'GET', body?: string, headers?: Record<string, string>) => handler.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body !== undefined ? { body } : {}),
}))
async function readAll(handler: DurableStreamsServer, path: string) {
  let offset = '-1', content = ''
  for (let i = 0; i < 100; i++) {
    const response = await request(handler, `${path}?offset=${offset}`)
    expect(response.status).toBe(200)
    content += await response.text()
    if (response.headers.get('Stream-Up-To-Date')) return content
    offset = response.headers.get('Stream-Next-Offset')!
  }
  throw new Error('Replay did not converge')
}
beforeAll(async () => {
  expect(new URL(TEST_DATABASE_URL).hostname).toContain('.neon.tech')
  expect(new URL(AWS_ENDPOINT_URL_S3).hostname).toContain('.storage.')
  await migrate(direct, { schema, storageIdentity: objects.identity })
})
afterAll(async () => {
  await Promise.all(servers.map((handler) => handler.close()))
  objects.close()
  await direct.query(`DROP SCHEMA "${schema}" CASCADE`)
  await pool.end()
  await direct.end()
})

test('replay crosses sealed and hot data after a new server instance, preserving producer deduplication', async () => {
  const writer = server()
  await request(writer, 'restart', 'PUT', 'start---')
  const tuple = { 'Producer-Id': 'writer', 'Producer-Epoch': '0', 'Producer-Seq': '0' }
  expect((await request(writer, 'restart', 'POST', 'once----', tuple)).status).toBe(200)
  expect((await writer.maintain({ maxChunks: 16 })).sealed).toBeGreaterThanOrEqual(2)
  expect((await request(writer, 'restart', 'POST', 'hot')).status).toBe(204)
  const stats = await pool.query(`SELECT c.state, a.payload IS NULL AS remote FROM "${schema}".appends a LEFT JOIN "${schema}".chunks c ON c.id = a.chunk_id JOIN "${schema}".streams s ON s.id = a.stream_id WHERE s.path = 'restart' ORDER BY a.end_offset`)
  expect(stats.rows).toEqual([{ state: 'sealed', remote: true }, { state: 'sealed', remote: true }, { state: null, remote: false }])
  const fresh = server()
  expect(await readAll(fresh, 'restart')).toBe('start---once----hot')
  expect((await request(fresh, 'restart', 'POST', 'different', tuple)).status).toBe(204)
  expect(await readAll(fresh, 'restart')).toBe('start---once----hot')
})

test('sealed JSON retains exact numeric lexemes and UTF-8 message indexes', async () => {
  const handler = server()
  await request(handler, 'json', 'PUT', '[9007199254740993,1e400,-0,"🐘"]', { 'Content-Type': 'application/json' })
  await handler.maintain({ maxChunks: 16 })
  expect(await readAll(handler, 'json')).toBe('[9007199254740993,1e400,-0,"🐘"]')
})

test('an upload whose response is lost preserves local bytes and can be adopted after restart', async () => {
  let uploaded: string | undefined
  const faulty = server({ ...objects, async put(key, bytes) { await objects.put(key, bytes); uploaded = key; throw new Error('simulated lost upload response') } })
  await request(faulty, 'upload-loss', 'PUT', 'keep these bytes')
  await expect(faulty.maintain({ maxChunks: 1 })).rejects.toThrow('simulated lost upload response')
  expect(uploaded).toMatch(/\.ndsc$/)
  expect((await objects.get(uploaded!, 1024)).length).toBeGreaterThan(0)
  const retained = await pool.query(`SELECT a.payload FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id = a.stream_id WHERE s.path = 'upload-loss'`)
  expect(retained.rows[0].payload.toString()).toBe('keep these bytes')
  const restarted = server()
  await restarted.maintain({ maxChunks: 16 })
  expect(await readAll(restarted, 'upload-loss')).toBe('keep these bytes')
})

test('a worker with an expired claim cannot publish over its replacement', async () => {
  let reached: () => void
  const barrier = new Promise<void>((resolve) => { reached = resolve })
  let resume: () => void
  const gate = new Promise<void>((resolve) => { resume = resolve })
  let waiting = true
  const slow = server({ ...objects, async get(key, maximum) {
    const bytes = await objects.get(key, maximum)
    if (waiting) { waiting = false; reached(); await gate }
    return bytes
  } })
  await request(slow, 'fence', 'PUT', 'fenced payload')
  const obsolete = slow.maintain({ maxChunks: 1 })
  try {
    await barrier
    await pool.query(`UPDATE "${schema}".chunks SET claim_until = clock_timestamp() - interval '1 second' WHERE claim_token IS NOT NULL`)
    expect((await server().maintain({ maxChunks: 16 })).sealed).toBeGreaterThanOrEqual(1)
  } finally { resume!() }
  expect((await obsolete).sealed).toBe(0)
  const rows = await pool.query(`SELECT c.state, c.claim_epoch FROM "${schema}".chunks c JOIN "${schema}".streams s ON s.id = c.stream_id WHERE s.path = 'fence'`)
  expect(rows.rows).toEqual([{ state: 'sealed', claim_epoch: '2' }])
})

test('checksum corruption fails closed before returning any stored bytes', async () => {
  const expected: unknown[] = []
  const corrupt = server({ ...objects, async get(key, maximum) {
    const bytes = Buffer.from(await objects.get(key, maximum))
    bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 1
    return bytes
  } }, (error) => expected.push(error))
  const response = await request(corrupt, 'restart')
  expect(response.status).toBe(500)
  expect(await response.text()).not.toContain('start---')
  expect(expected).toHaveLength(1)
})

test('a different bucket or branch identity requires explicit recovery', async () => {
  await expect(migrate(direct, { schema, storageIdentity: 'another-branch' })).rejects.toThrow('explicit branch adoption')
  expect((await pool.query(`SELECT storage_identity FROM "${schema}".installation`)).rows[0].storage_identity).toBe(objects.identity)
})

test('SIGKILL after publication preserves sealed replay without local payloads', async () => {
  const writer = server()
  await request(writer, 'sealed-crash', 'PUT', 'survives publication process loss')
  const child = fork(fileURLToPath(new URL('../fixtures/seal-worker.mjs', import.meta.url)), [], {
    execArgv: [], env: { ...process.env, TEST_SCHEMA: schema }, stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  })
  let stderr = ''
  child.stderr?.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
  try {
    const message = await Promise.race([
      once(child, 'message').then(([value]) => value as { ready: boolean }),
      once(child, 'exit').then(() => { throw new Error(`Sealing worker exited before commit barrier: ${stderr}`) }),
    ])
    expect(message.ready).toBe(true)
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit')
      child.kill('SIGKILL')
      await exited
    }
  }
  const payload = await pool.query(`SELECT a.payload FROM "${schema}".appends a JOIN "${schema}".streams s ON s.id = a.stream_id WHERE s.path = 'sealed-crash'`)
  expect(payload.rows[0].payload).toBeNull()
  expect(await readAll(server(), 'sealed-crash')).toBe('survives publication process loss')
})
