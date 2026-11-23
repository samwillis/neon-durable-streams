import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, beforeAll, expect, test } from 'vitest'
import fc from 'fast-check'
import { createDurableStreams } from '../../src/index.js'
import { migrate } from '../../src/migrations.js'
import { postgresTestLimits } from '../fixtures/postgres-limits.js'

if (!process.env.TEST_DATABASE_URL_UNPOOLED) throw new Error('Lifecycle tests require an isolated Neon database')
const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
pool.on('error', () => {})
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const server = createDurableStreams({ postgres: { ...postgresTestLimits, pool, schema },
  auth: { authenticate: () => ({ tenantId: 'lifecycle', subject: 'fixture' }), authorize: () => true } })
const request = (path: string, method = 'GET', body?: string, headers?: Record<string, string>) => server.fetch(new Request(`https://fixture.test/v1/stream/${path}`, {
  method, headers: { 'Content-Type': 'text/plain', ...headers }, ...(body === undefined ? {} : { body }),
}))
beforeAll(async () => { await migrate(pool, { schema }) })
afterAll(async () => { await server.close(); await pool.query(`DROP SCHEMA "${schema}" CASCADE`); await pool.end() })

test('Postgres rejects partial recovery operations and ownerless claims/leases', async () => {
  const sql = (text: string, values?: unknown[]) => pool.query(text.replaceAll('ds.', `"${schema}".`), values)
  await expect(sql("UPDATE ds.installation SET serving_state='recovering'")).rejects.toMatchObject({ code: '23514' })
  await expect(sql('UPDATE ds.installation SET recovery_verified=-1')).rejects.toMatchObject({ code: '23514' })
  const id = randomUUID()
  await sql(`INSERT INTO ds.subscriptions(id,tenant_id,name,config,config_hash,public_base_url)
    VALUES($1,'lifecycle','constraints','{}','fixture','https://fixture.test/v1/stream')`, [id])
  await expect(sql("UPDATE ds.subscriptions SET holder='worker' WHERE id=$1", [id])).rejects.toMatchObject({ code: '23514' })
  await expect(sql("UPDATE ds.subscriptions SET holder='worker',lease_until=clock_timestamp() WHERE id=$1", [id])).rejects.toMatchObject({ code: '23514' })
  await expect(sql(`INSERT INTO ds.subscription_wakes(id,subscription_id,tenant_id,generation,kind,state,delivery_claim)
    VALUES($1,$2,'lifecycle',1,'webhook','ready',$1)`, [randomUUID(), id])).rejects.toMatchObject({ code: '23514' })
  await expect(sql(`INSERT INTO ds.chunk_objects(object_key,stream_id,chunk_id,sha256,encoded_bytes,claim_token)
    VALUES('constraint',$1,$1,$2,1,$1)`, [randomUUID(), '0'.repeat(64)])).rejects.toMatchObject({ code: '23514' })
  expect((await request('constraint-chunk', 'PUT', 'x')).status).toBe(201)
  const stream = (await sql("SELECT id FROM ds.streams WHERE path='constraint-chunk'")).rows[0].id
  await expect(sql(`INSERT INTO ds.chunks(id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes,claim_token)
    VALUES($1,$2,'0000000000000000_00000000000000000000000000000000','0000000000000000_0000000000000006','frozen',1,1,$1)`, [randomUUID(), stream])).rejects.toMatchObject({ code: '23514' })
  const removed = await sql(`SELECT column_name FROM information_schema.columns
    WHERE table_schema=$1 AND column_name IN ('processed','pull_published')`, [schema])
  expect(removed.rows).toEqual([])
})

test('generated append, close, delete, and recreation sequences preserve bytes and generation boundaries on Neon', async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.constantFrom('append', 'close', 'delete', 'create', 'read'), { minLength: 8, maxLength: 18 }), async (commands) => {
    const path = `generated-${randomUUID()}`
    let exists = true, closed = false, bytes = ''
    expect((await request(path, 'PUT')).status).toBe(201)
    for (const command of commands) {
      if (command === 'create') {
        const response = await request(path, 'PUT')
        expect(response.status).toBe(exists ? closed ? 409 : 200 : 201)
        if (!exists) { exists = true; closed = false; bytes = '' }
      } else if (command === 'delete') {
        expect((await request(path, 'DELETE')).status).toBe(exists ? 204 : 404)
        exists = false
      } else if (command === 'append') {
        expect((await request(path, 'POST', 'x')).status).toBe(!exists ? 404 : closed ? 409 : 204)
        if (exists && !closed) bytes += 'x'
      } else if (command === 'close' && exists && !closed) {
        expect((await request(path, 'POST', '', { 'Stream-Closed': 'true' })).status).toBe(204)
        closed = true
      }
      const replay = await request(path)
      expect(replay.status).toBe(exists ? 200 : 404)
      if (exists) { expect(await replay.text()).toBe(bytes); expect(replay.headers.has('Stream-Closed')).toBe(closed) }
    }
  }), { numRuns: 8, seed: 20261123 })
})
