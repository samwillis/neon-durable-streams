import assert from 'node:assert/strict'
import { randomUUID, createHash } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'
import pg from 'pg'

const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state-replay.json', 'utf8'))
const schema = env.TEST_FUNCTION_SCHEMA
if (!state.name.startsWith('ds-test-') || state.branchId !== env.NEON_TEST_BRANCH_ID || state.branchId !== env.TEST_FUNCTION_BRANCH_ID || !/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Recorded isolated replay Function required')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, query_timeout: 120000 })
const path = `replay-${randomUUID()}/dense`
let count
const payload = Buffer.alloc(32, 120)
const request = (suffix, options = {}) => fetch(new URL(suffix, env.TEST_FUNCTION_URL), { ...options,
  headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, ...options.headers }, signal: AbortSignal.timeout(180000) })
const samples = []
let result
try {
  const health = await (await request('_benchmark/health')).json()
  assert.equal(health.build, env.TEST_FUNCTION_BUNDLE_SHA)
  count = health.settings.maxChunkFrames
  assert.ok([8192, 32768, 131072].includes(count))
  assert.equal(health.settings.targetChunkBytes, 4194304)
  const created = await request(`v1/stream/${path}`, { method: 'PUT', headers: { 'Content-Type': 'application/octet-stream' } })
  assert.equal(created.status, 201)
  const zero = created.headers.get('Stream-Next-Offset')
  const stream = (await pool.query(`SELECT id FROM "${schema}".streams WHERE tenant_id='replay' AND path=$1`, [path])).rows[0].id
  const history = stream.replaceAll('-', '')
  const footprint = async () => {
    const relations = []
    for (const name of ['appends', 'chunks', 'chunk_objects']) {
      const sizes = (await pool.query('SELECT pg_relation_size($1::regclass)::text AS heap_bytes,pg_indexes_size($1::regclass)::text AS index_bytes,pg_total_relation_size($1::regclass)::text AS total_bytes', [`"${schema}".${name}`])).rows[0]
      relations.push({ name, ...Object.fromEntries(Object.entries(sizes).map(([key, value]) => [key, Number(value)])) })
    }
    return relations
  }
  const before = (await pool.query('SELECT pg_current_wal_insert_lsn() AS lsn')).rows[0].lsn
  const seedStarted = performance.now()
  // Bulk setup is not append throughput: the final append, freeze, seal and
  // every replay use the deployed public consumer against actual Neon.
  await pool.query('BEGIN')
  try {
    await pool.query(`SET LOCAL statement_timeout='90s'`)
    await pool.query(`INSERT INTO "${schema}".appends(stream_id,start_offset,end_offset,payload,payload_bytes)
      SELECT $1::uuid,CASE WHEN n=0 THEN $2 ELSE lpad(n::text,16,'0')||'_'||$3 END,lpad((n+1)::text,16,'0')||'_'||$3,$4,32
      FROM generate_series(0,$5::integer-2) n`, [stream, zero, history, payload, count])
    await pool.query(`UPDATE "${schema}".streams SET tail_offset=$2,hot_frames=$3::integer,hot_bytes=$3::integer*32 WHERE id=$1`, [stream, `${String(count - 1).padStart(16, '0')}_${history}`, count - 1])
    await pool.query(`INSERT INTO "${schema}".hot_chunks VALUES($1,clock_timestamp()+interval '1 hour')`, [stream])
    await pool.query('COMMIT')
  } catch (error) { await pool.query('ROLLBACK'); throw error }
  const seedMs = performance.now() - seedStarted
  const stagingFootprint = await footprint()
  const appendStarted = performance.now()
  const append = await request(`v1/stream/${path}`, { method: 'POST', body: payload, headers: { 'Content-Type': 'application/octet-stream' } })
  assert.equal(append.status, 204)
  const tail = append.headers.get('Stream-Next-Offset')
  const appendMs = performance.now() - appendStarted
  const freezeMetrics = JSON.parse(append.headers.get('x-replay-metrics'))
  assert.equal(tail, `${String(count).padStart(16, '0')}_${history}`)
  console.log(`The public final append froze all ${count} frames`)
  const sealStarted = performance.now()
  const maintenance = await request('_benchmark/maintain', { method: 'POST' })
  assert.equal(maintenance.status, 200)
  const sealed = await maintenance.json()
  assert.equal(sealed.sealed, 1)
  const sealMs = performance.now() - sealStarted
  assert.equal((await pool.query(`SELECT count(*) FROM "${schema}".appends WHERE stream_id=$1`, [stream])).rows[0].count, '0')
  const manifests = (await pool.query(`SELECT frame_count,payload_bytes,encoded_bytes,octet_length(block_directory) AS directory_bytes FROM "${schema}".chunks WHERE stream_id=$1`, [stream])).rows
  assert.equal(manifests.length, 1)
  assert.equal(manifests[0].frame_count, count)
  const sealedFootprint = await footprint()
  for (const name of ['appends', 'chunks', 'chunk_objects']) await pool.query(`VACUUM (ANALYZE) "${schema}".${name}`)
  const vacuumedFootprint = await footprint()
  const digest = createHash('sha256')
  let cursor = zero, bytes = 0
  const replayStarted = performance.now()
  while (cursor !== tail) {
    const started = performance.now()
    const response = await request(`v1/stream/${path}?offset=${encodeURIComponent(cursor)}`)
    assert.equal(response.status, 200)
    const body = Buffer.from(await response.arrayBuffer())
    assert.deepEqual(body, Buffer.alloc(body.length, 120))
    assert.ok(body.length > 0)
    bytes += body.length; digest.update(body)
    const next = response.headers.get('Stream-Next-Offset')
    assert.ok(next > cursor)
    samples.push({ bytes: body.length, durationMs: performance.now() - started, metrics: JSON.parse(response.headers.get('x-replay-metrics')) })
    cursor = next
    assert.ok(samples.length <= Math.ceil(count / 8192))
  }
  assert.equal(samples.length, Math.ceil(count / 8192))
  assert.equal(bytes, count * 32)
  assert.equal(digest.digest('hex'), createHash('sha256').update(Buffer.alloc(count * 32, 120)).digest('hex'))
  assert.equal((await pool.query(`SELECT count(*) FROM "${schema}".replay_pins`)).rows[0].count, '0')
  result = { passed: true, count, stagingFootprint, sealedFootprint, vacuumedFootprint, seedMs, appendMs, freezeMetrics, sealMs, maintenance: sealed, manifests, replayMs: performance.now() - replayStarted,
    deliveredBytes: bytes, samples, observedBranchWalBytes: Number((await pool.query('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) AS bytes', [before])).rows[0].bytes) }
} finally { await pool.end() }
await writeFile(env.REPLAY_REPORT ?? 'qualification-results/replay-dense.json', JSON.stringify({
  ...result, sourceCommit: env.TEST_FUNCTION_SOURCE_COMMIT, bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA, branchId: state.branchId,
  harnessSha256: createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex'),
  scope: 'Actual configured-full-chunk qualification. Bulk seed count-1 constrained hot rows; final ordinary public append triggers freeze, deployed maintenance uploads/read-back-verifies/deletes staging, and default public pages verify exact bytes and final checkpoint. This is not append throughput. Lifetime RSS is the reached sealer process high-water, not fleet memory; branch WAL includes setup and background work.',
}, null, 2) + '\n')
console.log(JSON.stringify({ passed: result.passed, appendMs: result.appendMs, sealMs: result.sealMs, replayMs: result.replayMs, pages: result.samples.length, memory: result.maintenance.benchmark }))
