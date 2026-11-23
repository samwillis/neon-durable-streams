import { createHash, randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

// Research only: no objects are uploaded. All SQL uses a fresh disposable
// schema on the recorded Neon test branch; existing data/stats are untouched.
const root = fileURLToPath(new URL('../../', import.meta.url))
const require = createRequire(join(root, 'package.json'))
const pg = require('pg')
const { migrate } = await import(pathToFileURL(join(root, 'dist/migrations.js')).href)
const env = process.env
if (!env.TEST_DATABASE_URL_UNPOOLED || !env.NEON_TEST_BRANCH_ID) throw new Error('Recorded disposable Neon test environment required')
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? join(root, '.neon-test-state-smoke.json'), 'utf8'))
if (state.branchId !== env.NEON_TEST_BRANCH_ID || !state.name?.startsWith('ds-test-')
  || !new URL(env.TEST_DATABASE_URL_UNPOOLED).hostname.endsWith('.neon.tech')) throw new Error('Refusing measurement outside the recorded Neon test branch')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, connectionTimeoutMillis: 20_000, query_timeout: 60_000 })
pool.on('error', () => {})
const schemas = [], samples = []
let cleanupComplete = false
const rows = 65_536, payloadBytes = 32, stride = payloadBytes + 5
const column = '0000000000000000_' // The existing wire-offset representation.
const quote = (name) => { if (!/^ds_test_[a-f0-9]{32}$/.test(name)) throw new Error('Invalid owned schema'); return `"${name}"` }
try {
  const version = (await pool.query("SELECT current_setting('server_version_num') AS version")).rows[0].version
  if (Number(version) < 180000) throw new Error('Neon Postgres 18 required')
  for (const framesPerChunk of [128, 1024]) {
    const schema = `ds_test_${randomUUID().replaceAll('-', '')}`, sql = quote(schema), streamId = randomUUID()
    schemas.push(schema)
    await migrate(pool, { schema, storageIdentity: 'synthetic-metadata-measurement', environmentId: env.NEON_TEST_BRANCH_ID })
    await pool.query('BEGIN')
    try {
      await pool.query("SET LOCAL statement_timeout='60s'; SET LOCAL lock_timeout='5s'; SET LOCAL transaction_timeout='120s'")
      await pool.query(`INSERT INTO ${sql}.paths (tenant_id,path) VALUES ('measurement','stream')`)
      await pool.query(`INSERT INTO ${sql}.streams (id,tenant_id,path,content_type,tail_offset)
        VALUES ($1,'measurement','stream','application/octet-stream',$2)`, [streamId, column + String(rows * stride).padStart(16, '0')])
      await pool.query(`UPDATE ${sql}.paths SET active_stream_id=$1`, [streamId])
      await pool.query(`INSERT INTO ${sql}.chunks (id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes,object_key,sha256,encoded_bytes,sealed_at)
        SELECT md5($1::text||':'||n::text)::uuid,$1::uuid,$2||lpad((n*$3*$4)::text,16,'0'),$2||lpad(((n+1)*$3*$4)::text,16,'0'),
          'sealed',$3,$3*$5,'v1/'||i.id::text||'/'||$1::text||'/'||md5($1::text||':'||n::text)::uuid::text||'/'||repeat('0',64)||'.ndsc',repeat('0',64),60+$3*(74+$5),clock_timestamp()
        FROM generate_series(0,$6::int-1) AS n CROSS JOIN ${sql}.installation i`, [streamId, column, framesPerChunk, stride, payloadBytes, rows / framesPerChunk])
      await pool.query(`INSERT INTO ${sql}.appends (stream_id,start_offset,end_offset,payload,payload_bytes,chunk_id)
        SELECT $1::uuid,$2||lpad((n*$3)::text,16,'0'),$2||lpad(((n+1)*$3)::text,16,'0'),NULL,$4,md5($1::text||':'||(n/$5)::text)::uuid
        FROM generate_series(0,$6::int-1) AS n`, [streamId, column, stride, payloadBytes, framesPerChunk, rows])
      await pool.query(`INSERT INTO ${sql}.chunk_objects (object_key,stream_id,chunk_id,sha256,encoded_bytes)
        SELECT object_key,stream_id,id,sha256,encoded_bytes FROM ${sql}.chunks`)
      // Alternative research layout: one exact cumulative end-delta directory
      // per manifest. This array is not used by the implementation today.
      await pool.query(`CREATE TABLE ${sql}.directories (chunk_id uuid PRIMARY KEY REFERENCES ${sql}.chunks(id), end_deltas integer[] NOT NULL)`)
      await pool.query(`INSERT INTO ${sql}.directories SELECT id,ARRAY(SELECT n*$1 FROM generate_series(1,$2) n) FROM ${sql}.chunks`, [stride, framesPerChunk])
      // Copy the actual manifest columns/indexes so the proposed embedded
      // layout is measured too. LIKE does not copy foreign keys; those do not
      // add a relation/index here and are not part of this footprint comparison.
      await pool.query(`CREATE TABLE ${sql}.packed_chunks (LIKE ${sql}.chunks INCLUDING ALL)`)
      await pool.query(`ALTER TABLE ${sql}.packed_chunks ADD COLUMN end_deltas integer[] NOT NULL`)
      await pool.query(`INSERT INTO ${sql}.packed_chunks SELECT c.*,ARRAY(SELECT n*$1 FROM generate_series(1,$2) n) FROM ${sql}.chunks c`, [stride, framesPerChunk])
      await pool.query('COMMIT')
    } catch (error) { await pool.query('ROLLBACK'); throw error }
    const relations = []
    for (const name of ['appends', 'chunks', 'chunk_objects', 'directories', 'packed_chunks']) {
      const relation = `${sql}."${name}"`
      const counts = (await pool.query(`SELECT count(*)::int AS rows,COALESCE(sum(pg_column_size(t)),0)::bigint AS tuple_bytes FROM ${relation} t`)).rows[0]
      const sizes = (await pool.query('SELECT pg_relation_size($1::regclass)::bigint AS heap_bytes,pg_indexes_size($1::regclass)::bigint AS index_bytes,pg_total_relation_size($1::regclass)::bigint AS total_bytes', [relation])).rows[0]
      relations.push({ relation: name, rows: counts.rows, tupleBytes: Number(counts.tuple_bytes), heapBytes: Number(sizes.heap_bytes), indexBytes: Number(sizes.index_bytes), totalBytes: Number(sizes.total_bytes) })
    }
    const byName = Object.fromEntries(relations.map((value) => [value.relation, value]))
    const current = byName.appends.totalBytes + byName.chunks.totalBytes + byName.chunk_objects.totalBytes
    const manifests = byName.chunks.totalBytes + byName.chunk_objects.totalBytes
    // Keep the separate-table comparison, but use the measured embedded layout
    // for the recommendation: TOAST/page effects are not strictly monotonic.
    samples.push({ framesPerChunk, sealedAppendRows: rows, chunks: rows / framesPerChunk, payloadBytes,
      relations, currentMetadataBytes: current, manifestsAndLedgerBytes: manifests,
      manifestsLedgerAndSeparateDirectoriesBytes: manifests + byName.directories.totalBytes,
      packedManifestsAndLedgerBytes: byName.packed_chunks.totalBytes + byName.chunk_objects.totalBytes,
      idealPackedBoundaryBytes: rows * 4,
      excludes: ['payload bytes in Object Storage', 'other fixed/producer/subscription tables', 'prior MVCC versions, vacuum overhead, WAL and provider history', 'new-reader or write-throughput benchmark'] })
  }
} finally {
  try { for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${quote(schema)} CASCADE`); cleanupComplete = true }
  finally { await pool.end() }
}
const output = process.argv[2] ?? '/tmp/neon-postgres-metadata-baseline.json'
await mkdir(dirname(output), { recursive: true })
const report = { generatedAt: new Date().toISOString(), sourceCommit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
  harnessSha256: createHash('sha256').update(await readFile(fileURLToPath(import.meta.url))).digest('hex'),
  node: process.version, region: env.AWS_REGION, storage: 'Synthetic steady-state metadata in current schema on actual Neon Postgres 18',
  objectUploads: 0, cleanupComplete, samples }
await writeFile(output, JSON.stringify(report, null, 2)+'\n')
console.log(JSON.stringify(report))
