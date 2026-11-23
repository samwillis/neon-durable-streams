import { createHash, randomUUID } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'
import { pathToFileURL } from 'node:url'
import pg from 'pg'
import { createS3ChunkStorage } from '../../dist/s3.js'
import { advanceOffset, ZERO_OFFSET } from '../../dist/offset.js'

// Run after preparing independently packed artifacts with
// scripts/neon-test-rolling-artifacts.mjs. Uses fresh random schemas and exact
// owned keys only. No statistics resets, extension installation or VACUUM FULL.
const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8'))
if (
  !state.name?.startsWith('ds-test-') ||
  state.branchId !== env.NEON_TEST_BRANCH_ID ||
  !new URL(env.TEST_DATABASE_URL_UNPOOLED).hostname.endsWith('.neon.tech')
)
  throw new Error('Recorded disposable Neon environment required')
const artifacts = JSON.parse(await readFile('.neon-test-state-rolling-artifacts.json', 'utf8')).artifacts
if (artifacts.baseline.commit !== '1ba871b2495ceb0a988676c314886384446506a2') throw new Error('Unexpected baseline')
const base = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, connectionTimeoutMillis: 20_000, query_timeout: 60_000 })
pool.on('error', () => {})
let calls = 0,
  connection
pool.on('connect', (client) => {
  connection = client.connection.stream
  const original = client.query.bind(client)
  client.query = (...args) => {
    calls++
    return original(...args)
  }
})
const samples = [],
  schemas = [],
  keys = []
const rows = 8192,
  batchSize = 4096,
  ordinary = 64
const payload = (i) => String.fromCharCode(97 + (i % 26)).repeat(i < ordinary ? 32 : (i % 63) + 1)
const expected = createHash('sha256')
for (let i = 0; i < rows; i++) expected.update(payload(i))
const expectedDigest = expected.digest('hex')
let cleanupComplete = false
try {
  for (const [kind, framesPerChunk] of [
    ['baseline', 128],
    ['current', 128],
    ['current', 1024],
  ]) {
    const artifact = artifacts[kind]
    const api = await import(pathToFileURL(`${artifact.packageRoot}/dist/index.js`).href)
    const migrations = await import(pathToFileURL(`${artifact.packageRoot}/dist/migrations.js`).href)
    const schema = `ds_test_${randomUUID().replaceAll('-', '')}`,
      sql = `"${schema}"`
    schemas.push(schema)
    const objectCounts = { gets: 0, getBytes: 0, puts: 0, putBytes: 0 }
    const objects = {
      ...base,
      async get(key, max) {
        const bytes = await base.get(key, max)
        objectCounts.gets++
        objectCounts.getBytes += bytes.length
        return bytes
      },
      async put(key, bytes) {
        keys.push(key)
        objectCounts.puts++
        objectCounts.putBytes += bytes.length
        await base.put(key, bytes)
      },
    }
    await migrations.migrate(pool, { schema, storageIdentity: objects.identity })
    const errors = []
    const server = api.createDurableStreams({
      postgres: { pool, schema },
      storage: { objects, maxChunkFrames: framesPerChunk },
      auth: { authenticate: () => ({ tenantId: 'measure', subject: 'fixture' }), authorize: () => true },
      onError: (error) => errors.push(error.name),
    })
    const request = (method, body, path = 'data') =>
      server.fetch(
        new Request(`https://fixture.test/v1/stream/${path}`, {
          method,
          headers: { 'Content-Type': 'text/plain' },
          ...(body === undefined ? {} : { body }),
        }),
      )
    try {
      if ((await request('PUT')).status !== 201) throw new Error('Measurement stream creation failed')
      const id = (await pool.query(`SELECT id FROM ${sql}.streams WHERE path='data'`)).rows[0].id
      const lsn = (await pool.query('SELECT pg_current_wal_insert_lsn() AS lsn')).rows[0].lsn
      const appendStarted = performance.now(),
        beforeCalls = calls
      for (let i = 0; i < ordinary; i++) if ((await request('POST', payload(i))).status !== 204) throw new Error('Ordinary append failed')
      const appendDurationMs = performance.now() - appendStarted,
        appendSqlCalls = calls - beforeCalls
      const appendWalBytes = Number((await pool.query('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) AS bytes', [lsn])).rows[0].bytes)
      await pool.query('SELECT pg_stat_force_next_flush()')
      const headStats = (
        await pool.query(`SELECT n_tup_upd::text,n_tup_hot_upd::text FROM pg_stat_user_tables WHERE schemaname=$1 AND relname='streams'`, [schema])
      ).rows[0]
      let offset = ZERO_OFFSET
      for (let i = 0; i < ordinary; i++) offset = advanceOffset(offset, payload(i).length)
      const sealingStarted = performance.now()
      for (let batch = 0; batch < rows / batchSize; batch++) {
        const from = batch === 0 ? ordinary : batch * batchSize,
          to = (batch + 1) * batchSize,
          frames = []
        for (let i = from; i < to; i++) {
          const part = Buffer.from(payload(i)),
            start = offset
          offset = advanceOffset(offset, part.length)
          frames.push({ start, end: offset, bytes: part.length, payload: part.toString('hex') })
        }
        await pool.query('BEGIN')
        try {
          await pool.query(
            `INSERT INTO ${sql}.appends(stream_id,start_offset,end_offset,payload,payload_bytes)
            SELECT $1,x.start,x.end,decode(x.payload,'hex'),x.bytes FROM jsonb_to_recordset($2::jsonb) x(start text,"end" text,payload text,bytes integer)`,
            [id, JSON.stringify(frames)],
          )
          await pool.query(`UPDATE ${sql}.streams SET tail_offset=$2,hot_frames=hot_frames+$3,hot_bytes=hot_bytes+$4 WHERE id=$1`, [
            id,
            offset,
            frames.length,
            frames.reduce((sum, f) => sum + f.bytes, 0),
          ])
          if (kind === 'current')
            await pool.query(`INSERT INTO ${sql}.hot_chunks VALUES($1,clock_timestamp()+interval '1 hour') ON CONFLICT DO NOTHING`, [id])
          await pool.query('COMMIT')
        } catch (error) {
          await pool.query('ROLLBACK')
          throw error
        }
        await server.maintain({ maxChunks: 128, forceHot: true })
      }
      const sealingDurationMs = performance.now() - sealingStarted,
        sealingObjects = { ...objectCounts }
      const relations = []
      for (const name of ['appends', 'chunks', 'chunk_objects']) {
        const value = (
          await pool.query(`SELECT count(*)::integer AS rows,COALESCE(sum(pg_column_size(t)),0)::text AS live_tuple_bytes FROM ${sql}."${name}" t`)
        ).rows[0]
        const sizes = (
          await pool.query(
            'SELECT pg_relation_size($1::regclass)::text AS heap_bytes,pg_indexes_size($1::regclass)::text AS index_bytes,pg_total_relation_size($1::regclass)::text AS total_bytes',
            [`${sql}."${name}"`],
          )
        ).rows[0]
        relations.push({
          name,
          rows: value.rows,
          liveTupleBytes: Number(value.live_tuple_bytes),
          allocatedBeforeVacuum: Object.fromEntries(Object.entries(sizes).map(([key, val]) => [key, Number(val)])),
        })
        await pool.query(`VACUUM (ANALYZE) ${sql}."${name}"`)
        relations.at(-1).allocatedAfterVacuum = Number(
          (await pool.query('SELECT pg_total_relation_size($1::regclass)::text AS bytes', [`${sql}."${name}"`])).rows[0].bytes,
        )
      }
      objectCounts.gets = 0
      objectCounts.getBytes = 0
      const beforeReadCalls = calls,
        beforeReadBytes = connection.bytesRead,
        replayStarted = performance.now(),
        hash = createHash('sha256')
      let cursor = '-1',
        pages = 0
      do {
        const response = await request('GET', undefined, `data?offset=${cursor}`)
        if (response.status !== 200) throw new Error('Measured replay failed')
        hash.update(Buffer.from(await response.arrayBuffer()))
        pages++
        cursor = response.headers.get('Stream-Next-Offset')
        if (response.headers.has('Stream-Up-To-Date')) break
        if (pages > rows) throw new Error('Replay did not converge')
      } while (true)
      if (hash.digest('hex') !== expectedDigest || errors.length) throw new Error('Measured replay digest or runtime errors differ')
      const replay = {
        durationMs: performance.now() - replayStarted,
        pages,
        objectGets: objectCounts.gets,
        objectBytes: objectCounts.getBytes,
        sqlCalls: calls - beforeReadCalls,
        observedConnectionReadBytes: connection.bytesRead - beforeReadBytes,
      }
      samples.push({
        kind,
        framesPerChunk,
        rows,
        ordinaryAppends: ordinary,
        ordinary: {
          durationMs: appendDurationMs,
          sqlCalls: appendSqlCalls,
          observedBranchWalBytes: appendWalBytes,
          headTupleUpdates: Number(headStats.n_tup_upd),
          headHotUpdates: Number(headStats.n_tup_hot_upd),
        },
        sealingDurationMs,
        sealingObjects,
        relations,
        replay,
      })
      console.log(
        `Measured ${kind}/${framesPerChunk}: ${relations[0].rows} retained append rows; ${replay.objectGets} replay object reads across ${replay.pages} pages.`,
      )
    } finally {
      await server.close()
    }
  }
} finally {
  try {
    for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    for (const key of new Set(keys)) await base.delete(key)
    cleanupComplete = true
  } finally {
    await pool.end()
    base.close()
  }
}
const sanitized = Object.fromEntries(Object.entries(artifacts).map(([kind, { packageRoot, consumer, ...artifact }]) => [kind, artifact]))
await writeFile(
  process.argv[2] ?? '/tmp/neon-compact-measurement.json',
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      harnessSha256: createHash('sha256')
        .update(await readFile(new URL(import.meta.url)))
        .digest('hex'),
      artifacts: sanitized,
      node: process.version,
      region: env.AWS_REGION,
      cleanupComplete,
      scope:
        'Actual sealing/replay on disposable Neon; two 4096-frame batches per profile; 64 ordinary API appends; remaining frames seeded transactionally. Separate live tuple bytes from allocated high-water relation sizes. Branch WAL includes background activity; connection read bytes include protocol overhead. No throughput/capacity claim.',
      samples,
    },
    null,
    2,
  ) + '\n',
)
