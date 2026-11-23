import { randomUUID, createHash } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import pg from 'pg'
import { migrate, STORAGE_VERSION } from '../../dist/migrations.js'
import { encodeBlockObject } from '../../dist/block-codec.js'
import { advanceOffset, ZERO_OFFSET } from '../../dist/offset.js'

// Isolated footprint experiment. Real schema constraints and Postgres storage;
// generated sealed descriptors, no object uploads, no application traffic.
const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state-replay.json', 'utf8'))
if (!state.name.startsWith('ds-test-') || state.branchId !== env.NEON_TEST_BRANCH_ID || !env.TEST_DATABASE_URL_UNPOOLED) throw new Error('Recorded disposable Neon branch required')
const control = '366768286d504072b5af3fd6309ae317eccc9b1a'
const git = (...args) => execFileSync('git', args, { encoding: 'utf8' }).trim()
if (git('status', '--porcelain', '--', 'src', 'migrations', 'docs/experiments/measure-ordinal-metadata.mjs')) throw new Error('Commit the metadata candidate before measuring')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, query_timeout: 60000 })
const schemas = [], samples = []
const appends = 1048576, payloadBytes = 32
let cleanupComplete = false
try {
  for (const [kind, framesPerChunk] of [['byte-control', 1024], ['ordinal', 1024], ['ordinal', 131072]]) {
    const schema = `ds_test_${randomUUID().replaceAll('-', '')}`, sql = `"${schema}"`
    schemas.push(schema)
    if (kind === 'ordinal') await migrate(pool, { schema, storageIdentity: 'metadata-only', environmentId: state.branchId })
    else {
      await pool.query(`CREATE SCHEMA ${sql}`)
      for (const file of ['0001_installation', '0002_streams', '0003_storage', '0004_subscriptions']) {
        await pool.query(git('show', `${control}:migrations/${file}.sql`).replaceAll('__SCHEMA__', sql))
      }
      await pool.query(`INSERT INTO ${sql}.installation(id,storage_version) VALUES($1,2)`, [randomUUID()])
    }
    const installation = (await pool.query(`SELECT id,serving_epoch FROM ${sql}.installation`)).rows[0]
    const stream = randomUUID(), history = stream.replaceAll('-', '')
    const token = (ordinal) => kind === 'ordinal' ? `${String(ordinal).padStart(16, '0')}_${history}`
      : `0000000000000000_${String(ordinal * (payloadBytes + 5)).padStart(16, '0')}`
    const zero = kind === 'ordinal' ? ZERO_OFFSET : token(0)
    await pool.query(`INSERT INTO ${sql}.paths(tenant_id,path) VALUES('measure','history')`)
    await pool.query(`INSERT INTO ${sql}.streams(id,tenant_id,path,content_type,tail_offset${kind === 'ordinal' ? ',write_epoch,write_history' : ''})
      VALUES($1,'measure','history','application/octet-stream',$2${kind === 'ordinal' ? ',$3,$1' : ''})`,
    kind === 'ordinal' ? [stream, token(appends), installation.serving_epoch] : [stream, token(appends)])
    await pool.query(`UPDATE ${sql}.paths SET active_stream_id=$1`, [stream])
    const inserts = []
    const payload = Buffer.alloc(payloadBytes, 120)
    // This reusable verified-block directory has the same size/compressibility
    // as the codec's physical layout. SQL offsets below describe each chunk.
    let next = ZERO_OFFSET
    const frames = Array.from({ length: framesPerChunk }, () => {
      const start = next; next = advanceOffset(next, history)
      return { start, end: next, payload, messageEnds: null }
    })
    const example = encodeBlockObject({ installationId: installation.id, streamId: stream, chunkId: randomUUID(), frames })
    for (let chunk = 0; chunk < appends / framesPerChunk; chunk++) {
      const id = randomUUID(), first = chunk * framesPerChunk, last = first + framesPerChunk
      const directoryGrowth = kind === 'ordinal' && first > 0 ? 16 : 0
      let directory
      if (kind === 'ordinal') {
        // Rewrite logical directory endpoints while retaining codec block checksums.
        const { packBlockDirectory } = await import('../../dist/block-codec.js')
        directory = packBlockDirectory(example.directory.map((block) => ({ ...block,
          byteStart: block.byteStart + directoryGrowth,
          start: first + block.firstFrame === 0 ? zero : token(first + block.firstFrame),
          end: token(first + block.firstFrame + block.frameCount),
        })))
      } else {
        // The byte control's 1024 tiny frames fit one 64 KiB block. Its offset
        // strings are 33 bytes and its directory is exactly 122 bytes.
        const start = first === 0 ? zero : token(first), end = token(last)
        directory = Buffer.alloc(122)
        for (const [index, value] of [0, framesPerChunk, framesPerChunk * payloadBytes, 190, framesPerChunk * 34].entries()) directory.writeUInt32BE(value, index * 4)
        createHash('sha256').update(`${chunk}`).digest().copy(directory, 20)
        directory.writeUInt16BE(33, 52); directory.writeUInt16BE(33, 54)
        directory.write(start, 56); directory.write(end, 89)
      }
      inserts.push({ id, start: first === 0 ? zero : token(first), end: token(last), directory: directory.toString('hex'),
        sha: createHash('sha256').update(id).digest('hex'), encoded: kind === 'ordinal' ? example.bytes.length + directoryGrowth : 190 + framesPerChunk * 34 })
    }
    const before = (await pool.query('SELECT pg_current_wal_insert_lsn() AS lsn')).rows[0].lsn
    await pool.query(`INSERT INTO ${sql}.chunks(id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes,block_directory,object_key,sha256,encoded_bytes,sealed_at${kind === 'byte-control' ? ',end_deltas' : ''})
      SELECT x.id,$1::uuid,x.start,x.end,'sealed',$3,$4,decode(x.directory,'hex'),'v1/'||$5||'/'||$1::uuid::text||'/'||x.id||'/'||x.sha||'.ndsc',x.sha,x.encoded,clock_timestamp()
      ${kind === 'byte-control' ? ',ARRAY(SELECT n*37 FROM generate_series(1,$3::integer) n)' : ''}
      FROM jsonb_to_recordset($2::jsonb) x(id uuid,start text,"end" text,directory text,sha text,encoded integer)`,
    [stream, JSON.stringify(inserts), framesPerChunk, framesPerChunk * payloadBytes, installation.id])
    await pool.query(`INSERT INTO ${sql}.chunk_objects(object_key,stream_id,chunk_id,sha256,encoded_bytes) SELECT object_key,stream_id,id,sha256,encoded_bytes FROM ${sql}.chunks`)
    const wal = Number((await pool.query('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1) AS bytes', [before])).rows[0].bytes)
    const relations = []
    for (const name of ['appends', 'chunks', 'chunk_objects']) {
      await pool.query(`VACUUM (ANALYZE) ${sql}.${name}`)
      const sizes = (await pool.query(`SELECT pg_relation_size($1::regclass)::text AS heap_bytes,pg_indexes_size($1::regclass)::text AS index_bytes,pg_total_relation_size($1::regclass)::text AS total_bytes`, [`${sql}.${name}`])).rows[0]
      const live = (await pool.query(`SELECT count(*)::integer AS rows,COALESCE(sum(pg_column_size(t)),0)::text AS tuple_bytes FROM ${sql}.${name} t`)).rows[0]
      relations.push({ name, rows: live.rows, tupleBytes: Number(live.tuple_bytes), ...Object.fromEntries(Object.entries(sizes).map(([key, value]) => [key, Number(value)])) })
    }
    samples.push({ kind, framesPerChunk, sealedAppends: appends, payloadBytes, storageVersion: kind === 'ordinal' ? STORAGE_VERSION : 2,
      chunks: inserts.length, relations, observedBranchWalBytes: wal, directoryBytes: inserts.reduce((sum, row) => sum + row.directory.length / 2, 0),
      rawPerAppendDirectoryBytes: kind === 'byte-control' ? appends * 4 : 0,
      totalBytesAfterVacuum: relations.reduce((sum, relation) => sum + relation.total_bytes, 0) })
    console.log(JSON.stringify(samples.at(-1)))
  }
} finally {
  try { for (const schema of schemas) await pool.query(`DROP SCHEMA "${schema}" CASCADE`); cleanupComplete = true }
  finally { await pool.end() }
}
await writeFile(env.ORDINAL_METADATA_REPORT ?? 'qualification-results/replay-ordinal-metadata.json', JSON.stringify({
  sourceCommit: git('rev-parse', 'HEAD'), controlCommit: control, branchId: state.branchId,
  harnessSha256: createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex'),
  scope: 'Synthetic immutable manifests in actual constrained Neon schemas, 1048576 sealed appends of 32 bytes, after ordinary VACUUM ANALYZE. No object uploads or replay. Relation totals include heap/index/TOAST; excludes fixed installation, producer, subscription state, dead-tuple churn and provider history. WAL includes other branch activity and is not a per-append cost measurement.',
  cleanupComplete, samples,
}, null, 2) + '\n')
