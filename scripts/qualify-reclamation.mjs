import { createHash, randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import pg from 'pg'
import { ChunkNotFoundError } from '../dist/index.js'
import { createS3ChunkStorage } from '../dist/s3.js'

// Run alongside qualify-load.mjs on the same reserved, fresh test deployment.
// This creates/deletes only its UUID paths; policy deadlines are never changed.
const env = process.env, stateFile = env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json'
const state = JSON.parse(await readFile(stateFile, 'utf8'))
if (state.branchId !== env.NEON_TEST_BRANCH_ID || state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.TEST_FUNCTION_BRANCH_ID || !state.name.startsWith('ds-test-')
  || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA ?? '') || !env.TEST_FUNCTION_MAINTENANCE_KEY || !env.TEST_FUNCTION_API_KEY) throw new Error('Recorded isolated Function required')
const seconds = Number(env.DS_RECLAMATION_SECONDS ?? 2700), intervalMs = 10_000
if (!Number.isSafeInteger(seconds) || seconds < 1500 || seconds > 3600) throw new RangeError('Reclamation qualification needs 1500–3600 seconds of real elapsed time')
const schema = `"${env.TEST_FUNCTION_SCHEMA}"`, run = `reclaim-${randomUUID()}`, records = [], samples = [], errors = []
const startedAt = new Date().toISOString(), started = performance.now(), sourceCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
const harnessSha256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2, connectionTimeoutMillis: 10_000, query_timeout: 15_000 })
pool.on('error', (error) => errors.push({ phase: 'database', name: error.name }))
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
let checkpoint, verifiedMissing = 0, observedCollected = 0, stopped = false, report
process.once('SIGINT', () => { stopped = true })
process.once('SIGTERM', () => { stopped = true })
const request = (path, method = 'GET', body, maintenance = false) => fetch(new URL(path, env.TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, 'Content-Type': 'application/octet-stream',
    ...(maintenance ? { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY } : {}) },
  ...(body === undefined ? {} : { body }), signal: AbortSignal.timeout(30_000),
})
const maintenance = async () => {
  const response = await request('_maintenance', 'POST', undefined, true)
  if (!response.ok) throw new Error(`Maintenance HTTP ${response.status}`)
  return response.json()
}
const stream = (path) => `v1/stream/${path}`
async function sample() {
  const ledger = (await pool.query(`SELECT *,EXTRACT(epoch FROM (collect_after-clock_timestamp()))*1000 AS until_collection_ms
    FROM ${schema}.chunk_objects WHERE stream_id=ANY($1::uuid[])`, [records.map((record) => record.streamId)])).rows
  for (const object of ledger) {
    const record = records.find((value) => value.objectKey === object.object_key)
    if (!record) throw new Error('Unexpected immutable identity in collection ledger')
    if (object.quarantine_reason) throw new Error('Own exact object was quarantined')
    if (object.retired_at) {
      if (object.collect_after.getTime()-object.retired_at.getTime()<1_199_999) throw new Error('Persisted collection horizon was shortened')
      record.retiredAt = object.retired_at.toISOString(); record.collectAfter = object.collect_after.toISOString()
    }
    if (object.collected_at) {
      if (object.collected_at<object.collect_after) throw new Error('Object collected before its persisted horizon')
      record.collectedAt = object.collected_at.toISOString()
    }
    // A recovery point just before the first collection remains inside the
    // declared five-minute recovery window when the restore drill starts.
    if (!checkpoint && record === records[0] && object.retired_at && !object.collected_at && Number(object.until_collection_ms)>0 && Number(object.until_collection_ms)<60_000) {
      const point = (await pool.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn
      for (let n=0; n<100; n++) {
        if ((await pool.query('SELECT pg_current_wal_flush_lsn()>=$1::pg_lsn AS flushed', [point])).rows[0].flushed) break
        if (n===99) throw new Error('Recovery checkpoint did not flush')
        await new Promise((resolve) => setTimeout(resolve,100))
      }
      checkpoint = { lsn: point, recordedAt: new Date().toISOString(), schema: env.TEST_FUNCTION_SCHEMA, installationId: (await pool.query(`SELECT id FROM ${schema}.installation`)).rows[0].id,
        retiredChunk: { ...record }, projectId: state.projectId, branchId: state.branchId, branchName: state.name }
      await writeFile('.neon-test-state-reclamation-checkpoint.json',JSON.stringify(checkpoint,null,2)+'\n',{ mode: 0o600, flag: 'wx' })
      console.log('Recorded flushed recovery point before first collection; it contains the retired manifest.')
    }
  }
  observedCollected = records.filter((record) => record.collectedAt).length
  const count = (await pool.query(`SELECT
    (SELECT count(*) FROM ${schema}.streams WHERE path LIKE $1) AS generations,
    (SELECT count(*) FROM ${schema}.paths WHERE path LIKE $1) AS namespaces,
    (SELECT count(*) FROM ${schema}.chunks WHERE stream_id=ANY($2::uuid[])) AS chunks,
    (SELECT count(*) FROM ${schema}.chunk_objects WHERE stream_id=ANY($2::uuid[])) AS object_receipts`, [`${run}/%`,records.map((record) => record.streamId)])).rows[0]
  const health = await request('health','GET',undefined,true)
  if (!health.ok) throw new Error(`Runtime sampling HTTP ${health.status}`)
  const runtime = await health.json()
  if (runtime.build!==env.TEST_FUNCTION_BUNDLE_SHA) throw new Error('Runtime build changed during collection')
  const current = { elapsedSeconds:(performance.now()-started)/1000,created:records.length,collected:observedCollected,
    generations:Number(count.generations),namespaces:Number(count.namespaces),chunks:Number(count.chunks),objectReceipts:Number(count.object_receipts),runtime:runtime.runtime??null }
  samples.push(current)
  // Finite workload envelope: 20 minutes retained at one generation pair per
  // ten seconds, plus two minutes of maintenance slack and sixteen rows.
  if (current.generations>280 || current.namespaces>280 || current.objectReceipts>260) throw new Error('Ephemeral metadata exceeded the declared profile bound')
  console.log(JSON.stringify(current))
}
try {
  const policy = (await pool.query(`SELECT *,hold_until<=clock_timestamp() AS elapsed FROM ${schema}.reclamation_policy`)).rows[0]
  if (!policy?.enabled || Number(policy.recovery_window_ms)!==300_000 || policy.safety_margin_ms!==900_000) throw new Error('Explicit five-minute recovery plus fifteen-minute safety policy required')
  for (let sequence=0; !stopped && performance.now()-started<seconds*1000; sequence++) {
    const cycle = performance.now(), path = `${run}/${sequence}`, child = `${path}-fork`, payload = Buffer.alloc(65536,sequence%256)
    payload.write(`${run}:${sequence}`)
    const created = await request(stream(path),'PUT',payload)
    await created.arrayBuffer()
    if (created.status!==201) throw new Error(`Ephemeral create HTTP ${created.status}`)
    let manifest
    for (let n=0;n<8;n++) {
      await maintenance()
      manifest = (await pool.query(`SELECT c.* FROM ${schema}.chunks c JOIN ${schema}.streams s ON s.id=c.stream_id WHERE s.path=$1 AND s.tenant_id='function-a' AND c.state='sealed'`,[path])).rows[0]
      if (manifest) break
    }
    if (!manifest) throw new Error('Ephemeral payload did not reach sealed storage')
    const fork = await fetch(new URL(stream(child),env.TEST_FUNCTION_URL),{ method:'PUT',headers:{ Authorization:`Bearer ${env.TEST_FUNCTION_API_KEY}`,'Stream-Forked-From':`/v1/stream/${path}` },signal:AbortSignal.timeout(30_000) })
    await fork.arrayBuffer()
    if (fork.status!==201) throw new Error(`Ephemeral fork HTTP ${fork.status}`)
    const record = { sequence,path,streamId:manifest.stream_id,chunkId:manifest.id,objectKey:manifest.object_key,encodedBytes:manifest.encoded_bytes }
    records.push(record)
    const deleted = await request(stream(path),'DELETE'); await deleted.arrayBuffer()
    if (deleted.status!==204) throw new Error(`Ephemeral source delete HTTP ${deleted.status}`)
    const replay = await request(stream(child))
    if (replay.status!==200 || !Buffer.from(await replay.arrayBuffer()).equals(payload)) throw new Error('Deleted parent fork replay differs from exact sealed bytes')
    const detached = await request(stream(child),'DELETE'); await detached.arrayBuffer()
    if (detached.status!==204) throw new Error(`Ephemeral fork delete HTTP ${detached.status}`)
    if (sequence%3===0) await sample()
    await new Promise((resolve) => setTimeout(resolve,Math.max(0,intervalMs-(performance.now()-cycle))))
  }
  await maintenance(); await sample()
  for (const record of records.filter((value) => value.collectedAt)) {
    try { await objects.get(record.objectKey,record.encodedBytes); throw new Error('Collected object remains physically readable') }
    catch(error) { if (!(error instanceof ChunkNotFoundError)) throw error; verifiedMissing++ }
  }
  if (stopped || !observedCollected || verifiedMissing!==observedCollected) throw new Error('Real elapsed physical collection gate did not complete')
} catch(error) { errors.push({ phase:'collection-qualification',name:error.name,message:error.message }) }
finally {
  objects.close(); await pool.end()
  report = { generatedAt:new Date().toISOString(),startedAt,harnessCommit:sourceCommit,harnessSha256,deployedSourceCommit:env.TEST_FUNCTION_SOURCE_COMMIT,
    deployedSourceDirty:env.TEST_FUNCTION_SOURCE_DIRTY,bundleSha256:env.TEST_FUNCTION_BUNDLE_SHA,deploymentId:env.TEST_FUNCTION_DEPLOYMENT_ID,
    profile:{ seconds,intervalMs,recoveryWindowMs:300_000,safetyMarginMs:900_000,sourceBytes:65536,forksPerSource:1,storage:'actual Neon PG18/Realtime/Object Storage/Functions',deadlineBypasses:false },
    run,elapsedSeconds:(performance.now()-started)/1000,created:records.length,observedCollected,verifiedMissing,checkpoint,records,samples,errors,passed:errors.length===0&&observedCollected>0&&verifiedMissing===observedCollected }
  await mkdir('qualification-results',{recursive:true})
  await writeFile(`qualification-results/${run}.json`,JSON.stringify(report,null,2)+'\n')
}
console.log(JSON.stringify({ run,passed:report.passed,created:records.length,observedCollected,verifiedMissing,errors }))
if (!report.passed) process.exitCode=1
