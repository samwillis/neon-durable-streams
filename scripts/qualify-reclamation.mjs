import { createHash, randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import pg from 'pg'
import { ChunkNotFoundError } from '../dist/index.js'
import { createS3ChunkStorage } from '../dist/s3.js'

// Disposable lifecycle probe. Historical recovery is provider-owned; this
// tests current fork safety, real object deletion, and bounded maintenance.
const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8'))
if (state.branchId !== env.NEON_TEST_BRANCH_ID || state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.TEST_FUNCTION_BRANCH_ID
  || !state.name.startsWith('ds-test-') || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA ?? '')
  || !env.TEST_FUNCTION_MAINTENANCE_KEY || !env.TEST_FUNCTION_API_KEY) throw new Error('Recorded isolated Function required')
const seconds = Number(env.DS_RECLAMATION_SECONDS ?? 600), intervalMs = 10_000
if (!Number.isSafeInteger(seconds) || seconds < 30 || seconds > 3600) throw new RangeError('Collection qualification needs 30–3600 seconds')
const schema = `"${env.TEST_FUNCTION_SCHEMA}"`, run = `reclaim-${randomUUID()}`, records = [], samples = [], errors = []
const startedAt = new Date().toISOString(), started = performance.now()
const harnessCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
const harnessSha256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2, connectionTimeoutMillis: 10_000, query_timeout: 15_000 })
pool.on('error', (error) => errors.push({ phase: 'database', name: error.name }))
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
let stopped = false
process.once('SIGINT', () => { stopped = true })
process.once('SIGTERM', () => { stopped = true })
const request = (path, method = 'GET', body, headers = {}) => fetch(new URL(path, env.TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, 'Content-Type': 'application/octet-stream', ...headers },
  ...(body === undefined ? {} : { body }), signal: AbortSignal.timeout(30_000),
})
const expectStatus = async (response, status) => {
  if (response.status !== status) { await response.arrayBuffer(); throw new Error(`Fixture HTTP ${response.status}; expected ${status}`) }
  return response
}
const maintain = async () => {
  const response = await expectStatus(await request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY }), 200)
  return response.json()
}
const stream = (path) => `v1/stream/${path}`
async function sample() {
  // Check every retained key independently; ledger rows may already be pruned.
  for (const record of records.filter((value) => !value.collectedAt)) {
    try { await objects.get(record.objectKey, record.encodedBytes) }
    catch (error) {
      if (!(error instanceof ChunkNotFoundError)) throw error
      record.collectedAt = new Date().toISOString()
    }
  }
  const counts = (await pool.query(`SELECT
    (SELECT count(*) FROM ${schema}.streams WHERE path LIKE $1) AS generations,
    (SELECT count(*) FROM ${schema}.paths WHERE path LIKE $1) AS namespaces,
    (SELECT count(*) FROM ${schema}.chunks WHERE stream_id=ANY($2::uuid[])) AS chunks,
    (SELECT count(*) FROM ${schema}.chunk_objects WHERE stream_id=ANY($2::uuid[])) AS receipts`, [`${run}/%`, records.map((record) => record.streamId)])).rows[0]
  samples.push({ elapsedSeconds: (performance.now() - started) / 1000, created: records.length,
    verifiedMissing: records.filter((record) => record.collectedAt).length,
    generations: Number(counts.generations), namespaces: Number(counts.namespaces), chunks: Number(counts.chunks), objectReceipts: Number(counts.receipts) })
  return samples.at(-1)
}
try {
  if (!(await pool.query(`SELECT 1 FROM ${schema}.migrations WHERE id='0004_subscriptions'`)).rowCount
    || !(await pool.query(`SELECT enabled FROM ${schema}.reclamation_policy`)).rows[0]?.enabled) throw new Error('Current baseline with explicit collection opt-in required')
  for (let sequence = 0; !stopped && performance.now() - started < seconds * 1000; sequence++) {
    const cycle = performance.now(), path = `${run}/${sequence}`, child = `${path}-fork`, payload = Buffer.alloc(65536, sequence % 256)
    payload.write(`${run}:${sequence}`)
    await (await expectStatus(await request(stream(path), 'PUT', payload), 201)).arrayBuffer()
    let manifest
    for (let n = 0; n < 8; n++) {
      await maintain()
      manifest = (await pool.query(`SELECT c.* FROM ${schema}.chunks c JOIN ${schema}.streams s ON s.id=c.stream_id
        WHERE s.path=$1 AND s.tenant_id='function-a' AND c.state='sealed'`, [path])).rows[0]
      if (manifest) break
    }
    if (!manifest) throw new Error('Ephemeral payload did not seal')
    await (await expectStatus(await request(stream(child), 'PUT', undefined, { 'Stream-Forked-From': `/v1/stream/${path}` }), 201)).arrayBuffer()
    records.push({ sequence, path, streamId: manifest.stream_id, chunkId: manifest.id, objectKey: manifest.object_key, encodedBytes: manifest.encoded_bytes })
    await (await expectStatus(await request(stream(path), 'DELETE'), 204)).arrayBuffer()
    await maintain()
    const replay = await expectStatus(await request(stream(child)), 200)
    if (!Buffer.from(await replay.arrayBuffer()).equals(payload)) throw new Error('Deleted parent fork lost exact sealed bytes')
    await (await expectStatus(await request(stream(child), 'DELETE'), 204)).arrayBuffer()
    if (sequence % 3 === 0) console.log(JSON.stringify(await sample()))
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, intervalMs - (performance.now() - cycle))))
  }
  // Explicitly measure the bounded post-load drain instead of assuming a cron
  // alone kept pace. This is a lifecycle probe, not scheduler capacity proof.
  const drainStarted = performance.now()
  let drained = false
  for (let n = 0; n < 128; n++) {
    await maintain()
    const current = await sample()
    if (current.verifiedMissing === records.length && !current.generations && !current.namespaces && !current.chunks && !current.objectReceipts) { drained = true; break }
    if (performance.now() - drainStarted > 120_000) break
  }
  if (stopped || !records.length || !drained) throw new Error('Collection/metadata drain did not complete within the probe budget')
} catch (error) { errors.push({ phase: 'collection-qualification', name: error.name, message: error.message }) }
finally {
  objects.close(); await pool.end()
  const verifiedMissing = records.filter((record) => record.collectedAt).length
  const report = { generatedAt: new Date().toISOString(), startedAt, harnessCommit, harnessSha256,
    deployedSourceCommit: env.TEST_FUNCTION_SOURCE_COMMIT, deployedSourceDirty: env.TEST_FUNCTION_SOURCE_DIRTY,
    bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA, deploymentId: env.TEST_FUNCTION_DEPLOYMENT_ID,
    profile: { seconds, intervalMs, recovery: 'provider-owned', sourceBytes: 65536, forksPerSource: 1, explicitMaintenance: true },
    run, elapsedSeconds: (performance.now() - started) / 1000, created: records.length, verifiedMissing, records, samples, errors, passed: errors.length === 0 }
  await mkdir('qualification-results', { recursive: true })
  await writeFile(`qualification-results/${run}.json`, JSON.stringify(report, null, 2) + '\n')
  console.log(JSON.stringify({ run, passed: report.passed, created: records.length, verifiedMissing, errors }))
  if (!report.passed) process.exitCode = 1
}
