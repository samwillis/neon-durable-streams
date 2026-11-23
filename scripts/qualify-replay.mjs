import assert from 'node:assert/strict'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import pg from 'pg'
import { replayProfiles, replayFrame, expectedReplay } from '../test/fixtures/replay-data.mjs'

const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state-replay.json', 'utf8'))
if (state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.TEST_FUNCTION_BRANCH_ID || !state.name.startsWith('ds-test-')
  || env.TEST_FUNCTION_BRANCH_ID !== env.NEON_TEST_BRANCH_ID || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA ?? '')) throw new Error('Recorded replay branch and Function required')
const run = `replay-${randomUUID()}`
const repetitions = Number(env.REPLAY_REPETITIONS ?? 3)
if (!Number.isSafeInteger(repetitions) || repetitions < 2 || repetitions > 10) throw new Error('Invalid replay repetitions')
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, query_timeout: 30000 })
const request = (path, options = {}) => fetch(new URL(path, env.TEST_FUNCTION_URL), { ...options,
  headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, ...options.headers }, signal: options.signal ?? AbortSignal.timeout(180000) })
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')
const samples = []
const datasets = []
const errors = []
const started = performance.now()
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const quantiles = (values) => {
  const ordered = values.toSorted((a, b) => a - b)
  return Object.fromEntries([['count', values.length], ...[.5, .95, .99].map((q) => [`p${q * 100}`, ordered[Math.min(ordered.length - 1, Math.floor(q * ordered.length))] ?? null])])
}
let storageBefore, storageAfter

async function readPage(dataset, from, cache, mode = 'seek', extra = {}) {
  const url = `v1/stream/${dataset.path}?offset=${encodeURIComponent(dataset.offsets[from])}`
  const begin = performance.now()
  const response = await request(url, { headers: { 'x-replay-cache': cache, ...extra.headers } })
  const headerMs = performance.now() - begin
  const metrics = JSON.parse(response.headers.get('x-replay-metrics') ?? 'null')
  assert.ok(metrics, 'Missing per-request deployed metrics')
  assert.equal(response.status, extra.status ?? 200)
  const reader = response.body.getReader()
  const parts = []
  let firstByteMs, size = 0
  try {
    for (;;) {
      const part = await reader.read()
      if (part.done) break
      firstByteMs ??= performance.now() - begin
      size += part.value.byteLength
      assert.ok(size <= 3 * 1024 * 1024, 'Replay page exceeds benchmark limit')
      parts.push(part.value)
      if (extra.slow) await pause(10)
    }
  } finally { await reader.cancel(); reader.releaseLock() }
  const completeMs = performance.now() - begin
  const next = dataset.offsets.indexOf(response.headers.get('Stream-Next-Offset'))
  assert.ok(next >= from && next <= dataset.frames, 'Checkpoint is not an acknowledged frame boundary')
  const bytes = Buffer.concat(parts)
  if (response.status === 200) assert.deepEqual(bytes, expectedReplay(dataset.profile, from, next), 'Replay bytes differ from independent deterministic inputs')
  else assert.equal(bytes.length, 0)
  samples.push({ profile: dataset.profile, from, through: next, mode, headerMs, firstByteMs: firstByteMs ?? headerMs, completeMs,
    deliveredBytes: bytes.length, digest: digest(bytes), metrics })
  return { next, response }
}

try {
  const health = await (await request('_benchmark/health')).json()
  assert.equal(health.build, env.TEST_FUNCTION_BUNDLE_SHA)
  assert.equal(health.sourceCommit, env.TEST_FUNCTION_SOURCE_COMMIT)
  storageBefore = await (await request('_benchmark/storage')).json()
  for (const [profile, options] of Object.entries(replayProfiles)) {
    const response = await request('_benchmark/seed', { method: 'POST', body: JSON.stringify({ run, profile, frames: options.frames }), headers: { 'Content-Type': 'application/json' } })
    assert.equal(response.status, 200, `Dataset ${profile} creation failed`)
    const dataset = await response.json()
    assert.equal(dataset.offsets.length, dataset.frames + 1)
    assert.equal(new Set(dataset.offsets).size, dataset.offsets.length)
    assert.ok(dataset.offsets.every((offset, index) => !index || offset > dataset.offsets[index - 1]))
    datasets.push(dataset)
    console.log(`Seeded ${profile}: ${dataset.frames} public API appends`)
  }
  for (let pass = 0; pass < 12; pass++) {
    const response = await request('_benchmark/maintain', { method: 'POST' })
    assert.equal(response.status, 200)
    await response.arrayBuffer()
    const storage = await (await request('_benchmark/storage')).json()
    if (Number(storage.counts.appends) === 0 && Number(storage.counts.frozen) === 0) break
    assert.ok(pass < 11, 'Sealing did not drain')
  }
  await pool.query(`VACUUM (ANALYZE) "${env.TEST_FUNCTION_SCHEMA}".chunks`)
  await pool.query(`VACUUM (ANALYZE) "${env.TEST_FUNCTION_SCHEMA}".appends`)
  for (const dataset of datasets) {
    for (let repetition = 0; repetition < repetitions; repetition++) {
      const from = [Math.floor(dataset.frames / 2), Math.floor(dataset.frames / 3), Math.max(0, dataset.frames - 2)][repetition % 3]
      await readPage(dataset, from, 'cold', 'cold-seek')
      await readPage(dataset, from, 'shared', 'repeat-seek')
      const { response } = await readPage(dataset, from, 'shared', 'repeat-seek')
      await readPage(dataset, from, 'shared', 'conditional', { headers: { 'if-none-match': response.headers.get('etag') }, status: 304 })
    }
    for (let repetition = 0; repetition < repetitions; repetition++) {
      let from = 0, pages = 0
      while (from < dataset.frames) {
        const page = await readPage(dataset, from, 'shared', 'sequential')
        assert.ok(page.next > from, 'Replay made no progress')
        from = page.next
        assert.ok(++pages < 10000)
      }
    }
    await readPage(dataset, 0, 'shared', 'slow-reader', { slow: true })
    await Promise.all(Array.from({ length: 8 }, () => readPage(dataset, Math.floor(dataset.frames / 2), 'shared', 'fanout')))
    console.log(`Verified replay bytes and checkpoints for ${dataset.profile}`)
  }
  // Add write pressure to a sealed stream, then repeat with separate writers.
  for (const sameStream of [true, false]) {
    const dataset = datasets.find((item) => item.profile === 'binary')
    const writePath = sameStream ? dataset.path : `${run}/writer`
    if (!sameStream) assert.equal((await request(`v1/stream/${writePath}`, { method: 'PUT' })).status, 201)
    const writer = (async () => {
      for (let index = 0; index < 24; index++) {
        const begin = performance.now()
        const response = await request(`v1/stream/${writePath}`, { method: 'POST', body: replayFrame('binary', dataset.frames + index), headers: { 'Content-Type': 'application/octet-stream' } })
        await response.arrayBuffer()
        assert.equal(response.status, 204)
        samples.push({ mode: sameStream ? 'writer-shared-stream' : 'writer-separate-stream', completeMs: performance.now() - begin,
          metrics: JSON.parse(response.headers.get('x-replay-metrics')) })
      }
    })()
    await Promise.all([writer, ...Array.from({ length: 4 }, async () => {
      for (let index = 0; index < 8; index++) await readPage(dataset, 0, 'shared', sameStream ? 'reader-shared-stream' : 'reader-separate-stream')
    })])
  }
  storageAfter = await (await request('_benchmark/storage')).json()
} catch (error) {
  errors.push({ name: error.name, message: error.message })
  console.error(error.name, error.message)
} finally {
  await pool.end()
  const report = { sourceCommit: env.TEST_FUNCTION_SOURCE_COMMIT, bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA,
    deploymentId: env.TEST_FUNCTION_DEPLOYMENT_ID, branchId: env.NEON_TEST_BRANCH_ID, region: env.AWS_REGION,
    harnessSha256: digest(await readFile(new URL(import.meta.url))), generatedAt: new Date().toISOString(), run, seed: 20261123, repetitions,
    settings: JSON.parse(env.REPLAY_SETTINGS), durationMs: performance.now() - started, errors,
    scope: 'Public API appends and reads through deployed instrumented consumer. Per-request SQL/object metrics cover handler work through response headers. Disabled-cache requests create a separate server on the reached isolate. Instance IDs distinguish routed isolates. Client times include coordinator network. No open-loop capacity claim.',
    profiles: datasets.map((dataset) => ({ profile: dataset.profile, frames: dataset.frames })), storageBefore, storageAfter, samples,
    summary: [...new Set(samples.map((sample) => sample.mode))].map((mode) => ({ mode,
      latencyMs: quantiles(samples.filter((sample) => sample.mode === mode).map((sample) => sample.completeMs)),
      sqlCalls: quantiles(samples.filter((sample) => sample.mode === mode).map((sample) => sample.metrics.sqlCalls)),
      objectGets: samples.filter((sample) => sample.mode === mode).reduce((sum, sample) => sum + sample.metrics.objectGets, 0) })) }
  await mkdir('qualification-results', { recursive: true })
  const output = env.REPLAY_REPORT ?? `qualification-results/${run}.json`
  await writeFile(output, JSON.stringify(report, null, 2) + '\n')
  console.log(JSON.stringify({ output, passed: errors.length === 0, samples: samples.length, summary: report.summary }))
}
if (errors.length) process.exitCode = 1
