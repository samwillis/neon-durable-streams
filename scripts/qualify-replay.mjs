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
  const reader = response.body?.getReader()
  const parts = []
  let firstByteMs, size = 0
  try {
    while (reader) {
      const part = await reader.read()
      if (part.done) break
      firstByteMs ??= performance.now() - begin
      size += part.value.byteLength
      assert.ok(size <= 3 * 1024 * 1024, 'Replay page exceeds benchmark limit')
      parts.push(part.value)
      if (extra.slow) await pause(10)
    }
  } finally { if (reader) { await reader.cancel(); reader.releaseLock() } }
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
  for (let index = 0; index < 8; index++) {
    const path = `${run}/quiet-${index}`
    const created = await request(`v1/stream/${path}`, { method: 'PUT' })
    assert.equal(created.status, 201)
    const appended = await request(`v1/stream/${path}`, { method: 'POST', body: replayFrame('tiny', 0), headers: { 'Content-Type': 'application/octet-stream' } })
    assert.equal(appended.status, 204)
    datasets.push({ path, profile: 'tiny', frames: 1, offsets: [created.headers.get('Stream-Next-Offset'), appended.headers.get('Stream-Next-Offset')] })
  }
  const source = datasets.find((item) => item.profile === 'binary')
  let parent = source.path
  for (let depth = 0; depth < 16; depth++) {
    const path = `${run}/fork-${depth}`
    const response = await request(`v1/stream/${path}`, { method: 'PUT', headers: { 'Stream-Forked-From': `/v1/stream/${parent}`, 'Stream-Fork-Offset': source.offsets[128] } })
    assert.equal(response.status, 201)
    parent = path
  }
  const deepFork = { path: parent, profile: 'binary', frames: 128, offsets: source.offsets.slice(0, 129) }
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
  for (let repetition = 0; repetition < repetitions; repetition++) {
    await readPage(deepFork, 0, 'cold', 'deep-fork-cold')
    await readPage(deepFork, 64, 'shared', 'deep-fork-warm')
  }
  const binary = datasets.find((item) => item.profile === 'binary')
  for (let index = 0; index < 8; index++) {
    const begin = performance.now()
    const response = await request(`v1/stream/${binary.path}?offset=${encodeURIComponent(binary.offsets[0])}&live=sse`)
    assert.equal(response.status, 200)
    const reader = response.body.getReader()
    assert.equal((await reader.read()).done, false)
    await reader.cancel()
    reader.releaseLock()
    samples.push({ mode: 'canceled-sse', completeMs: performance.now() - begin, metrics: JSON.parse(response.headers.get('x-replay-metrics')) })
  }
  const catchup = await request(`v1/stream/${deepFork.path}?offset=${encodeURIComponent(deepFork.offsets[126])}&live=sse`)
  assert.equal(catchup.status, 200)
  const liveReader = catchup.body.getReader()
  const decoder = new TextDecoder()
  const liveDigest = createHash('sha256')
  let pending = '', appended = false, eof = false, liveNext
  const liveStarted = performance.now()
  try {
    while (!eof) {
      const part = await liveReader.read()
      assert.equal(part.done, false, 'SSE ended before its final checkpoint')
      pending += decoder.decode(part.value, { stream: true })
      assert.ok(pending.length < 4 * 1024 * 1024)
      let end
      while ((end = pending.indexOf('\n\n')) !== -1) {
        const event = pending.slice(0, end)
        pending = pending.slice(end + 2)
        const data = event.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5)).join('\n')
        if (event.startsWith('event: data')) liveDigest.update(Buffer.from(data, 'base64'))
        if (event.startsWith('event: control')) {
          const control = JSON.parse(data)
          if (control.upToDate && !appended) {
            const response = await request(`v1/stream/${deepFork.path}`, { method: 'POST', body: replayFrame('binary', 128),
              headers: { 'Content-Type': 'application/octet-stream', 'Stream-Closed': 'true' } })
            assert.equal(response.status, 204)
            liveNext = response.headers.get('Stream-Next-Offset')
            appended = true
          }
          if (control.streamClosed) { assert.equal(control.streamNextOffset, liveNext); eof = true }
        }
      }
    }
  } finally { await liveReader.cancel(); liveReader.releaseLock() }
  assert.equal(liveDigest.digest('hex'), digest(expectedReplay('binary', 126, 129)))
  samples.push({ mode: 'catchup-to-live', completeMs: performance.now() - liveStarted, metrics: JSON.parse(catchup.headers.get('x-replay-metrics')) })
  // Add write pressure to a sealed stream, then repeat with separate writers.
  for (const sameStream of [true, false]) {
    const dataset = datasets.find((item) => item.profile === 'binary')
    const writePath = sameStream ? dataset.path : `${run}/writer`
    let writeStart = dataset.offsets.at(-1)
    if (!sameStream) {
      const created = await request(`v1/stream/${writePath}`, { method: 'PUT' })
      assert.equal(created.status, 201)
      writeStart = created.headers.get('Stream-Next-Offset')
    }
    let writeEnd
    const writer = (async () => {
      for (let index = 0; index < 24; index++) {
        const begin = performance.now()
        const response = await request(`v1/stream/${writePath}`, { method: 'POST', body: replayFrame('binary', dataset.frames + index), headers: { 'Content-Type': 'application/octet-stream' } })
        await response.arrayBuffer()
        assert.equal(response.status, 204)
        writeEnd = response.headers.get('Stream-Next-Offset')
        samples.push({ mode: sameStream ? 'writer-shared-stream' : 'writer-separate-stream', completeMs: performance.now() - begin,
          metrics: JSON.parse(response.headers.get('x-replay-metrics')) })
      }
    })()
    await Promise.all([writer, ...Array.from({ length: 4 }, async () => {
      for (let index = 0; index < 8; index++) await readPage(dataset, 0, 'shared', sameStream ? 'reader-shared-stream' : 'reader-separate-stream')
    })])
    const replayed = await request(`v1/stream/${writePath}?offset=${encodeURIComponent(writeStart)}`)
    assert.equal(replayed.status, 200)
    assert.equal(replayed.headers.get('Stream-Next-Offset'), writeEnd)
    assert.deepEqual(Buffer.from(await replayed.arrayBuffer()), expectedReplay('binary', dataset.frames, dataset.frames + 24))
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
