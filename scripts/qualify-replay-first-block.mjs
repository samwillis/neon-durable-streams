import assert from 'node:assert/strict'
import { randomUUID, createHash } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'
import { expectedReplay } from '../test/fixtures/replay-data.mjs'

const env = process.env
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state-replay.json', 'utf8'))
if (state.branchId !== env.NEON_TEST_BRANCH_ID || state.branchId !== env.TEST_FUNCTION_BRANCH_ID || !state.name.startsWith('ds-test-')) throw new Error('Recorded replay environment required')
const request = (path, options = {}) => fetch(new URL(path, env.TEST_FUNCTION_URL), { ...options,
  headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, ...options.headers }, signal: AbortSignal.timeout(180000) })
const health = await (await request('_benchmark/health')).json()
assert.equal(health.build, env.TEST_FUNCTION_BUNDLE_SHA)
assert.equal(health.settings.firstBlockProbe, true)
assert.equal(health.settings.blockBytes, 65536)
const run = `replay-${randomUUID()}`
const seeded = await request('_benchmark/seed', { method: 'POST', body: JSON.stringify({ run, profile: 'binary', frames: 256 }) })
assert.equal(seeded.status, 200)
const dataset = await seeded.json()
for (let pass = 0; pass < 3; pass++) {
  const sealed = await request('_benchmark/maintain', { method: 'POST' })
  assert.equal(sealed.status, 200); await sealed.arrayBuffer()
}
const samples = []
async function page(from, narrow, mode) {
  const started = performance.now()
  const response = await request(`v1/stream/${dataset.path}?offset=${encodeURIComponent(dataset.offsets[from])}`, {
    headers: { 'x-replay-cache': 'cold', ...(narrow ? { 'x-replay-first-block': '1' } : {}) },
  })
  assert.equal(response.status, 200)
  const headerMs = performance.now() - started
  const reader = response.body.getReader(), parts = []
  let firstByteMs
  for (;;) {
    const part = await reader.read()
    if (part.done) break
    firstByteMs ??= performance.now() - started
    parts.push(part.value)
  }
  const next = dataset.offsets.indexOf(response.headers.get('Stream-Next-Offset'))
  const bytes = Buffer.concat(parts)
  assert.ok(next > from)
  assert.deepEqual(bytes, expectedReplay('binary', from, next))
  if (narrow) assert.ok(bytes.length <= 65536)
  samples.push({ mode, from, next, pageBytes: narrow ? 65536 : 2097152, headerMs, firstByteMs,
    completeMs: performance.now() - started, deliveredBytes: bytes.length, metrics: JSON.parse(response.headers.get('x-replay-metrics')) })
  return next
}
for (let repetition = 0; repetition < 3; repetition++) {
  for (const from of [0, 32, 126]) { await page(from, false, 'normal-seek'); await page(from, true, 'first-block-seek') }
}
const scans = []
for (const narrow of [false, true]) {
  let from = 0, pages = 0
  const started = performance.now(), sampleStart = samples.length
  while (from < dataset.frames) {
    from = await page(from, narrow, narrow ? 'first-block-scan' : 'normal-scan')
    if (++pages % 16 === 0) console.log(`Verified ${pages} ${narrow ? 'first-block' : 'normal'} scan pages`)
  }
  scans.push({ pageBytes: narrow ? 65536 : 2097152, pages, durationMs: performance.now() - started,
    sqlCalls: samples.slice(sampleStart).reduce((sum, sample) => sum + sample.metrics.sqlCalls, 0),
    objectGets: samples.slice(sampleStart).reduce((sum, sample) => sum + sample.metrics.objectGets, 0) })
}
await writeFile(env.REPLAY_REPORT ?? 'qualification-results/replay-first-block.json', JSON.stringify({
  sourceCommit: env.TEST_FUNCTION_SOURCE_COMMIT, bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA, branchId: state.branchId,
  harnessSha256: createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex'),
  settings: health.settings, scans, samples,
  scope: 'Same deployed Function, deterministic 8 MiB binary stream of 32 KiB frames, cold cache per request. Compare a 2 MiB page with a 64 KiB page ending at a verified block. Both verify complete pages before headers. Remote coordinator timings include networking; this is not a capacity benchmark.',
}, null, 2) + '\n')
console.log(JSON.stringify({ passed: true, scans }))
