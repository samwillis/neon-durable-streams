import { createHash, randomUUID } from 'node:crypto'
import { mkdir, writeFile, readFile } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import pg from 'pg'
import { createNeonRealtimeNotifier } from '../dist/neon.js'

const env = process.env
if (!env.TEST_FUNCTION_URL || !env.TEST_FUNCTION_API_KEY || !env.TEST_FUNCTION_MAINTENANCE_KEY || !env.TEST_FUNCTION_SCHEMA
  || !env.TEST_FUNCTION_BUNDLE_SHA || !env.TEST_DATABASE_URL || !env.NEON_TEST_BRANCH_ID || env.TEST_FUNCTION_BRANCH_ID !== env.NEON_TEST_BRANCH_ID
  || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA) || !new URL(env.TEST_FUNCTION_URL).hostname.includes('.compute.')) throw new Error('Load qualification requires this run\'s isolated deployed Neon Function')
const integer = (name, fallback, min, max) => {
  const value = Number(env[name] ?? fallback)
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`Invalid ${name}`)
  return value
}
const seconds = integer('DS_LOAD_SECONDS', 60, 10, 3600)
const writers = integer('DS_LOAD_WRITERS', 4, 1, 32)
const readers = integer('DS_LOAD_LIVE_READERS', 16, 0, 64)
const streamCount = integer('DS_LOAD_STREAMS', 8, 1, 64)
const writingStreams = Math.min(writers, streamCount)
const readersOnWritingStreams = Array.from({ length: readers }, (_, index) => index % streamCount).filter((index) => index < writingStreams).length
const recordBytes = integer('DS_LOAD_PAYLOAD_BYTES', 1024, 128, 65536)
const realtimeProbe = env.DS_LOAD_REALTIME_PROBE === '1'
if (realtimeProbe && (!env.NEON_REALTIME_URL || !env.NEON_REALTIME_SECRET)) throw new Error('Realtime head probe requires this branch\'s Realtime credentials')
const maximum = 1_000_000
const run = `qualify-${randomUUID()}`
const paths = Array.from({ length: streamCount }, (_, i) => `${run}/${i}`)
const expected = new Set(), ackLatency = [], liveLatency = [], errors = []
const sent = new Map()
const latestAcknowledged = new Map(), readerOffsets = new Map()
const backlog = []
const headLatency = [], headPending = new Map(), latestHead = new Map(), headWatches = []
const liveChecks = Array.from({ length: readers }, () => ({ hash: createHash('sha256'), records: 0 }))
let verifiedLiveReaders = 0
let headNotifier
const coverHead = (path, offset, at) => {
  latestHead.set(path, { offset, at })
  const pending = headPending.get(path) ?? []
  headPending.set(path, pending.filter((record) => {
    if (record.offset > offset) return true
    headLatency.push(at - record.started)
    return false
  }))
}
let appends = 0, liveBytes = 0, rotations = 0, replayed = 0, stopping = false
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')
const harnessSha256 = digest(await readFile(new URL(import.meta.url)))
const harnessCommitAtStart = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
const request = (path, method = 'GET', body, headers = {}, signal = AbortSignal.timeout(30_000)) => fetch(new URL(path, env.TEST_FUNCTION_URL), {
  method, headers: { Authorization: `Bearer ${env.TEST_FUNCTION_API_KEY}`, 'Content-Type': 'application/octet-stream', ...headers },
  ...(body !== undefined ? { body } : {}), signal,
})
const stream = (path) => `v1/stream/${path}`
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL, max: 2, connectionTimeoutMillis: 10_000, query_timeout: 15_000 })
pool.on('error', (error) => errors.push({ phase: 'database', name: error.name }))
const liveAbort = new AbortController()
const tasks = []
const timings = (values) => {
  const sorted = values.toSorted((a, b) => a - b)
  const at = (p) => sorted.length ? Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] * 100) / 100 : null
  return { samples: sorted.length, p50: at(.5), p95: at(.95), p99: at(.99), max: sorted.at(-1) ?? null }
}
let started, finished, connectionsBefore, connectionsAfter, frozenAfter, report, drainSeconds, replaySeconds, liveDrainSeconds
let sampler, sampling
const sample = async (phase = 'writing') => {
  const row = (await pool.query(`SELECT
    (SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".chunks c JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = c.stream_id WHERE c.state = 'frozen' AND s.path LIKE $1) AS frozen_chunks,
    (SELECT COALESCE(sum(a.payload_bytes), 0) FROM "${env.TEST_FUNCTION_SCHEMA}".appends a JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE a.payload IS NOT NULL AND s.path LIKE $1) AS local_payload_bytes,
    (SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".stream_changes c JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = c.stream_id WHERE s.path LIKE $1) AS retained_changes,
    (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()) AS database_connections`, [`${run}/%`])).rows[0]
  const response = await request('health', 'GET', undefined, { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY })
  if (!response.ok) throw new Error(`Runtime sampling failed: HTTP ${response.status}`)
  const health = await response.json()
  if (health.build !== env.TEST_FUNCTION_BUNDLE_SHA) throw new Error('Runtime sampling reached a different build')
  backlog.push({ phase, elapsedSeconds: started ? (performance.now() - started) / 1000 : 0, frozenChunks: Number(row.frozen_chunks), localPayloadBytes: Number(row.local_payload_bytes),
    pendingChanges: Number(row.retained_changes), retainedChanges: Number(row.retained_changes), databaseConnections: Number(row.database_connections), runtime: health.runtime ?? null })
}
try {
  connectionsBefore = Number((await pool.query('SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()')).rows[0].count)
  for (const path of paths) {
    const response = await request(stream(path), 'PUT')
    if (response.status !== 201) throw new Error(`Create failed: HTTP ${response.status}`)
  }
  if (realtimeProbe) {
    headNotifier = createNeonRealtimeNotifier({ url: env.NEON_REALTIME_URL, secret: env.NEON_REALTIME_SECRET,
      database: new URL(env.TEST_DATABASE_URL).pathname.slice(1), onError: (error) => errors.push({ phase: 'head-probe', name: error.name }) })
    const streams = await pool.query(`SELECT id, path FROM "${env.TEST_FUNCTION_SCHEMA}".streams WHERE tenant_id='function-a' AND path=ANY($1::text[])`, [paths])
    for (const row of streams.rows) headWatches.push(headNotifier.watch({ schema: env.TEST_FUNCTION_SCHEMA, tenantId: 'function-a', streamId: row.id },
      (head) => { if (head) coverHead(row.path, head.offset, performance.now()) }))
    await Promise.all(headWatches.map((watch) => watch.ready))
  }
  // Establish actual SSE ingress before applying write load. Each reader keeps
  // its durable checkpoint across bounded server rotations.
  const ready = []
  for (let i = 0; i < readers; i++) {
    let connected
    ready.push(new Promise((resolve) => { connected = resolve }))
    tasks.push((async () => {
      let offset = '-1'
      const decoder = new TextDecoder()
      while (!stopping) {
        try {
          const response = await request(`${stream(paths[i % streamCount])}?live=sse&offset=${encodeURIComponent(offset)}`, 'GET', undefined, {}, liveAbort.signal)
          if (!response.ok || !response.body) throw new Error(`Live admission failed: HTTP ${response.status}`)
          connected(); let buffer = ''
          const reader = response.body.getReader()
          try {
            for (;;) {
              const part = await reader.read()
              if (part.done) { rotations++; break }
              buffer += decoder.decode(part.value, { stream: true })
              if (buffer.length > 8 * 1024 * 1024) throw new Error('SSE fixture buffer bound exceeded')
              let boundary
              while ((boundary = buffer.indexOf('\n\n')) !== -1) {
                const event = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2)
                const data = event.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5)).join('\n')
                if (event.startsWith('event: control')) { offset = JSON.parse(data).streamNextOffset; readerOffsets.set(i, offset) }
                if (event.startsWith('event: data')) {
                  const payload = Buffer.from(data, 'base64'); liveBytes += payload.length
                  if (payload.length % recordBytes) throw new Error('SSE payload lost a record boundary')
                  liveChecks[i].hash.update(payload)
                  liveChecks[i].records += payload.length / recordBytes
                  for (let start = 0; start < payload.length; start += recordBytes) {
                    const hash = digest(payload.subarray(start, start + recordBytes)), at = sent.get(hash)
                    if (at === undefined) throw new Error('Live delivery contains an unknown record')
                    if (liveLatency.length < maximum) liveLatency.push(performance.now() - at)
                  }
                }
              }
            }
          } finally { await reader.cancel(); reader.releaseLock() }
        } catch (error) {
          connected()
          if (!stopping) errors.push({ phase: 'live', name: error.name, message: error.message })
          break
        }
      }
    })())
  }
  await Promise.all(ready)
  started = performance.now()
  await sample('before-writes')
  sampler = setInterval(() => {
    if (sampling) return
    sampling = sample().catch((error) => { errors.push({ phase: 'sampling', name: error.name }) }).finally(() => { sampling = undefined })
  }, 30_000).unref()
  const deadline = started + seconds * 1000
  await Promise.all(Array.from({ length: writers }, async (_, writer) => {
    for (let sequence = 0; performance.now() < deadline; sequence++) {
      if (appends >= maximum) { errors.push({ phase: 'append', message: 'Qualification record bound reached' }); break }
      const payload = Buffer.alloc(recordBytes, writer % 256)
      payload.write(JSON.stringify({ run, writer, sequence }), 0, recordBytes, 'utf8')
      const hash = digest(payload), begin = performance.now()
      sent.set(hash, begin)
      try {
        const response = await request(stream(paths[writer % streamCount]), 'POST', payload, {
          'Producer-Id': `${run}-${writer}`, 'Producer-Epoch': '0', 'Producer-Seq': String(sequence),
        })
        await response.arrayBuffer()
        if (response.status !== 200 && response.status !== 204) throw new Error(`Append failed: HTTP ${response.status}`)
        ackLatency.push(performance.now() - begin); expected.add(hash); appends++
        const path = paths[writer % streamCount], offset = response.headers.get('stream-next-offset')
        if (!offset) throw new Error('Append response has no checkpoint')
        if (!latestAcknowledged.has(path) || latestAcknowledged.get(path) < offset) latestAcknowledged.set(path, offset)
        if (realtimeProbe) {
          const latest = latestHead.get(path)
          if (latest && latest.offset >= offset) headLatency.push(latest.at - begin)
          else { const pending = headPending.get(path) ?? []; pending.push({ offset, started: begin }); headPending.set(path, pending) }
        }
      } catch (error) { errors.push({ phase: 'append', name: error.name, message: error.message }); break }
    }
  }))
  finished = performance.now()
  clearInterval(sampler); await sampling; await sample('after-writes')
  // Let every reader reach the final acknowledged checkpoint before measuring
  // latency. Otherwise the last slow notifications would be censored at stop.
  const liveDrainStarted = performance.now(), liveDeadline = liveDrainStarted + 20_000
  const uncoveredReaders = () => Array.from({ length: readers }, (_, i) => i).filter((i) => {
    const end = latestAcknowledged.get(paths[i % streamCount])
    return end && (!readerOffsets.has(i) || readerOffsets.get(i) < end)
  }).length
  const uncoveredHeads = () => [...headPending.values()].reduce((n, pending) => n + pending.length, 0)
  while ((uncoveredReaders() || (realtimeProbe && uncoveredHeads())) && performance.now() < liveDeadline) await new Promise((resolve) => setTimeout(resolve, 100))
  liveDrainSeconds = (performance.now() - liveDrainStarted) / 1000
  if (uncoveredReaders()) errors.push({ phase: 'live', message: 'Readers did not reach all acknowledged checkpoints before drain deadline', count: uncoveredReaders() })
  if (realtimeProbe && uncoveredHeads()) errors.push({ phase: 'head-probe', message: 'Head coverage incomplete at drain deadline', count: uncoveredHeads() })
  stopping = true; liveAbort.abort(); await Promise.allSettled(tasks)
  const drainStarted = performance.now()
  // Qualify sealed replay as well as delivery: bounded maintenance must recover
  // every outstanding chunk before comparing complete immutable record sets.
  const drainDeadline = performance.now() + 90_000
  for (let i = 0; i < 128 && performance.now() < drainDeadline; i++) {
    const maintenance = await request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY })
    if (!maintenance.ok) throw new Error(`Maintenance failed: HTTP ${maintenance.status}`)
    const maintenanceStats = await maintenance.json()
    frozenAfter = Number((await pool.query(`SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".appends a JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE s.path LIKE $1 AND a.payload IS NOT NULL`, [`${run}/%`])).rows[0].count)
    if (!frozenAfter) break
    if (!maintenanceStats.sealed && !maintenanceStats.frozen) await new Promise((resolve) => setTimeout(resolve, 250))
  }
  const observed = new Set()
  const sequenceByWriter = new Map()
  drainSeconds = (performance.now() - drainStarted) / 1000
  const replayStarted = performance.now()
  const replayDigests = new Map()
  async function replay(path) {
    const ordered = createHash('sha256')
    let offset = '-1'
    for (let page = 0; page < maximum; page++) {
      const response = await request(`${stream(path)}?offset=${encodeURIComponent(offset)}`)
      if (!response.ok) throw new Error(`Replay failed: HTTP ${response.status}`)
      const bytes = Buffer.from(await response.arrayBuffer())
      if (bytes.length % recordBytes) throw new Error('Replay record boundary mismatch')
      ordered.update(bytes)
      for (let start = 0; start < bytes.length; start += recordBytes) {
        const hash = digest(bytes.subarray(start, start + recordBytes))
        if (observed.has(hash) || !expected.has(hash)) throw new Error('Replay contains duplicate or unacknowledged record')
        const record = bytes.subarray(start, start + recordBytes)
        const identity = JSON.parse(record.subarray(0, record.indexOf(125) + 1).toString('utf8'))
        if (identity.sequence !== (sequenceByWriter.get(identity.writer) ?? -1) + 1) throw new Error('Replay reordered a producer sequence')
        sequenceByWriter.set(identity.writer, identity.sequence)
        observed.add(hash); replayed++
      }
      if (response.headers.get('stream-up-to-date') === 'true') break
      const next = response.headers.get('stream-next-offset')
      if (!next || next === offset) throw new Error('Replay did not advance its checkpoint')
      offset = next
    }
    replayDigests.set(path, ordered.digest('hex'))
  }
  let nextPath = 0
  await Promise.all(Array.from({ length: Math.min(4, paths.length) }, async () => {
    for (;;) { const index = nextPath++; if (index >= paths.length) break; await replay(paths[index]) }
  }))
  replaySeconds = (performance.now() - replayStarted) / 1000
  if (observed.size !== expected.size) throw new Error('Replay lost acknowledged records')
  for (let i = 0; i < readers; i++) {
    if (liveChecks[i].hash.digest('hex') !== replayDigests.get(paths[i % streamCount])) throw new Error('Live delivery differs from ordered durable replay')
    verifiedLiveReaders++
  }
  if (frozenAfter) throw new Error('Maintenance did not seal all workload payloads')
  await sample('after-replay')
  connectionsAfter = Number((await pool.query('SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()')).rows[0].count)
} catch (error) { errors.push({ phase: 'qualification', name: error.name, message: error.message }) }
finally {
  clearInterval(sampler); await sampling
  stopping = true; liveAbort.abort(); await Promise.allSettled(tasks)
  for (const watch of headWatches) watch.close()
  headNotifier?.close()
  await pool.end()
  report = { harnessSha256, harnessCommitAtStart, generatedAt: new Date().toISOString(),
    deployedSourceCommit: env.TEST_FUNCTION_SOURCE_COMMIT ?? null, deployedSourceDirty: env.TEST_FUNCTION_SOURCE_DIRTY ?? null, deploymentId: env.TEST_FUNCTION_DEPLOYMENT_ID ?? null,
    bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA, node: process.version, profile: { seconds, writers, readers, streams: streamCount, writingStreams, readersOnWritingStreams, idleReaders: readers - readersOnWritingStreams, recordBytes, replayConcurrency: 4, realtimeProbe, maintenanceHints: env.TEST_FUNCTION_MAINTENANCE_HINTS ?? 'unknown', hasCron: env.TEST_FUNCTION_HAS_CRON ?? 'unknown', liveFallbackMs: env.TEST_FUNCTION_LIVE_FALLBACK_MS ?? null, liveValidationMs: env.TEST_FUNCTION_LIVE_VALIDATION_MS ?? null, cronChunks: env.TEST_FUNCTION_CRON_CHUNKS ?? null, cronSubscriptionWork: env.TEST_FUNCTION_CRON_SUBSCRIPTION_WORK ?? null, storage: 'actual Neon PG18/Realtime/Object Storage/Functions' },
    elapsedSeconds: started && finished ? (finished - started) / 1000 : null, acknowledgedAppends: appends, replayedRecords: replayed,
    appendsPerSecond: started && finished ? appends / ((finished - started) / 1000) : null,
    appendLatencyMs: timings(ackLatency), observedLiveLatencyMs: timings(liveLatency), observedHeadCoverageMs: timings(headLatency), liveBytes, rotations,
    liveVerification: { verifiedReaders: verifiedLiveReaders, receivedRecords: liveChecks.map((value) => value.records), orderedSha256MatchesReplay: verifiedLiveReaders === readers },
    connectionsBefore, connectionsAfter, backlog, liveDrainSeconds, drainSeconds, replaySeconds, remainingLocalPayloads: frozenAfter, errors, passed: errors.length === 0 && appends > 0 && replayed === appends && verifiedLiveReaders === readers && (!readers || liveLatency.length > 0) }
  await mkdir('qualification-results', { recursive: true })
  await writeFile(`qualification-results/${run}.json`, JSON.stringify(report, null, 2) + '\n')
}
console.log(JSON.stringify(report, null, 2))
if (!report.passed) process.exitCode = 1
