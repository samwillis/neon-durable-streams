import { createHash, randomUUID } from 'node:crypto'
import { mkdir, writeFile, readFile } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import pg from 'pg'

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
const recordBytes = integer('DS_LOAD_PAYLOAD_BYTES', 1024, 128, 65536)
const maximum = 1_000_000
const run = `qualify-${randomUUID()}`
const paths = Array.from({ length: streamCount }, (_, i) => `${run}/${i}`)
const expected = new Set(), ackLatency = [], liveLatency = [], errors = []
const sent = new Map()
const backlog = []
let appends = 0, liveBytes = 0, rotations = 0, replayed = 0, stopping = false
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')
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
let started, finished, connectionsBefore, connectionsAfter, frozenAfter, report
let sampler, sampling
const sample = async () => {
  const row = (await pool.query(`SELECT
    (SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".chunks c JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = c.stream_id WHERE c.state = 'frozen' AND s.path LIKE $1) AS frozen_chunks,
    (SELECT COALESCE(sum(a.payload_bytes), 0) FROM "${env.TEST_FUNCTION_SCHEMA}".appends a JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE a.payload IS NOT NULL AND s.path LIKE $1) AS local_payload_bytes,
    (SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".stream_changes c JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = c.stream_id WHERE NOT c.processed AND s.path LIKE $1) AS pending_changes`, [`${run}/%`])).rows[0]
  backlog.push({ elapsedSeconds: started ? (performance.now() - started) / 1000 : 0, frozenChunks: Number(row.frozen_chunks), localPayloadBytes: Number(row.local_payload_bytes), pendingChanges: Number(row.pending_changes) })
}
try {
  connectionsBefore = Number((await pool.query('SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()')).rows[0].count)
  for (const path of paths) {
    const response = await request(stream(path), 'PUT')
    if (response.status !== 201) throw new Error(`Create failed: HTTP ${response.status}`)
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
                if (event.startsWith('event: control')) offset = JSON.parse(data).streamNextOffset
                if (event.startsWith('event: data')) {
                  const payload = Buffer.from(data, 'base64'); liveBytes += payload.length
                  if (payload.length % recordBytes) throw new Error('SSE payload lost a record boundary')
                  for (let start = 0; start < payload.length; start += recordBytes) {
                    const hash = digest(payload.subarray(start, start + recordBytes)), at = sent.get(hash)
                    if (at !== undefined && liveLatency.length < maximum) liveLatency.push(performance.now() - at)
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
  await sample()
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
      } catch (error) { errors.push({ phase: 'append', name: error.name, message: error.message }); break }
    }
  }))
  finished = performance.now()
  clearInterval(sampler); await sampling; await sample()
  stopping = true; liveAbort.abort(); await Promise.allSettled(tasks)
  // Qualify sealed replay as well as delivery: bounded maintenance must recover
  // every outstanding chunk before comparing complete immutable record sets.
  for (let i = 0; i < 32; i++) {
    const maintenance = await request('_maintenance', 'POST', undefined, { 'x-ds-maintenance-key': env.TEST_FUNCTION_MAINTENANCE_KEY })
    if (!maintenance.ok) throw new Error(`Maintenance failed: HTTP ${maintenance.status}`)
    await maintenance.arrayBuffer()
    frozenAfter = Number((await pool.query(`SELECT count(*) FROM "${env.TEST_FUNCTION_SCHEMA}".appends a JOIN "${env.TEST_FUNCTION_SCHEMA}".streams s ON s.id = a.stream_id WHERE s.path LIKE $1 AND a.payload IS NOT NULL`, [`${run}/%`])).rows[0].count)
    if (!frozenAfter) break
  }
  const observed = new Set()
  const sequenceByWriter = new Map()
  for (const path of paths) {
    let offset = '-1'
    for (let page = 0; page < maximum; page++) {
      const response = await request(`${stream(path)}?offset=${encodeURIComponent(offset)}`)
      if (!response.ok) throw new Error(`Replay failed: HTTP ${response.status}`)
      const bytes = Buffer.from(await response.arrayBuffer())
      if (bytes.length % recordBytes) throw new Error('Replay record boundary mismatch')
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
      offset = response.headers.get('stream-next-offset')
    }
  }
  if (observed.size !== expected.size) throw new Error('Replay lost acknowledged records')
  if (frozenAfter) throw new Error('Maintenance did not seal all workload payloads')
  connectionsAfter = Number((await pool.query('SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()')).rows[0].count)
} catch (error) { errors.push({ phase: 'qualification', name: error.name, message: error.message }) }
finally {
  clearInterval(sampler); await sampling
  stopping = true; liveAbort.abort(); await Promise.allSettled(tasks); await pool.end()
  report = { harnessSha256: digest(await readFile(new URL(import.meta.url))), generatedAt: new Date().toISOString(), commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    bundleSha256: env.TEST_FUNCTION_BUNDLE_SHA, node: process.version, profile: { seconds, writers, readers, streams: streamCount, recordBytes, storage: 'actual Neon PG18/Realtime/Object Storage/Functions' },
    elapsedSeconds: started && finished ? (finished - started) / 1000 : null, acknowledgedAppends: appends, replayedRecords: replayed,
    appendsPerSecond: started && finished ? appends / ((finished - started) / 1000) : null,
    appendLatencyMs: timings(ackLatency), observedLiveLatencyMs: timings(liveLatency), liveBytes, rotations,
    connectionsBefore, connectionsAfter, backlog, remainingLocalPayloads: frozenAfter, errors, passed: errors.length === 0 && appends > 0 && replayed === appends && (!readers || liveLatency.length > 0) }
  await mkdir('qualification-results', { recursive: true })
  await writeFile(`qualification-results/${run}.json`, JSON.stringify(report, null, 2) + '\n')
}
console.log(JSON.stringify(report, null, 2))
if (!report.passed) process.exitCode = 1
