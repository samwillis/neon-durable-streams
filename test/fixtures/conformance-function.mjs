// Test-only coordinator. Never included in the package or consumer example.
import { spawn } from 'node:child_process'
import { createHash, timingSafeEqual } from 'node:crypto'
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { unzipSync } from 'fflate'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'

const key = process.env.DS_CONFORMANCE_KEY
const digest = process.env.DS_CONFORMANCE_PAYLOAD_SHA
if (!key || !digest) throw new Error('Isolated conformance credentials required')
const sensitive = Object.entries(process.env).filter(([name, value]) => /SECRET|TOKEN|KEY|PASSWORD|DATABASE_URL/.test(name) && value?.length >= 8).map(([, value]) => value)
if (process.env.DURABLE_STREAMS_KEYRING) sensitive.push(...Object.values(JSON.parse(process.env.DURABLE_STREAMS_KEYRING).keys))
function redact(value) {
  let text = String(value)
  for (const secret of sensitive) text = text.replaceAll(secret, '[redacted]')
  return text.replace(/postgres(?:ql)?:\/\/[^\s'"<>]+/g, '[database URL]').replace(/dsw\.2\.[A-Za-z0-9_-]{1,128}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}|dsw\.[0-9a-f-]{36}\.[A-Za-z0-9_-]{43}/gi, '[wake token]')
}
function authorized(request) {
  const supplied = Buffer.from(request.headers.get('authorization') ?? '')
  const expected = Buffer.from(`Bearer ${key}`)
  return supplied.length === expected.length && timingSafeEqual(supplied, expected)
}
let busy = false
let active
let forceStop
function stop() {
  if (!active?.pid) return
  const pid = active.pid
  const kill = (signal) => { try { process.kill(-pid, signal) } catch (error) { if (error.code !== 'ESRCH') throw error } }
  kill('SIGTERM')
  forceStop ??= setTimeout(() => kill('SIGKILL'), 5000).unref()
}
process.once('SIGINT', stop)
export default {
  async fetch(request) {
    if (!authorized(request)) return new Response(null, { status: 401 })
    if (request.method === 'GET') return Response.json({ ready: true, payloadSha256: digest, node: process.version, arch: process.arch, platform: process.platform,
      glibc: process.report.getReport().header.glibcVersionRuntime ?? null })
    if (request.method !== 'POST') return new Response(null, { status: 405 })
    if (busy) return new Response(null, { status: 409 })
    busy = true
    let cancel
    const body = new ReadableStream({
      start(controller) {
        let disconnected = false, logBytes = 0
        const encoder = new TextEncoder()
        const emit = (event) => { if (!disconnected) controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`)) }
        const abort = () => { disconnected = true; stop() }
        cancel = abort
        request.signal.addEventListener('abort', abort, { once: true })
        const heartbeat = setInterval(() => emit({ type: 'heartbeat' }), 10_000)
        const deadline = setTimeout(stop, 20 * 60_000)
        const task = async () => {
          let directory
          try {
            emit({ type: 'started', node: process.version, arch: process.arch, platform: process.platform })
            const s3 = new S3Client({ endpoint: process.env.AWS_ENDPOINT_URL_S3, region: process.env.AWS_REGION, forcePathStyle: true,
              requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED' })
            let payload
            try {
              const object = await s3.send(new GetObjectCommand({ Bucket: process.env.NEON_TEST_BUCKET, Key: process.env.DS_CONFORMANCE_PAYLOAD_KEY }), { abortSignal: AbortSignal.timeout(120_000) })
              if (!object.Body || object.ContentLength !== Number(process.env.DS_CONFORMANCE_PAYLOAD_BYTES)) throw new Error('Invalid conformance artifact size')
              payload = await object.Body.transformToByteArray()
            } finally { s3.destroy() }
            if (createHash('sha256').update(payload).digest('hex') !== digest) throw new Error('Conformance artifact hash mismatch')
            directory = await mkdtemp(join(tmpdir(), 'ds-conformance-'))
            const entries = unzipSync(payload)
            const safe = (path) => { const result = resolve(directory, path); if (!result.startsWith(`${directory}/`)) throw new Error('Invalid artifact path'); return result }
            for (const [path, bytes] of Object.entries(entries)) {
              const target = safe(path); await mkdir(dirname(target), { recursive: true }); await writeFile(target, bytes)
            }
            for (const path of JSON.parse(new TextDecoder().decode(entries['.artifact-executable.json']))) await chmod(safe(path), 0o755)
            for (const [path, target] of Object.entries(JSON.parse(new TextDecoder().decode(entries['.artifact-links.json'])))) {
              const link = safe(path); safe(join(dirname(path), target)); await mkdir(dirname(link), { recursive: true }); await symlink(target, link)
            }
            const nativeFiles = JSON.parse(new TextDecoder().decode(entries['.artifact-native.json']))
            for (const [path, target] of Object.entries(nativeFiles)) {
              if (!/^native\/[a-f0-9]{64}\.(node|bin)$/.test(target)) throw new Error('Invalid native artifact path')
              const deployed = fileURLToPath(new URL(`./${target}`, import.meta.url))
              if (createHash('sha256').update(await readFile(deployed)).digest('hex') !== target.split('/')[1].split('.')[0]) throw new Error('Native artifact hash mismatch')
              const link = safe(path); await mkdir(dirname(link), { recursive: true }); await symlink(deployed, link)
            }
            if (disconnected) return
            const started = performance.now()
            // No patterns, exclusions, retries, clock mocking, or upstream edits.
            const child = spawn(process.execPath, ['node_modules/vitest/vitest.mjs', 'run', '--project', 'conformance', '--reporter=default', '--reporter=json', '--outputFile=conformance-results.json'], {
              cwd: directory, env: { ...process.env, NODE_OPTIONS: '', CI: 'true', NO_COLOR: '1', NAPI_RS_FORCE_WASI: 'error' }, stdio: ['ignore', 'pipe', 'pipe'], detached: true,
            })
            active = child
            // Redact complete lines, so a credential split between transport
            // chunks cannot bypass redaction. Oversized logs are dropped.
            for (const stream of [child.stdout, child.stderr]) {
              let pending = ''
              stream.setEncoding('utf8')
              stream.on('data', (text) => {
                logBytes += Buffer.byteLength(text)
                if (logBytes > 1024 * 1024) { pending = ''; return }
                pending += text
                const newline = pending.lastIndexOf('\n')
                if (newline >= 0) { emit({ type: 'log', text: redact(pending.slice(0, newline + 1)) }); pending = pending.slice(newline + 1) }
              })
              stream.on('end', () => { if (pending) emit({ type: 'log', text: redact(pending) }) })
            }
            const result = await new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => resolve({ code, signal })) })
            active = undefined
            const raw = await readFile(join(directory, 'conformance-results.json'), 'utf8')
            if (raw.length > 8 * 1024 * 1024) throw new Error('Conformance report exceeds bound')
            const sanitize = (value) => typeof value === 'string' ? redact(value) : Array.isArray(value) ? value.map(sanitize)
              : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([name, field]) => [name, sanitize(field)])) : value
            const report = sanitize(JSON.parse(raw))
            const passed = result.code === 0 && report.success === true && report.numTotalTests === 338 && report.numPassedTests === 338 && report.numFailedTests === 0 && report.numPendingTests === 0 && report.numTodoTests === 0
            emit({ type: 'result', passed, ...result, durationMs: Math.round(performance.now() - started), report })
          } catch (error) { emit({ type: 'error', message: redact(error?.stack ?? error) }) }
          finally {
            try { if (directory) await rm(directory, { recursive: true, force: true }) }
            catch (error) { emit({ type: 'error', message: `Coordinator cleanup failed: ${redact(error.message)}` }) }
            clearInterval(heartbeat); clearTimeout(deadline); request.signal.removeEventListener('abort', abort)
            clearTimeout(forceStop); forceStop = undefined
            busy = false
            if (!disconnected) controller.close()
          }
        }
        void task()
      },
      cancel() { cancel?.() },
    })
    return new Response(body, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store' } })
  },
}
