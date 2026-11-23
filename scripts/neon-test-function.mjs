import { randomBytes, randomUUID, createHash } from 'node:crypto'
import { appendFile, mkdir, readFile, writeFile } from 'node:fs/promises'
import { build } from 'esbuild'
import { zipSync } from 'fflate'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { execFileSync } from 'node:child_process'
import { deployTestWebhook } from './neon-test-webhook.mjs'
import { migrate, MIGRATION_IDS } from '../dist/migrations.js'
import { createS3ChunkStorage } from '../dist/s3.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../dist/subscriptions.js'

const projectId = process.env.NEON_PROJECT_ID
const apiKey = process.env.NEON_API_KEY
const branchId = process.env.NEON_TEST_BRANCH_ID
const stateFile = process.env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json'
const outputFile = process.env.NEON_TEST_FUNCTION_ENV_FILE ?? '.env.neon.function'
if (!projectId || !apiKey || !branchId || !process.env.TEST_DATABASE_URL_UNPOOLED || !process.env.NEON_REALTIME_URL || !process.env.NEON_REALTIME_SECRET || !process.env.NEON_TEST_BUCKET || !process.env.DURABLE_STREAMS_KEYRING) throw new Error('Complete isolated Neon test environment required before Function deployment')
const state = JSON.parse(await readFile(stateFile, 'utf8'))
if (state.projectId !== projectId || state.branchId !== branchId || !state.name.startsWith('ds-test-')) throw new Error('Function deployment requires the recorded isolated test branch')
const neon = createNeonClient({ apiKey, throwOnError: true })
const branch = await neon.branches.get({ projectId, branchId })
if (branch.name !== state.name) throw new Error('Test branch identity changed')
const secret = () => randomBytes(32).toString('hex')
let previous
try { previous = Object.fromEntries((await readFile(outputFile, 'utf8')).trim().split('\n').map((line) => { const n = line.indexOf('='); return [line.slice(0, n), line.slice(n + 1)] })) }
catch (error) { if (error.code !== 'ENOENT') throw error }
if (previous && previous.TEST_FUNCTION_BRANCH_ID !== branchId) throw new Error('Refusing to overwrite another test branch Function environment')
const schema = previous?.TEST_FUNCTION_SCHEMA ?? `ds_test_${randomUUID().replaceAll('-', '')}`
if (!/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Invalid Function test schema')
const userKey = previous?.TEST_FUNCTION_API_KEY ?? secret()
const otherKey = previous?.TEST_FUNCTION_OTHER_KEY ?? secret()
const maintenanceKey = previous?.TEST_FUNCTION_MAINTENANCE_KEY ?? secret()
const hintMode = process.env.NEON_TEST_MAINTENANCE_HINTS ?? 'off'
const liveInterval = (name) => {
  const value = Number(process.env[name] ?? 60_000)
  if (!Number.isSafeInteger(value) || value < 100 || value > 60_000) throw new Error(`Invalid ${name}`)
  return String(value)
}
const liveFallbackMs = liveInterval('NEON_TEST_LIVE_FALLBACK_MS')
const liveValidationMs = liveInterval('NEON_TEST_LIVE_VALIDATION_MS')
if (!['on', 'off'].includes(hintMode)) throw new Error('Invalid test maintenance hint mode')
if (process.env.GITHUB_ACTIONS === 'true') for (const value of [userKey, otherKey, maintenanceKey]) console.log(`::add-mask::${value}`)
const objects = createS3ChunkStorage({ endpoint: process.env.AWS_ENDPOINT_URL_S3, region: process.env.AWS_REGION, bucket: process.env.NEON_TEST_BUCKET })
const direct = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
try {
  const installed = await direct.query('SELECT to_regclass($1) AS relation', [`${schema}.migrations`])
  if (installed.rows[0].relation) {
    const history = await direct.query(`SELECT id FROM "${schema}".migrations ORDER BY id`)
    if (history.rows.length < MIGRATION_IDS.length && (await neon.functions.list({ projectId, branchId }).all()).length) {
      throw new Error('A stopped schema upgrade is required: stop the recorded branch Functions and workers before deploying across migrations')
    }
  }
  await migrate(direct, { schema, storageIdentity: objects.identity, environmentId: branchId })
  await initializeSubscriptions(direct, { schema, keyring: parseSubscriptionKeyring(process.env.DURABLE_STREAMS_KEYRING) })
}
finally { await direct.end(); objects.close() }
const bundle = await build({ entryPoints: ['examples/neon-function/index.ts'], bundle: true, platform: 'node', target: 'node24', format: 'esm', write: false,
  external: ['pg-native', 'bufferutil', 'utf-8-validate'],
  banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' },
})
if (bundle.outputFiles.length !== 1) throw new Error('Expected a single self-contained Function bundle')
const code = bundle.outputFiles[0].contents
const hash = createHash('sha256').update(code).digest('hex')
const sourceCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
const deploymentId = randomUUID()
const sourceDirty = execFileSync('git', ['status', '--porcelain', '--untracked-files=all', '--', 'src', 'examples/neon-function', 'package.json', 'pnpm-lock.yaml'], { encoding: 'utf8' }).trim() ? 'yes' : 'no'
const deployment = await neon.functions.deploy({ projectId, branchId, slug: 'durablestreams', runtime: 'nodejs24',
  zip: new File([zipSync({ 'index.mjs': code })], 'durable-streams.zip', { type: 'application/zip' }),
  environment: JSON.stringify({ DS_SCHEMA: schema, DS_ENVIRONMENT_ID: branchId, DS_BUCKET: process.env.NEON_TEST_BUCKET,
    DS_REALTIME_URL: process.env.NEON_REALTIME_URL, DS_REALTIME_SECRET: process.env.NEON_REALTIME_SECRET,
    DS_KEYRING_JSON: process.env.DURABLE_STREAMS_KEYRING,
    DS_AUTH_JSON: JSON.stringify([{ key: userKey, tenantId: 'function-a', subject: 'qualification-a' }, { key: otherKey, tenantId: 'function-b', subject: 'qualification-b' }]),
    DS_MAINTENANCE_KEY: maintenanceKey, DS_BUILD_HASH: hash, DS_DEPLOYMENT_ID: deploymentId,
    DS_MAINTENANCE_TRIGGER_NAME: 'durable-streams-maintenance', DS_LIVE_FALLBACK_MS: liveFallbackMs, DS_LIVE_VALIDATION_MS: liveValidationMs,
    DS_MAINTENANCE_HINTS: hintMode, DS_CRON_MAX_CHUNKS: '64', DS_CRON_SUBSCRIPTION_WORK: '64' }),
})
let url
const deadline = Date.now() + 180_000
while (Date.now() < deadline) {
  const fn = await neon.functions.get({ projectId, branchId, slug: 'durablestreams' })
  if (fn.current_deployment?.id === deployment.id && fn.current_deployment.status === 'failed') throw new Error(`Function build failed: ${fn.current_deployment.error ?? 'unknown build error'}`)
  if (fn.active_deployment?.id === deployment.id && fn.invocation_url) { url = fn.invocation_url; break }
  await new Promise((resolve) => setTimeout(resolve, 1000))
}
if (!url) throw new Error('Neon Function deployment did not become active')
const triggerName = 'durable-streams-maintenance'
const existing = (await neon.triggers.list({ projectId, branchId })).find((item) => item.name === triggerName)
const noCron = process.env.NEON_TEST_NO_CRON === '1'
if (noCron && existing) throw new Error('Cron-free qualification requires a fresh isolated branch')
if (!noCron && !existing) await neon.triggers.create({ projectId, branchId, type: 'schedule', name: triggerName,
  function_slug: 'durablestreams', function_path: '/_trigger', schedule: { cron: '* * * * *' } })
const receiver = await deployTestWebhook({ neon, projectId, branchId, schema, sourceUrl: url, previous })
const values = { ...receiver, TEST_FUNCTION_URL: url, TEST_FUNCTION_API_KEY: userKey, TEST_FUNCTION_OTHER_KEY: otherKey,
  TEST_FUNCTION_MAINTENANCE_KEY: maintenanceKey, TEST_FUNCTION_SCHEMA: schema, TEST_FUNCTION_BRANCH_ID: branchId, TEST_FUNCTION_BUNDLE_SHA: hash,
  TEST_FUNCTION_SOURCE_COMMIT: sourceCommit, TEST_FUNCTION_SOURCE_DIRTY: sourceDirty, TEST_FUNCTION_DEPLOYMENT_ID: deploymentId,
  TEST_FUNCTION_MAINTENANCE_HINTS: hintMode, TEST_FUNCTION_HAS_CRON: noCron ? 'no' : 'yes', TEST_FUNCTION_CRON_CHUNKS: '64', TEST_FUNCTION_CRON_SUBSCRIPTION_WORK: '64',
  TEST_FUNCTION_LIVE_FALLBACK_MS: liveFallbackMs, TEST_FUNCTION_LIVE_VALIDATION_MS: liveValidationMs }
if (process.env.GITHUB_ACTIONS === 'true') for (const [key, value] of Object.entries(values)) if (/_KEY$|_URL$/.test(key)) console.log(`::add-mask::${value}`)
const environment = Object.entries(values).map(([k, v]) => `${k}=${v}`).join('\n') + '\n'
if (process.env.GITHUB_ENV) await appendFile(process.env.GITHUB_ENV, environment)
else await writeFile(outputFile, environment, { mode: 0o600 })
// Deployment readiness is not runtime readiness. Verify the actual HTTP handler.
let ready = false
let consecutiveReady = 0
const readinessProbes = []
const readinessStarted = performance.now()
while (Date.now() < deadline) {
  try {
    const response = await fetch(new URL('health', url), { headers: { Authorization: `Bearer ${userKey}` }, signal: AbortSignal.timeout(15_000) })
    let matches = false
    if (response.ok) {
      const health = await response.json()
      matches = health.ready === true && health.build === hash && health.deploymentId === deploymentId
    } else await response.arrayBuffer()
    readinessProbes.push({ status: response.status, matches, elapsedMs: Math.round(performance.now() - readinessStarted) })
    consecutiveReady = matches ? consecutiveReady + 1 : 0
    if (consecutiveReady >= 3) { ready = true; break }
  } catch {
    consecutiveReady = 0
    readinessProbes.push({ status: 'network-error', matches: false, elapsedMs: Math.round(performance.now() - readinessStarted) })
  }
  await new Promise((resolve) => setTimeout(resolve, 1000))
}
await mkdir('qualification-results', { recursive: true })
await writeFile(`qualification-results/function-readiness-${branchId}-${deploymentId}.json`, JSON.stringify({
  branchId, deploymentId, bundleSha256: hash, ready, consecutiveReady, durationMs: Math.round(performance.now() - readinessStarted), probes: readinessProbes,
}, null, 2) + '\n')
if (!ready) throw new Error('Deployed Function failed its authenticated runtime health check')
console.log(`Deployed and verified the current bundle on test branch ${branchId}.`)
