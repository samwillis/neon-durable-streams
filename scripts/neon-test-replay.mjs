import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { build } from 'esbuild'
import { zipSync } from 'fflate'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { migrate } from '../dist/migrations.js'
import { createS3ChunkStorage } from '../dist/s3.js'
import { initializeSubscriptions, parseSubscriptionKeyring } from '../dist/subscriptions.js'

const env = process.env
const stateFile = env.NEON_TEST_STATE_FILE ?? '.neon-test-state-replay.json'
const outputFile = env.NEON_TEST_FUNCTION_ENV_FILE ?? '.env.neon.replay.function'
const state = JSON.parse(await readFile(stateFile, 'utf8'))
if (!env.NEON_API_KEY || !env.NEON_PROJECT_ID || !env.NEON_TEST_BRANCH_ID || !env.TEST_DATABASE_URL_UNPOOLED
  || state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.NEON_TEST_BRANCH_ID || !state.name.startsWith('ds-test-')) throw new Error('Recorded isolated replay branch required')
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true })
if ((await neon.branches.get({ projectId: state.projectId, branchId: state.branchId })).name !== state.name) throw new Error('Replay branch identity changed')
const sourcePaths = ['src', 'migrations', 'package.json', 'pnpm-lock.yaml', 'test/fixtures/replay-function.mjs', 'test/fixtures/replay-data.mjs', 'scripts/neon-test-replay.mjs', 'scripts/qualify-replay.mjs']
const git = (...args) => execFileSync('git', args, { encoding: 'utf8' }).trim()
if (git('status', '--porcelain', '--untracked-files=all', '--', ...sourcePaths)) throw new Error('Commit replay source before deployment')
const sourceCommit = git('rev-parse', 'HEAD')
const integer = (name, fallback, min, max) => {
  const value = Number(env[name] ?? fallback)
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`Invalid ${name}`)
  return value
}
const settings = { targetChunkBytes: integer('REPLAY_CHUNK_BYTES', 4 * 1024 * 1024, 1, 16 * 1024 * 1024),
  maxChunkFrames: integer('REPLAY_CHUNK_FRAMES', 1024, 1, 1024), blockBytes: integer('REPLAY_BLOCK_BYTES', 65536, 4096, 16 * 1024 * 1024),
  cacheBytes: integer('REPLAY_CACHE_BYTES', 32 * 1024 * 1024, 0, 256 * 1024 * 1024), readAhead: integer('REPLAY_READ_AHEAD', 1, 0, 1) }
let previous
try { previous = Object.fromEntries((await readFile(outputFile, 'utf8')).trim().split('\n').map((line) => { const index = line.indexOf('='); return [line.slice(0, index), line.slice(index + 1)] })) }
catch (error) { if (error.code !== 'ENOENT') throw error }
if (previous && previous.TEST_FUNCTION_BRANCH_ID !== state.branchId) throw new Error('Replay environment belongs to another branch')
const schema = previous?.TEST_FUNCTION_SCHEMA ?? `ds_test_${randomUUID().replaceAll('-', '')}`
const token = previous?.TEST_FUNCTION_API_KEY ?? randomBytes(32).toString('hex')
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
const direct = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
try {
  await migrate(direct, { schema, storageIdentity: objects.identity, environmentId: state.branchId })
  await initializeSubscriptions(direct, { schema, keyring: parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING) })
} finally { await direct.end(); objects.close() }
const bundled = await build({ entryPoints: ['test/fixtures/replay-function.mjs'], bundle: true, platform: 'node', target: 'node24', format: 'esm', write: false,
  external: ['pg-native', 'bufferutil', 'utf-8-validate'],
  banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' } })
if (bundled.outputFiles.length !== 1) throw new Error('Expected one replay bundle')
const code = bundled.outputFiles[0].contents
const bundleSha256 = createHash('sha256').update(code).digest('hex')
const slug = env.REPLAY_FUNCTION_SLUG ?? 'replaybench'
if (!/^replay[a-z0-9-]{1,24}$/.test(slug)) throw new Error('Invalid replay Function slug')
const deployed = await neon.functions.deploy({ projectId: state.projectId, branchId: state.branchId, slug, runtime: 'nodejs24',
  zip: new File([zipSync({ 'index.mjs': code })], 'replay.zip', { type: 'application/zip' }),
  environment: JSON.stringify({ DS_SCHEMA: schema, DS_ENVIRONMENT_ID: state.branchId, DS_BUCKET: env.NEON_TEST_BUCKET,
    DS_REALTIME_URL: env.NEON_REALTIME_URL, DS_REALTIME_SECRET: env.NEON_REALTIME_SECRET, DS_KEYRING_JSON: env.DURABLE_STREAMS_KEYRING,
    DS_BENCHMARK_KEY: `Bearer ${token}`, DS_BUILD_HASH: bundleSha256, DS_SOURCE_COMMIT: sourceCommit, DS_BENCHMARK_SETTINGS: JSON.stringify(settings) }) })
const deadline = Date.now() + 180000
let url, ready = false
while (Date.now() < deadline) {
  const fn = await neon.functions.get({ projectId: state.projectId, branchId: state.branchId, slug })
  if (fn.current_deployment?.id === deployed.id && fn.current_deployment.status === 'failed') throw new Error('Replay Function deployment failed')
  if (fn.active_deployment?.id === deployed.id && fn.invocation_url) {
    url = fn.invocation_url
    try {
      const response = await fetch(new URL('_benchmark/health', url), { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10000) })
      const health = await response.json()
      if (health.ready && health.build === bundleSha256 && health.sourceCommit === sourceCommit) { ready = true; break }
    } catch { /* The deployment may precede runtime readiness. */ }
  }
  await new Promise((resolve) => setTimeout(resolve, 1000))
}
if (!ready) throw new Error('Replay Function readiness deadline exceeded')
await writeFile(outputFile, Object.entries({ TEST_FUNCTION_URL: url, TEST_FUNCTION_API_KEY: token, TEST_FUNCTION_BRANCH_ID: state.branchId,
  TEST_FUNCTION_SCHEMA: schema, TEST_FUNCTION_BUNDLE_SHA: bundleSha256, TEST_FUNCTION_SOURCE_COMMIT: sourceCommit,
  TEST_FUNCTION_DEPLOYMENT_ID: deployed.id, REPLAY_SETTINGS: JSON.stringify(settings) }).map(([name, value]) => `${name}=${value}`).join('\n') + '\n', { mode: 0o600 })
console.log(JSON.stringify({ ready, branchId: state.branchId, schema, sourceCommit, bundleSha256, settings }))
