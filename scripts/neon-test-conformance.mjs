import { createHash, randomBytes } from 'node:crypto'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { build } from 'esbuild'
import { zipSync } from 'fflate'
import { createNeonClient } from '@neon/sdk'
import { DeleteObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { buildConformanceArtifact } from './neon-conformance-artifact.mjs'

const env = process.env, slug = 'dsconformance'
const projectId = env.NEON_PROJECT_ID, branchId = env.NEON_TEST_BRANCH_ID
if (!projectId || !branchId || !env.NEON_API_KEY) throw new Error('Recorded isolated Neon environment required')
const state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json', 'utf8'))
if (state.projectId !== projectId || state.branchId !== branchId || !state.name.startsWith('ds-test-')) throw new Error('Unrecognized conformance branch')
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true })
const branch = await neon.branches.get({ projectId, branchId })
if (branch.name !== state.name || branch.default || branch.protected) throw new Error('Conformance must run on the recorded disposable branch')
const runtimeEnv = {}
for (const name of ['TEST_DATABASE_URL', 'TEST_DATABASE_URL_UNPOOLED', 'NEON_REALTIME_URL', 'NEON_REALTIME_SECRET', 'AWS_ENDPOINT_URL_S3', 'AWS_REGION', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'NEON_TEST_BUCKET', 'DURABLE_STREAMS_KEYRING']) {
  if (!env[name]) throw new Error(`Missing conformance environment: ${name}`)
  runtimeEnv[name] = env[name]
}
// No management API key or caller-controlled script enters the Function.
await rm('conformance-results.json', { force: true })
const key = randomBytes(32).toString('hex')
if (env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${key}`)
const { payload, nativeFiles, manifest } = await buildConformanceArtifact()
console.log(`Built unchanged suite artifact (${manifest.payloadBytes} compressed bytes).`)
const bundle = await build({ entryPoints: ['test/fixtures/conformance-function.mjs'], bundle: true, platform: 'node', target: 'node24', format: 'esm', write: false,
  banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' } })
const code = bundle.outputFiles[0].contents
manifest.launcherSha256 = createHash('sha256').update(code).digest('hex')
const deploymentZip = zipSync({ 'index.mjs': code, ...nativeFiles })
if (deploymentZip.length > 32 * 1024 * 1024) throw new Error('Native conformance deployment exceeds the Neon upload limit')
manifest.deploymentZipSha256 = createHash('sha256').update(deploymentZip).digest('hex')
manifest.deploymentZipBytes = deploymentZip.length
manifest.generatedAt = new Date().toISOString()
await mkdir('qualification-results', { recursive: true })
const evidence = { ...manifest, passed: false }
const artifactKey = `qualification/conformance/${manifest.payloadSha256}.zip`
const s3 = new S3Client({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, forcePathStyle: true,
  requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED' })
let attempted = false
try {
  // Functions has a 32 MiB compressed deployment cap. Store the larger locked
  // test graph privately on this disposable branch; the launcher pins its hash.
  await s3.send(new PutObjectCommand({ Bucket: env.NEON_TEST_BUCKET, Key: artifactKey, Body: payload }), { abortSignal: AbortSignal.timeout(120_000) })
  // The branch ledger predates deployment; whole-branch cleanup and the CI
  // janitor cover runner loss, including an ambiguous deploy response.
  attempted = true
  const deployment = await neon.functions.deploy({ projectId, branchId, slug, runtime: 'nodejs24',
    zip: new File([deploymentZip], 'conformance.zip', { type: 'application/zip' }),
    environment: JSON.stringify({ ...runtimeEnv, DS_CONFORMANCE_KEY: key, DS_CONFORMANCE_PAYLOAD_SHA: manifest.payloadSha256,
      DS_CONFORMANCE_PAYLOAD_KEY: artifactKey, DS_CONFORMANCE_PAYLOAD_BYTES: String(payload.length) }),
  })
  const deadline = Date.now() + 180_000
  let url, runtime
  while (Date.now() < deadline) {
    const fn = await neon.functions.get({ projectId, branchId, slug })
    if (fn.current_deployment?.id === deployment.id && fn.current_deployment.status === 'failed') {
      evidence.deploymentError = fn.current_deployment.error
      throw new Error(`Conformance Function deployment failed: ${fn.current_deployment.error ?? 'unknown build failure'}`)
    }
    if (fn.active_deployment?.id === deployment.id && fn.invocation_url) {
      try {
        const response = await fetch(fn.invocation_url, { headers: { Authorization: `Bearer ${key}` }, signal: AbortSignal.timeout(10_000) })
        if (response.ok) { const health = await response.json(); if (health.ready === true && health.payloadSha256 === manifest.payloadSha256) { url = fn.invocation_url; runtime = health; break } }
        else await response.arrayBuffer()
      } catch { /* Deployment and cold start readiness are distinct. */ }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  if (!url) throw new Error('Conformance Function did not become ready')
  evidence.runtime = runtime
  if (`${runtime.platform}/${runtime.arch}/${runtime.glibc ? 'glibc' : 'musl'}` !== manifest.target) throw new Error('Conformance artifact does not support this Function architecture')
  console.log(`Running all 338 upstream cases on Neon ${runtime.platform}/${runtime.arch}, ${runtime.node}.`)
  const response = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${key}` }, signal: AbortSignal.timeout(25 * 60_000) })
  if (!response.ok || !response.body) throw new Error(`Conformance coordinator returned HTTP ${response.status}`)
  let pending = '', result
  const decoder = new TextDecoder()
  for await (const chunk of response.body) {
    pending += decoder.decode(chunk, { stream: true })
    if (pending.length > 12 * 1024 * 1024) throw new Error('Conformance event exceeds bound')
    let newline
    while ((newline = pending.indexOf('\n')) >= 0) {
      const line = pending.slice(0, newline); pending = pending.slice(newline + 1)
      if (!line) continue
      if (!line.startsWith('data: ')) throw new Error('Malformed conformance event')
      const event = JSON.parse(line.slice(6))
      if (event.type === 'log') process.stdout.write(event.text)
      else if (event.type === 'heartbeat') console.log('Neon conformance still running.')
      else if (event.type === 'error') throw new Error(event.message)
      else if (event.type === 'result') {
        if (result) throw new Error('Duplicate conformance result')
        result = event
        // Retain all upstream results even if later stream/cleanup fails.
        await writeFile('conformance-results.json', JSON.stringify(result.report) + '\n')
      }
    }
  }
  if (pending.trim() || !result) throw new Error('Conformance stream ended without a complete result')
  Object.assign(evidence, { passed: result.passed === true, exitCode: result.code, signal: result.signal, durationMs: result.durationMs,
    total: result.report.numTotalTests, passedTests: result.report.numPassedTests, failedTests: result.report.numFailedTests, skippedTests: result.report.numPendingTests })
  if (!evidence.passed) throw new Error(`Full Neon conformance failed: ${evidence.passedTests}/${evidence.total}`)
  console.log(`Full Neon conformance passed: ${evidence.passedTests}/${evidence.total}, no failures or skips.`)
} finally {
  // Save a failed report on interrupted/ambiguous runs; successful cleanup is
  // part of qualification, as with the multi-instance suite.
  try {
    if (attempted) {
      try { await neon.functions.delete({ projectId, branchId, slug }) }
      catch (error) { if (error.status !== 404) throw error }
    }
    await s3.send(new DeleteObjectCommand({ Bucket: env.NEON_TEST_BUCKET, Key: artifactKey }), { abortSignal: AbortSignal.timeout(30_000) })
    evidence.cleanupComplete = true
  } catch (error) { evidence.passed = false; throw error }
  finally { s3.destroy(); await writeFile('qualification-results/colocated-conformance.json', JSON.stringify(evidence, null, 2) + '\n') }
}
