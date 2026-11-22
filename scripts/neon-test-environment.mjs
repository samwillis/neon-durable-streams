import { createNeonClient } from '@neon/sdk'
import { randomBytes } from 'node:crypto'
import { appendFile, open, readFile, unlink, writeFile } from 'node:fs/promises'

// No application imports: this is a trusted CI/development control-plane tool.
const apiKey = process.env.NEON_API_KEY
const projectId = process.env.NEON_PROJECT_ID
if (!apiKey || !projectId) throw new Error('NEON_API_KEY and NEON_PROJECT_ID are required; Neon qualification cannot be skipped')
const neon = createNeonClient({ apiKey, throwOnError: true, waitForReadiness: true, wait: { timeoutMs: 180_000 } })
const stateFile = process.env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json'
const outputFile = process.env.NEON_TEST_ENV_FILE ?? '.env.neon.test'
const operation = process.argv[2] ?? 'create'
const escape = (value) => value.replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A')
const mask = (value) => { if (process.env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${escape(value)}`) }

async function storageEnvironment(branchId) {
  const bucket = 'durable-streams-test'
  const existing = (await neon.storage.buckets.list({ projectId, branchId })).find((item) => item.name === bucket)
  if (existing && existing.access_level !== 'private') throw new Error('Test chunk bucket must be private')
  if (!existing) await neon.storage.buckets.create({ projectId, branchId, name: bucket, access_level: 'private' })
  const storage = await neon.storage.get({ projectId, branchId })
  if (!storage.enabled || !storage.force_path_style) throw new Error('Neon Object Storage is not ready')
  const credential = await neon.credentials.create({ projectId, branchId, name: 'durable-streams-qualification', principal_type: 'user',
    scopes: ['storage:read', 'storage:write'], expires_at: new Date(Date.now() + 24 * 3600_000).toISOString() })
  return { AWS_ENDPOINT_URL_S3: storage.s3_endpoint, AWS_REGION: storage.region,
    AWS_ACCESS_KEY_ID: credential.token_id, AWS_SECRET_ACCESS_KEY: credential.s3_secret_access_key, NEON_TEST_BUCKET: bucket }
}
async function output(values, append = false) {
  for (const value of Object.values(values)) mask(value)
  if (values.DURABLE_STREAMS_KEYRING) for (const value of Object.values(JSON.parse(values.DURABLE_STREAMS_KEYRING).keys)) mask(value)
  const environment = Object.entries(values).map(([k, v]) => `${k}=${v}`).join('\n') + '\n'
  if (process.env.GITHUB_ENV) await appendFile(process.env.GITHUB_ENV, environment)
  else if (append) await appendFile(outputFile, environment, { mode: 0o600 })
  else await writeFile(outputFile, environment, { mode: 0o600, flag: 'wx' })
}

if (operation === 'delete') {
  let state
  try { state = JSON.parse(await readFile(stateFile, 'utf8')) } catch (error) {
    if (error.code === 'ENOENT') { console.log('No branch was provisioned.'); process.exit(0) }
    throw error
  }
  if (state.projectId !== projectId || !state.name.startsWith('ds-test-')) throw new Error('Refusing cleanup outside the recorded test installation')
  await neon.branches.delete({ projectId, branchId: state.branchId })
  try {
    const environment = await readFile(outputFile, 'utf8')
    if (environment.split('\n').includes(`NEON_TEST_BRANCH_ID=${state.branchId}`)) await unlink(outputFile)
  } catch (error) { if (error.code !== 'ENOENT') throw error }
  const functionFile = process.env.NEON_TEST_FUNCTION_ENV_FILE ?? '.env.neon.function'
  try {
    const environment = await readFile(functionFile, 'utf8')
    if (environment.split('\n').includes(`TEST_FUNCTION_BRANCH_ID=${state.branchId}`)) await unlink(functionFile)
  } catch (error) { if (error.code !== 'ENOENT') throw error }
  await unlink(stateFile)
  console.log(`Deleted test branch ${state.branchId}.`)
} else if (operation === 'storage') {
  const state = JSON.parse(await readFile(stateFile, 'utf8'))
  if (state.projectId !== projectId || !state.name.startsWith('ds-test-')) throw new Error('Storage provisioning requires this run\'s test branch')
  const environment = await readFile(outputFile, 'utf8')
  if (!environment.split('\n').includes(`NEON_TEST_BRANCH_ID=${state.branchId}`) || environment.includes('AWS_ACCESS_KEY_ID=')) throw new Error('Refusing to overwrite a storage environment')
  await output(await storageEnvironment(state.branchId), true)
  console.log('Private Neon Object Storage bucket ready; credentials saved without logging them.')
} else if (operation === 'create') {
  const suffix = process.env.GITHUB_RUN_ID ? `${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}` : `${Date.now()}`
  const name = `ds-test-${suffix}`
  const parentId = process.env.NEON_PARENT_BRANCH_ID ?? (await neon.branches.getDefault({ projectId })).id
  // Refuse stale state before allocating infrastructure. An exclusive handle
  // also prevents two local runs from overwriting each other's cleanup identity.
  const state = await open(stateFile, 'wx', 0o600)
  let branch
  try {
    branch = await neon.branches.create({ projectId, name, parent_id: parentId,
      expires_at: new Date(Date.now() + 24 * 3600_000).toISOString(),
      compute: { minCu: 0.25, maxCu: 1, suspendTimeoutSeconds: 300 },
    })
    // Persist cleanup identity before fetching secrets or enabling services.
    await state.writeFile(JSON.stringify({ projectId, branchId: branch.id, name }))
    await state.sync()
  } catch (error) {
    if (!branch) await unlink(stateFile)
    throw error
  } finally { await state.close() }
  await neon.realtime.enable({ projectId, branchId: branch.id })
  const [pooled, direct, realtime, key] = await Promise.all([
    neon.postgres.connectionString({ projectId, branchId: branch.id, pooled: true }),
    neon.postgres.connectionString({ projectId, branchId: branch.id, pooled: false }),
    neon.realtime.get({ projectId, branchId: branch.id }),
    neon.realtime.secret({ projectId, branchId: branch.id }),
  ])
  if (!realtime.enabled || realtime.pending || !realtime.invocation_url || key.pending) throw new Error('Realtime did not reach ready state')
  const verified = (uri) => { const url = new URL(uri); url.searchParams.set('sslmode', 'verify-full'); return url.href }
  const values = { TEST_DATABASE_URL: verified(pooled), TEST_DATABASE_URL_UNPOOLED: verified(direct),
    NEON_REALTIME_URL: realtime.invocation_url, NEON_REALTIME_SECRET: key.secret,
    NEON_TEST_BRANCH_ID: branch.id, NEON_TEST_PROJECT_ID: projectId,
    DURABLE_STREAMS_KEYRING: JSON.stringify({ currentKeyId: 'v1', keys: { v1: randomBytes(32).toString('base64') } }),
    ...await storageEnvironment(branch.id),
  }
  await output(values)
  console.log(`Ready: Postgres 18, Realtime, and Object Storage on isolated branch ${branch.id}; credentials saved without logging them.`)
} else throw new Error('Expected create, storage, or delete')
