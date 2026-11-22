import { createNeonClient } from '@neon/sdk'
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
  await unlink(stateFile)
  console.log(`Deleted test branch ${state.branchId}.`)
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
  }
  for (const value of Object.values(values)) mask(value)
  const environment = Object.entries(values).map(([k, v]) => `${k}=${v}`).join('\n') + '\n'
  if (process.env.GITHUB_ENV) await appendFile(process.env.GITHUB_ENV, environment)
  else await writeFile(outputFile, environment, { mode: 0o600, flag: 'wx' })
  console.log(`Ready: Postgres 18 and Realtime on isolated branch ${branch.id}; credentials written without logging them.`)
} else throw new Error('Expected create or delete')
