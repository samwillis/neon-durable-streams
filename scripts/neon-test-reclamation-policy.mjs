import { randomUUID } from 'node:crypto'
import { open, readFile, writeFile } from 'node:fs/promises'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { migrate } from '../dist/migrations.js'
import { beginRecovery, configureReclamation, finishRecovery, inspectInstallation, verifyRecovery } from '../dist/operations.js'
import { createS3ChunkStorage } from '../dist/s3.js'

// Test setup only. Refuse a serving deployment or an unrecorded branch. The
// ordinary example has no route that enables or changes collection policy.
const env = process.env, stateFile = env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json'
const functionFile = env.NEON_TEST_FUNCTION_ENV_FILE ?? '.env.neon.function'
const state = JSON.parse(await readFile(stateFile, 'utf8'))
if (!env.NEON_API_KEY || !env.TEST_DATABASE_URL_UNPOOLED || state.projectId !== env.NEON_PROJECT_ID || state.branchId !== env.NEON_TEST_BRANCH_ID || !state.name.startsWith('ds-test-')) throw new Error('Recorded isolated Neon environment required')
const neon = createNeonClient({ apiKey: env.NEON_API_KEY, throwOnError: true })
const branch = await neon.branches.get({ projectId: state.projectId, branchId: state.branchId })
if (branch.default || branch.protected || branch.name !== state.name || (await neon.functions.list({ projectId: state.projectId, branchId: state.branchId }).all()).length
  || (await neon.triggers.list({ projectId: state.projectId, branchId: state.branchId })).length) throw new Error('Reclamation test setup requires a fresh stopped branch without Functions or triggers')
const schema = `ds_test_${randomUUID().replaceAll('-', '')}`
const handle = await open(functionFile, 'wx', 0o600)
try { await handle.writeFile(`TEST_FUNCTION_BRANCH_ID=${state.branchId}\nTEST_FUNCTION_SCHEMA=${schema}\n`); await handle.sync() }
finally { await handle.close() }
const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 1, connectionTimeoutMillis: 20_000 })
const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
try {
  await migrate(pool, { schema, environmentId: state.branchId, storageIdentity: objects.identity })
  const before = await inspectInstallation(pool, { schema }), operationId = randomUUID()
  const operation = { schema, operationId, expectedInstallationId: before.id, expectedServingEpoch: before.serving_epoch,
    environmentId: state.branchId, storageIdentity: objects.identity }
  await writeFile(`${stateFile}.policy.json`, JSON.stringify({ ...operation, recoveryWindowMs: 300_000, safetyMarginMs: 900_000 }), { mode: 0o600, flag: 'wx' })
  const { token } = await beginRecovery(pool, operation)
  await configureReclamation(pool, { schema, token, enabled: true, recoveryWindowMs: 300_000, safetyMarginMs: 900_000 })
  await verifyRecovery(pool, { schema, token, objects })
  await finishRecovery(pool, { schema, token })
  console.log('Fresh isolated schema ready: collection enabled, five-minute supported recovery window plus fifteen-minute safety margin. No deadline bypasses.')
} finally { objects.close(); await pool.end() }
