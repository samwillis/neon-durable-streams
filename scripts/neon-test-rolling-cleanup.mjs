import { readFile, realpath, rm, unlink } from 'node:fs/promises'
import { basename, dirname } from 'node:path'
import { tmpdir } from 'node:os'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'

const stateFile = '.neon-test-state-rolling.json', artifactsFile = '.neon-test-state-rolling-artifacts.json'
let state
try { state = JSON.parse(await readFile(stateFile, 'utf8')) } catch (error) { if (error.code !== 'ENOENT') throw error }
if (state) {
  const { NEON_API_KEY: apiKey, NEON_PROJECT_ID: projectId, NEON_TEST_BRANCH_ID: branchId, TEST_DATABASE_URL_UNPOOLED: uri } = process.env
  const parsed = uri && new URL(uri)
  if (!apiKey || state.projectId !== projectId || state.branchId !== branchId || !/^ds_test_[a-f0-9]{32}$/.test(state.schema)
    || !parsed || parsed.hostname !== state.databaseHost || parsed.pathname !== state.databaseName
    || !Array.isArray(state.slugs) || state.slugs.length !== 4 || state.slugs.some((slug) => !/^dsmix[a-d][a-f0-9]{12}$/.test(slug))) throw new Error('Unrecognized rolling fixture cleanup')
  const neon = createNeonClient({ apiKey, throwOnError: true, waitForReadiness: true })
  const branch = await neon.branches.get({ projectId, branchId })
  if (branch.default || branch.protected || branch.name !== state.branchName || !branch.name.startsWith('ds-test-')) throw new Error('Rolling fixture branch identity changed')
  const functions = await neon.functions.list({ projectId, branchId }).all()
  for (const fn of functions) if (state.slugs.includes(fn.slug)) await neon.functions.delete({ projectId, branchId, slug: fn.slug })
  await new Promise((resolve) => setTimeout(resolve, 6000))
  const pool = new pg.Pool({ connectionString: uri, max: 1, connectionTimeoutMillis: 20_000, query_timeout: 60_000 })
  try { await pool.query(`DROP SCHEMA IF EXISTS "${state.schema}" CASCADE`) } finally { await pool.end() }
  await unlink(stateFile)
}
let artifacts
try { artifacts = JSON.parse(await readFile(artifactsFile, 'utf8')) } catch (error) { if (error.code !== 'ENOENT') throw error }
if (artifacts) {
  if (!basename(artifacts.directory).startsWith('durable-streams-rolling-')
    || await realpath(dirname(artifacts.directory)) !== await realpath(tmpdir())) throw new Error('Unrecognized rolling artifact directory')
  await rm(artifacts.directory, { recursive: true, force: true })
  await unlink(artifactsFile)
}
console.log('Cleaned up the recorded rolling Functions, isolated schema, and packed consumers.')
