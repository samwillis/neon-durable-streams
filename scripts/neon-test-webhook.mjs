import { randomBytes, createHash } from 'node:crypto'
import { build } from 'esbuild'
import { zipSync } from 'fflate'
import pg from 'pg'

export async function deployTestWebhook({ neon, projectId, branchId, schema, sourceUrl, previous }) {
  const admin = previous?.TEST_WEBHOOK_RECEIVER_KEY ?? randomBytes(32).toString('hex')
  if (process.env.GITHUB_ACTIONS === 'true') console.log(`::add-mask::${admin}`)
  const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
  let jwks
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS "${schema}".webhook_test_deliveries (
      wake_id text PRIMARY KEY, subscription_id text NOT NULL, payload jsonb NOT NULL, raw_body bytea NOT NULL,
      signature text NOT NULL, attempts integer NOT NULL DEFAULT 1)`)
    jwks = { keys: (await pool.query(`SELECT public_jwk FROM "${schema}".webhook_signing_keys`)).rows.map((row) => row.public_jwk) }
  } finally { await pool.end() }
  const bundle = await build({ entryPoints: ['test/fixtures/webhook-function.ts'], bundle: true, platform: 'node', target: 'node24', format: 'esm', write: false,
    external: ['pg-native'], banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' } })
  const code = bundle.outputFiles[0].contents
  const hash = createHash('sha256').update(code).digest('hex')
  const slug = 'dswebhookreceiver'
  const deployed = await neon.functions.deploy({ projectId, branchId, slug, runtime: 'nodejs24',
    zip: new File([zipSync({ 'index.mjs': code })], 'webhook-receiver.zip', { type: 'application/zip' }),
    environment: JSON.stringify({ DS_SCHEMA: schema, DS_RECEIVER_KEY: admin, DS_RECEIVER_JWKS: JSON.stringify(jwks),
      DS_EXPECTED_SERVER: new URL('v1/stream', sourceUrl).href, DS_BUILD_HASH: hash }) })
  const deadline = Date.now() + 180_000
  let url
  while (Date.now() < deadline) {
    const fn = await neon.functions.get({ projectId, branchId, slug })
    if (fn.current_deployment?.id === deployed.id && fn.current_deployment.status === 'failed') throw new Error('HTTPS receiver Function build failed')
    if (fn.active_deployment?.id === deployed.id && fn.invocation_url) { url = fn.invocation_url; break }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  if (!url) throw new Error('HTTPS receiver Function did not become active')
  while (Date.now() < deadline) {
    try {
      const response = await fetch(new URL('health', url), { headers: { Authorization: admin }, signal: AbortSignal.timeout(10_000) })
      if (response.ok && (await response.json()).build === hash) return { TEST_WEBHOOK_RECEIVER_URL: url, TEST_WEBHOOK_RECEIVER_KEY: admin }
    } catch { /* Cold starts are bounded by the outer deployment deadline. */ }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('HTTPS receiver Function failed its runtime readiness check')
}
