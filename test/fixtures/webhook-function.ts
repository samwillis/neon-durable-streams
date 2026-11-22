// Test-only public HTTPS receiver. Never bundled into the consumer example.
import { createHash, createPublicKey, timingSafeEqual, verify } from 'node:crypto'
import type { JsonWebKey } from 'node:crypto'
import { attachDatabasePool } from '@neon/functions'
import pg from 'pg'

const required = (name: string) => { const value = process.env[name]; if (!value) throw new Error(`Missing receiver setting: ${name}`); return value }
const schema = required('DS_SCHEMA')
if (!/^ds_test_[a-f0-9]{32}$/.test(schema)) throw new Error('Receiver only supports isolated test schemas')
const digest = (value: string) => createHash('sha256').update(value).digest()
const admin = digest(required('DS_RECEIVER_KEY'))
const base = required('DS_EXPECTED_SERVER')
const jwks = JSON.parse(required('DS_RECEIVER_JWKS')) as { keys: (JsonWebKey & { kid: string })[] }
const keys = new Map(jwks.keys.map((key) => [key.kid, createPublicKey({ key, format: 'jwk' })]))
const pool = new pg.Pool({ connectionString: required('DATABASE_URL'), max: 2, connectionTimeoutMillis: 5000 })
attachDatabasePool(pool)
process.once('SIGINT', () => { void pool.end() })
export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/health' || url.pathname === '/deliveries') {
      const authorization = request.headers.get('authorization') ?? ''
      if (authorization.length > 1024 || !timingSafeEqual(digest(authorization), admin)) return new Response(null, { status: 401 })
      if (url.pathname === '/health') return Response.json({ ready: true, build: required('DS_BUILD_HASH') })
      const rows = await pool.query(`SELECT wake_id, payload, encode(raw_body, 'base64') AS raw_body, signature, attempts FROM "${schema}".webhook_test_deliveries WHERE subscription_id = $1 ORDER BY wake_id LIMIT 64`, [url.searchParams.get('subscription_id')])
      return Response.json(rows.rows)
    }
    if (request.method !== 'POST' || !['/sync', '/async', '/retry'].includes(url.pathname)) return new Response(null, { status: 404 })
    const signature = request.headers.get('webhook-signature') ?? ''
    const fields = /^t=(\d{1,12}),kid=([A-Za-z0-9_-]{1,128}),ed25519=([A-Za-z0-9_-]{86})$/.exec(signature)
    if (!fields || Math.abs(Date.now() / 1000 - Number(fields[1])) > 300 || !keys.has(fields[2]!)) return new Response(null, { status: 401 })
    const reader = request.body?.getReader()
    if (!reader) return new Response(null, { status: 400 })
    const chunks: Uint8Array[] = []
    let size = 0
    try {
      for (;;) {
        const next = await reader.read()
        if (next.done) break
        size += next.value.length
        if (size > 512 * 1024) { await reader.cancel(); return new Response(null, { status: 413 }) }
        chunks.push(next.value)
      }
    } finally { reader.releaseLock() }
    const raw = Buffer.concat(chunks, size)
    if (!verify(null, Buffer.concat([Buffer.from(`${fields[1]}.`), raw]), keys.get(fields[2]!)!, Buffer.from(fields[3]!, 'base64url'))) return new Response(null, { status: 401 })
    const payload = JSON.parse(raw.toString('utf8')) as { subscription_id: string; tenant_id: string; wake_id: string; callback_url: string }
    if (payload.tenant_id !== 'function-a' || !/^[A-Za-z0-9_-]{1,128}$/.test(payload.subscription_id)
      || payload.callback_url !== `${base}/__ds/subscriptions/${encodeURIComponent(payload.subscription_id)}/callback`) return new Response(null, { status: 403 })
    const saved = await pool.query<{ attempts: number }>(`INSERT INTO "${schema}".webhook_test_deliveries (wake_id, subscription_id, payload, raw_body, signature)
      VALUES ($1,$2,$3,$4,$5) ON CONFLICT (wake_id) DO UPDATE SET attempts = webhook_test_deliveries.attempts + 1 RETURNING attempts`,
    [payload.wake_id, payload.subscription_id, payload, raw, signature])
    if (url.pathname === '/retry' && saved.rows[0]!.attempts === 1) return Response.json({ done: false }, { status: 500 })
    return Response.json({ done: url.pathname !== '/async' }, { status: url.pathname === '/async' ? 202 : 200 })
  },
}
