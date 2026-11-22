import { createCipheriv, createDecipheriv, createHash, createPrivateKey, generateKeyPairSync, randomBytes, sign } from 'node:crypto'
import type { JsonWebKey } from 'node:crypto'
import type { Pool } from 'pg'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'
import { StorageCompatibilityError } from './errors.js'

export interface SubscriptionKeyring {
  readonly currentKeyId: string
  readonly keys: Readonly<Record<string, Uint8Array>>
}
export function parseSubscriptionKeyring(json: string): SubscriptionKeyring {
  let value: { currentKeyId?: unknown; keys?: unknown }
  try { value = JSON.parse(json) as typeof value } catch { throw new TypeError('Invalid subscription keyring JSON') }
  if (!value || typeof value.currentKeyId !== 'string' || !value.keys || typeof value.keys !== 'object' || Array.isArray(value.keys)) throw new TypeError('Invalid subscription keyring')
  const keys: Record<string, Buffer> = Object.create(null) as Record<string, Buffer>
  for (const [id, encoded] of Object.entries(value.keys)) {
    if (typeof encoded !== 'string' || !/^[A-Za-z0-9+/]{43}=$/.test(encoded)) throw new TypeError('Keyring keys must be canonical base64-encoded 32-byte values')
    const bytes = Buffer.from(encoded, 'base64')
    if (bytes.toString('base64') !== encoded) throw new TypeError('Noncanonical base64 keyring value')
    keys[id] = bytes
  }
  const ring = { currentKeyId: value.currentKeyId, keys }
  new SubscriptionCipher(ring)
  return ring
}
export class SubscriptionCipher {
  private readonly keys = new Map<string, Buffer>()
  private readonly current: string
  constructor(ring: SubscriptionKeyring) {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(ring.currentKeyId)) throw new TypeError('Invalid wrapping key ID')
    for (const [id, bytes] of Object.entries(ring.keys)) {
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(id) || bytes.byteLength !== 32) throw new TypeError('Subscription wrapping keys must have 32 bytes')
      this.keys.set(id, Buffer.from(bytes))
    }
    if (!this.keys.has(ring.currentKeyId)) throw new TypeError('Current subscription wrapping key is absent')
    this.current = ring.currentKeyId
  }
  encrypt(bytes: Uint8Array, aad: string): Buffer {
    const id = Buffer.from(this.current)
    const iv = randomBytes(12)
    const cipher = createCipheriv('aes-256-gcm', this.keys.get(this.current)!, iv)
    cipher.setAAD(Buffer.from(aad))
    const encrypted = Buffer.concat([cipher.update(bytes), cipher.final()])
    return Buffer.concat([Buffer.from('NDSE1'), Buffer.from([id.length]), id, iv, cipher.getAuthTag(), encrypted])
  }
  decrypt(envelope: Buffer, aad: string): Buffer {
    const n = envelope[5] ?? 0
    if (envelope.length < 34 + n || !envelope.subarray(0, 5).equals(Buffer.from('NDSE1')) || !n) throw new StorageCompatibilityError('Invalid encrypted subscription envelope')
    const key = this.keys.get(envelope.toString('utf8', 6, 6 + n))
    if (!key) throw new StorageCompatibilityError('Subscription wrapping key is unavailable; restore the configured keyring')
    const decipher = createDecipheriv('aes-256-gcm', key, envelope.subarray(6 + n, 18 + n))
    decipher.setAAD(Buffer.from(aad))
    decipher.setAuthTag(envelope.subarray(18 + n, 34 + n))
    return Buffer.concat([decipher.update(envelope.subarray(34 + n)), decipher.final()])
  }
}
export interface InitializeSubscriptionsOptions {
  readonly schema?: string
  readonly keyring: SubscriptionKeyring
  /** Retain old public keys. This explicitly rotates the active signing key. */
  readonly rotateSigningKey?: boolean
}
export async function initializeSubscriptions(pool: Pool, options: InitializeSubscriptionsOptions): Promise<{ kid: string }> {
  const schema = validateSchema(options.schema ?? 'durable_streams')
  const cipher = new SubscriptionCipher(options.keyring)
  return transaction(pool, {}, async (client) => {
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`durable-streams:signing:${schema}`])
    const installation = await client.query<{ id: string }>(`SELECT id FROM ${schema}.installation`)
    if (installation.rows.length !== 1) throw new StorageCompatibilityError('Missing installation identity')
    const existing = await client.query<{ kid: string }>(`SELECT kid FROM ${schema}.webhook_signing_keys WHERE active`)
    if (existing.rows[0] && !options.rotateSigningKey) return existing.rows[0]
    const pair = generateKeyPairSync('ed25519')
    const publicJwk = pair.publicKey.export({ format: 'jwk' })
    const kid = `ds_${createHash('sha256').update(JSON.stringify({ crv: publicJwk.crv, kty: publicJwk.kty, x: publicJwk.x })).digest('base64url')}`
    const der = pair.privateKey.export({ format: 'der', type: 'pkcs8' })
    const encrypted = cipher.encrypt(der, `${installation.rows[0]!.id}:signing:${kid}`)
    der.fill(0)
    await client.query(`UPDATE ${schema}.webhook_signing_keys SET active = false WHERE active`)
    await client.query(`INSERT INTO ${schema}.webhook_signing_keys (kid, public_jwk, private_key, active) VALUES ($1, $2, $3, true)`,
      [kid, { ...publicJwk, kid, alg: 'EdDSA', use: 'sig' }, encrypted])
    return { kid }
  })
}
export class SubscriptionSigner {
  private readonly schema: string
  private readonly cipher: SubscriptionCipher
  constructor(private readonly pool: Pool, schema: string, ring: SubscriptionKeyring) { this.schema = validateSchema(schema); this.cipher = new SubscriptionCipher(ring) }
  async active(): Promise<string> {
    const key = await this.pool.query<{ kid: string }>(`SELECT kid FROM ${this.schema}.webhook_signing_keys WHERE active`)
    if (!key.rows[0]) throw new StorageCompatibilityError('Run initializeSubscriptions before enabling webhook subscriptions')
    return key.rows[0].kid
  }
  async jwks(): Promise<{ keys: readonly JsonWebKey[] }> {
    const result = await this.pool.query<{ public_jwk: JsonWebKey }>(`SELECT public_jwk FROM ${this.schema}.webhook_signing_keys ORDER BY created_at, kid`)
    if (!result.rows.length) throw new StorageCompatibilityError('Run initializeSubscriptions before serving JWKS')
    return { keys: result.rows.map((row) => row.public_jwk) }
  }
  async signature(body: Buffer): Promise<string> {
    const result = await this.pool.query<{ kid: string; private_key: Buffer; installation_id: string }>(`SELECT k.kid, k.private_key, i.id AS installation_id
      FROM ${this.schema}.webhook_signing_keys k CROSS JOIN ${this.schema}.installation i WHERE k.active`)
    const row = result.rows[0]
    if (!row) throw new StorageCompatibilityError('No active webhook signing key')
    const der = this.cipher.decrypt(row.private_key, `${row.installation_id}:signing:${row.kid}`)
    const key = createPrivateKey({ key: der, format: 'der', type: 'pkcs8' })
    der.fill(0)
    const timestamp = String(Math.floor(Date.now() / 1000))
    const signature = sign(null, Buffer.concat([Buffer.from(`${timestamp}.`), body]), key).toString('base64url')
    return `t=${timestamp},kid=${row.kid},ed25519=${signature}`
  }
}
