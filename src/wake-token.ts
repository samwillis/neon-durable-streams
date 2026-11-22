import { createHmac, hkdfSync, randomBytes, timingSafeEqual } from 'node:crypto'
import { SubscriptionCipher } from './subscription-crypto.js'
import type { SubscriptionKeyring } from './subscription-crypto.js'

export interface WakeTokenIdentity {
  readonly installationId: string
  readonly servingEpoch: string
  readonly tenantId: string
  readonly subscriptionId: string
  readonly name: string
  readonly wakeId: string
  readonly generation: number
  readonly kind: 'webhook' | 'pull-wake'
}
export interface WakeTokenClaims extends WakeTokenIdentity {
  readonly v: 2
  readonly issuedAt: number
  readonly nonce: string
}
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/
function valid(value: unknown): value is WakeTokenClaims {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const claims = value as Record<string, unknown>
  return claims.v === 2 && ['installationId', 'servingEpoch', 'subscriptionId', 'wakeId'].every((key) => typeof claims[key] === 'string' && UUID.test(claims[key]))
    && typeof claims.tenantId === 'string' && !!claims.tenantId && claims.tenantId.isWellFormed() && Buffer.byteLength(claims.tenantId) <= 512
    && typeof claims.name === 'string' && !!claims.name && claims.name.isWellFormed() && Buffer.byteLength(claims.name) <= 2048
    && typeof claims.generation === 'number' && Number.isSafeInteger(claims.generation) && claims.generation >= 1
    && (claims.kind === 'webhook' || claims.kind === 'pull-wake')
    && typeof claims.issuedAt === 'number' && Number.isSafeInteger(claims.issuedAt) && claims.issuedAt >= 0
    && typeof claims.nonce === 'string' && /^[A-Za-z0-9_-]{22}$/.test(claims.nonce)
}

/** Signed identity survives wake pruning. Database time still fences live leases. */
export class WakeTokens {
  private readonly keys = new Map<string, Buffer>()
  private readonly current: string
  constructor(ring: SubscriptionKeyring) {
    new SubscriptionCipher(ring) // Keep key validation identical to encryption.
    this.current = ring.currentKeyId
    for (const [id, bytes] of Object.entries(ring.keys)) this.keys.set(id,
      Buffer.from(hkdfSync('sha256', bytes, Buffer.from('durable-streams-neon'), Buffer.from('wake-token-v2'), 32)))
  }
  sign(identity: WakeTokenIdentity): string {
    const claims: WakeTokenClaims = { v: 2, installationId: identity.installationId, servingEpoch: identity.servingEpoch,
      tenantId: identity.tenantId, subscriptionId: identity.subscriptionId, name: identity.name, wakeId: identity.wakeId,
      generation: identity.generation, kind: identity.kind, issuedAt: Date.now(), nonce: randomBytes(16).toString('base64url') }
    if (!valid(claims)) throw new TypeError('Invalid wake token identity')
    const message = `dsw.2.${this.current}.${Buffer.from(JSON.stringify(claims)).toString('base64url')}`
    const token = `${message}.${createHmac('sha256', this.keys.get(this.current)!).update(message).digest('base64url')}`
    if (token.length > 8192) throw new RangeError('Wake token exceeds the header budget')
    return token
  }
  verify(token: string): WakeTokenClaims | undefined {
    if (token.length > 8192) return undefined
    const match = /^dsw\.2\.([A-Za-z0-9_-]{1,128})\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]{43})$/.exec(token)
    if (!match) return undefined
    const key = this.keys.get(match[1]!)
    if (!key) return undefined
    const signature = Buffer.from(match[3]!, 'base64url')
    if (signature.toString('base64url') !== match[3]) return undefined
    const expected = createHmac('sha256', key).update(token.slice(0, token.lastIndexOf('.'))).digest()
    if (signature.length !== expected.length || !timingSafeEqual(signature, expected)) return undefined
    const bytes = Buffer.from(match[2]!, 'base64url')
    if (bytes.toString('base64url') !== match[2]) return undefined
    try { const claims: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); return valid(claims) ? claims : undefined }
    catch { return undefined }
  }
}
