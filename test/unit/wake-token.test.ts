import { randomBytes, randomUUID } from 'node:crypto'
import { expect, test } from 'vitest'
import { WakeTokens } from '../../src/wake-token.js'

const identity = () => ({ installationId: randomUUID(), servingEpoch: randomUUID(), tenantId: 'tenant', subscriptionId: randomUUID(),
  name: 'worker', wakeId: randomUUID(), generation: 5, kind: 'pull-wake' as const })
test('signed tokens authenticate wake identity without a persisted wake record', () => {
  const tokens = new WakeTokens({ currentKeyId: 'v1', keys: { v1: randomBytes(32) } }), claims = identity()
  const token = tokens.sign(claims)
  expect(tokens.verify(token)).toMatchObject({ ...claims, v: 2 })
  expect(tokens.sign(claims)).not.toBe(token)
})
test('key rotation verifies retained keys and rejects missing or incorrect keys', () => {
  const v1 = randomBytes(32), v2 = randomBytes(32)
  const token = new WakeTokens({ currentKeyId: 'v1', keys: { v1 } }).sign(identity())
  expect(new WakeTokens({ currentKeyId: 'v2', keys: { v1, v2 } }).verify(token)).toBeDefined()
  expect(new WakeTokens({ currentKeyId: 'v2', keys: { v2 } }).verify(token)).toBeUndefined()
  expect(new WakeTokens({ currentKeyId: 'v1', keys: { v1: v2 } }).verify(token)).toBeUndefined()
})
test('payload, key ID, signature, and oversized input mutations cannot authenticate', () => {
  const tokens = new WakeTokens({ currentKeyId: 'v1', keys: { v1: randomBytes(32) } }), token = tokens.sign(identity())
  const parts = token.split('.')
  const body = JSON.parse(Buffer.from(parts[3]!, 'base64url').toString('utf8')) as Record<string, unknown>
  body.tenantId = 'another-tenant'
  expect(tokens.verify([...parts.slice(0, 3), Buffer.from(JSON.stringify(body)).toString('base64url'), parts[4]].join('.'))).toBeUndefined()
  expect(tokens.verify(token.replace('.v1.', '.v2.'))).toBeUndefined()
  expect(tokens.verify(token.slice(0, -2))).toBeUndefined()
  expect(tokens.verify(token + '=' )).toBeUndefined()
  expect(tokens.verify('x'.repeat(8193))).toBeUndefined()
})
test('the keyring is captured before caller mutation and identities have finite bounds', () => {
  const key = randomBytes(32), ring = { currentKeyId: 'v1', keys: { v1: key } }, tokens = new WakeTokens(ring)
  const token = tokens.sign(identity()); key.fill(0); ring.currentKeyId = 'missing'
  expect(tokens.verify(token)).toBeDefined()
  expect(() => tokens.sign({ ...identity(), generation: Number.MAX_SAFE_INTEGER + 1 })).toThrow('identity')
  expect(() => tokens.sign({ ...identity(), tenantId: 'x'.repeat(513) })).toThrow('identity')
})
