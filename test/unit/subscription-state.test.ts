import { expect, test } from 'vitest'
import fc from 'fast-check'
import { decideCompletion, decideDelivery, decideMembership, leaseState } from '../../src/subscription-state.js'

test('a published pull wake waits for a claim even when new work or retries arrive', () => {
  fc.assert(fc.property(fc.boolean(), (retryDue) => {
    expect(decideDelivery({ retryDue, wake: { phase: 'published' } })).toBe('wait-for-claim')
  }))
})
test('webhook success without completion waits for a callback and cannot redeliver', () => {
  expect(decideCompletion({ deleted: false, currentWake: 'w', wakeId: 'w', leaseHeld: true, currentClaim: 'c', claim: 'c', ok: true, done: false })).toBe('await-callback')
  expect(decideDelivery({ retryDue: true, wake: { phase: 'delivery', state: 'delivered', available: true } })).toBe('wait-for-delivery')
  expect(decideDelivery({ retryDue: false, wake: { phase: 'delivery', state: 'ready', available: true } })).toBe('backoff')
})
test('expired, deleted, replaced, and recovered work cannot complete or schedule retries', () => {
  fc.assert(fc.property(fc.array(fc.constantFrom('expire', 'delete', 'replace', 'recover'), { minLength: 1, maxLength: 50 }), fc.boolean(), fc.boolean(), (events, ok, done) => {
    const job = { wakeId: 'original-wake', claim: 'original-claim' }
    let currentWake: string | null = job.wakeId, currentClaim: string | null = job.claim, deleted = false, leaseHeld = true
    for (const [index, event] of events.entries()) {
      if (event === 'expire') leaseHeld = false
      if (event === 'delete') { deleted = true; currentWake = null }
      if (event === 'replace') { currentClaim = `replacement-${index}` }
      if (event === 'recover') { currentWake = null; currentClaim = null }
      expect(decideCompletion({ ...job, currentWake, currentClaim, deleted, leaseHeld, ok, done })).toBe('ignore')
    }
  }))
})
test('membership capacity never drops existing members or adopts a stale stream generation', () => {
  fc.assert(fc.property(fc.boolean(), fc.boolean(), (full, explicit) => {
    const member = { stream_id: 'current', explicit_link: explicit }
    expect(decideMembership({ streamId: 'current', activeId: 'current', full, member })).toBe('advance')
    expect(decideMembership({ streamId: 'old', activeId: 'current', full, member })).toBe('ignore')
    expect(decideMembership({ streamId: 'current', activeId: undefined, full, member })).toBe('retire')
    expect(decideMembership({ streamId: 'current', activeId: 'current', full, member: undefined })).toBe(full ? 'defer' : 'insert')
  }))
})
test('lease ownership uses the database validity fact independently of the local clock', () => {
  const until = new Date(0)
  expect(leaseState({ holder: 'worker', lease_until: until, lease_valid: true }).phase).toBe('held')
  expect(leaseState({ holder: 'worker', lease_until: new Date('2999-01-01'), lease_valid: false }).phase).toBe('expired')
  expect(() => leaseState({ holder: 'worker', lease_until: null })).toThrow('owner or deadline')
})
