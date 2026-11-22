import { randomBytes } from 'node:crypto'
import { expect, test } from 'vitest'
import { SubscriptionCipher, parseSubscriptionKeyring } from '../../src/subscription-crypto.js'
import { globExpression, globMatches, normalizeSubscription, subscriptionRoute } from '../../src/subscription-protocol.js'
import { publicWebhookAddress } from '../../src/webhook.js'

test('subscription globs agree on zero/many segment boundaries and literal regex characters', () => {
  const cases: [string, readonly string[], readonly string[]][] = [
    ['events/*', ['events/a', 'events/🐘', 'events/***'], ['events', 'events/a/b', 'x/events/a']],
    ['events/**', ['events', 'events/a', 'events/a/b'], ['event', 'x/events']],
    ['**/tail', ['tail', 'a/tail', 'a/b/tail'], ['tail/a', 'retail']],
    ['a/**/b', ['a/b', 'a/c/b', 'a/c/d/b'], ['a/b/c', 'a/cb']],
    ['**', ['x', 'a/b'], []],
    ['a.b/*', ['a.b/x'], ['axb/x']],
    ['a/**/b/**/c', ['a/b/c', 'a/x/b/y/z/c'], ['a/b/c/d']],
  ]
  for (const [pattern, yes, no] of cases) {
    const regex = new RegExp(globExpression(pattern))
    for (const path of yes) { expect(regex.test(path), `${pattern} should match ${path}`).toBe(true); expect(globMatches(pattern, path)).toBe(true) }
    for (const path of no) { expect(regex.test(path), `${pattern} should exclude ${path}`).toBe(false); expect(globMatches(pattern, path)).toBe(false) }
  }
})
test('adversarial wildcard repetition uses bounded matching and malformed Unicode is rejected', () => {
  expect(globMatches(`events/${'a*'.repeat(128)}z`, `events/${'a'.repeat(1024)}`)).toBe(false)
  expect(() => normalizeSubscription({ type: 'pull-wake', wake_stream: 'wake/pool', pattern: '\ud800' })).toThrow('Invalid stream pattern')
})
test('normalized hashes ignore explicit-list order and duplicates and include delivery/lease policy', () => {
  const config = { type: 'pull-wake', wake_stream: 'wake/pool', streams: ['events/a', 'events/b'] }
  const first = normalizeSubscription(config)
  expect(normalizeSubscription({ ...config, streams: ['events/b', 'events/a', 'events/a'] }).hash).toBe(first.hash)
  expect(normalizeSubscription({ ...config, lease_ttl_ms: 1000 }).hash).not.toBe(first.hash)
  expect(() => normalizeSubscription({ ...config, pattern: '**' })).toThrow('own wake channel')
})
test('control routing decodes an encoded explicit path once without relaxing ordinary stream paths', () => {
  expect(subscriptionRoute(new URL('https://fixture.test/v1/stream/__ds/subscriptions/sub/streams/events%2Fa'), '/v1/stream')).toMatchObject({ action: 'member', member: 'events/a' })
  expect(() => subscriptionRoute(new URL('https://fixture.test/v1/stream/__ds/subscriptions/sub/streams/events%252Fa'), '/v1/stream')).toThrow()
})
test('encrypted state is bound to its installation/purpose and supports wrapping-key rotation', () => {
  const first = randomBytes(32), second = randomBytes(32)
  const original = new SubscriptionCipher({ currentKeyId: 'v1', keys: { v1: first } })
  const bytes = Buffer.from('private signing material')
  const encrypted = original.encrypt(bytes, 'installation:signing:key')
  expect(encrypted.includes(bytes)).toBe(false)
  const rotated = new SubscriptionCipher({ currentKeyId: 'v2', keys: { v1: first, v2: second } })
  expect(rotated.decrypt(encrypted, 'installation:signing:key')).toEqual(bytes)
  expect(() => rotated.decrypt(encrypted, 'different-installation:signing:key')).toThrow()
  expect(() => new SubscriptionCipher({ currentKeyId: 'v2', keys: { v2: second } }).decrypt(encrypted, 'installation:signing:key')).toThrow('unavailable')
  const tampered = Buffer.from(encrypted); tampered[tampered.length - 1] = tampered[tampered.length - 1]! ^ 1
  expect(() => rotated.decrypt(tampered, 'installation:signing:key')).toThrow()
  expect(parseSubscriptionKeyring(JSON.stringify({ currentKeyId: 'v1', keys: { v1: first.toString('base64') } })).keys.v1).toEqual(first)
})
test('webhook address policy rejects non-public IPv4/IPv6 and mapped-address bypasses', () => {
  for (const address of ['127.0.0.1', '10.1.2.3', '172.16.0.1', '192.168.1.1', '169.254.169.254', '100.64.0.1', '0.0.0.0', '::', '::1', 'fd00::1', 'fe80::1', '::ffff:127.0.0.1', '224.0.0.1']) expect(publicWebhookAddress(address), address).toBe(false)
  for (const address of ['8.8.8.8', '1.1.1.1', '2001:4860:4860::8888']) expect(publicWebhookAddress(address), address).toBe(true)
})
