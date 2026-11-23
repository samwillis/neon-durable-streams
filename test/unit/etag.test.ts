import { expect, test } from 'vitest'
import { replayEtag, matchesEtag } from '../../src/etag.js'

const identity = { generationId: 'stream', servingEpoch: 'epoch', start: 'start', end: 'end', contentType: 'application/json', closed: false }
test('replay validators change with byte identity, serving epoch, encoding and closure', () => {
  const etag = replayEtag(identity)
  expect(replayEtag({ ...identity })).toBe(etag)
  for (const patch of [{ generationId: 'other' }, { servingEpoch: 'restored' }, { start: 'later' }, { end: 'later' }, { contentType: 'text/plain' }, { closed: true }]) {
    expect(replayEtag({ ...identity, ...patch })).not.toBe(etag)
  }
})
test('conditional GET recognizes weak validators, lists and wildcard', () => {
  const etag = replayEtag(identity)
  expect(matchesEtag(`"different", W/${etag}`, etag)).toBe(true)
  expect(matchesEtag('*', etag)).toBe(true)
  expect(matchesEtag('"different"', etag)).toBe(false)
  expect(matchesEtag(undefined, etag)).toBe(false)
})
