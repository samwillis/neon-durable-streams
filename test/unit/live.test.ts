import { expect, test } from 'vitest'
import { responseCursor } from '../../src/live.js'

test('CDN cursors advance past echoed future intervals without losing integer precision', () => {
  const future = '9999999999999999999999999999999999999999'
  expect(BigInt(responseCursor(future))).toBeGreaterThan(BigInt(future))
  const current = responseCursor(null)
  expect(BigInt(responseCursor(current))).toBeGreaterThan(BigInt(current))
  expect(responseCursor('invalid', Date.UTC(2024, 9, 9) + 20_000)).toBe('1')
})
