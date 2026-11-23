import { expect, test } from 'vitest'
import fc from 'fast-check'
import { directoryFromLengths } from '../../src/boundary-directory.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'

test('relative directories preserve exact boundaries and payload prefix budgets', () => {
  fc.assert(
    fc.property(fc.array(fc.integer({ min: 1, max: 1_000_000 }), { minLength: 1, maxLength: 1024 }), (lengths) => {
      let offset = '0000000000000017_9000000000000000',
        total = 0
      const start = offset
      const frames = lengths.map((bytes) => {
        const before = offset
        offset = advanceOffset(offset, bytes)
        return { start: before, end: offset, bytes }
      })
      const directory = directoryFromLengths(start, frames)
      for (let i = 0; i < directory.length; i++) {
        total += lengths[i]!
        expect(directory[i]! - 5 * (i + 1)).toBe(total)
      }
    }),
  )
})

test('directories reject gaps, changed epochs, empty frames, and relative overflow', () => {
  const end = advanceOffset(ZERO_OFFSET, 1)
  for (const frames of [
    [],
    [{ start: end, end: advanceOffset(end, 1), bytes: 1 }],
    [{ start: ZERO_OFFSET, end: '0000000000000001_0000000000000006', bytes: 1 }],
    [{ start: ZERO_OFFSET, end, bytes: 0 }],
    [{ start: ZERO_OFFSET, end: advanceOffset(ZERO_OFFSET, 2_147_483_647), bytes: 2_147_483_647 }],
  ]) {
    expect(() => directoryFromLengths(ZERO_OFFSET, frames)).toThrow()
  }
})
