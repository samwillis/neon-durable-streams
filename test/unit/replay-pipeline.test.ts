import { expect, test } from 'vitest'
import { replayPipeline } from '../../src/replay.js'

for (const ahead of [0, 1] as const) test(`pipeline preserves order with at most ${ahead + 1} active operations`, async () => {
  let active = 0, maximum = 0
  const result = await replayPipeline([0, 1, 2, 3, 4], async (value) => {
    active++; maximum = Math.max(maximum, active)
    await new Promise((resolve) => setTimeout(resolve, value % 2 ? 1 : 5))
    active--
    return value
  }, ahead)
  expect(result).toEqual([0, 1, 2, 3, 4])
  expect(maximum).toBe(ahead + 1)
  expect(active).toBe(0)
})

test('failed pipeline aborts and settles look-ahead before returning', async () => {
  let settled = false
  await expect(replayPipeline([0, 1, 2], async (value, signal) => {
    if (!value) { await new Promise((resolve) => setTimeout(resolve, 5)); throw new Error('broken block') }
    await new Promise<void>((resolve) => signal.addEventListener('abort', () => { setTimeout(() => { settled = true; resolve() }, 5) }, { once: true }))
    return value
  }, 1)).rejects.toThrow('broken block')
  expect(settled).toBe(true)
})
