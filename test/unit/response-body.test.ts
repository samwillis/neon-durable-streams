import { expect, test } from 'vitest'
import { ReplayBudget } from '../../src/replay-budget.js'
import { ownedResponseBody } from '../../src/response-body.js'

test('verified pages retain their reservation through backpressure and release at EOF', async () => {
  const budget = new ReplayBudget(300000)
  const payload = Buffer.alloc(150000, 55)
  const body = ownedResponseBody(payload, { signal: new AbortController().signal, release: budget.acquire(payload.length), timeoutMs: 1000 })
  const reader = body.getReader()
  expect(budget.activeBytes).toBe(payload.length)
  const first = await reader.read()
  expect(first.value!.buffer).toBe(payload.buffer)
  expect(first.value!.length).toBe(65536)
  expect(budget.activeBytes).toBe(payload.length)
  const parts = [first.value!]
  for (;;) {
    const part = await reader.read()
    if (part.done) break
    parts.push(part.value)
  }
  expect(Buffer.concat(parts)).toEqual(payload)
  expect(budget.activeBytes).toBe(0)
  expect(budget.activeRequests).toBe(0)
})

test('cancellation and response expiry release capacity exactly once', async () => {
  const budget = new ReplayBudget(32)
  const abort = new AbortController()
  const body = ownedResponseBody(Buffer.from('payload'), { signal: abort.signal, release: budget.acquire(7), timeoutMs: 1000 })
  await body.cancel()
  abort.abort()
  expect(budget.activeRequests).toBe(0)
  const expired = ownedResponseBody(Buffer.from('payload'), { signal: new AbortController().signal, release: budget.acquire(7), timeoutMs: 5 })
  await expect(expired.getReader().read()).resolves.toMatchObject({ done: false })
  await new Promise((resolve) => setTimeout(resolve, 15))
  expect(budget.activeBytes).toBe(0)
})

test('request and memory budgets reject excess work and tolerate repeated release', () => {
  const budget = new ReplayBudget(10, 2)
  const first = budget.acquire(6)
  expect(() => budget.acquire(5)).toThrow('capacity')
  const second = budget.acquire(4)
  expect(() => budget.acquire(1)).toThrow('capacity')
  first(); first(); second()
  expect(budget.activeBytes).toBe(0)
  budget.close()
  expect(() => budget.acquire(1)).toThrow('capacity')
})
