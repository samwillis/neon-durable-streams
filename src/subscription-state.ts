/** Decisions use facts read under the subscription lock, including database-
 * computed deadlines. No local clock or process owns durable subscription state. */
export type WakeState = 'ready' | 'delivered' | 'completed' | 'expired' | 'fenced'
export type LeaseState = { readonly phase: 'free' } | { readonly phase: 'held' | 'expired'; readonly holder: string; readonly until: Date }
export function leaseState(row: { readonly holder: string | null; readonly lease_until: Date | null; readonly lease_valid?: boolean | null }): LeaseState {
  if (row.holder === null && row.lease_until === null) return { phase: 'free' }
  if (row.holder === null || row.lease_until === null) throw new Error('Subscription lease has no owner or deadline')
  return { phase: row.lease_valid ? 'held' : 'expired', holder: row.holder, until: row.lease_until }
}
export type DeliveryInput = {
  readonly retryDue: boolean
  readonly wake: { readonly phase: 'none'; readonly pending: boolean }
    | { readonly phase: 'published' }
    | { readonly phase: 'delivery'; readonly state: WakeState; readonly available: boolean }
}
export function decideDelivery(input: DeliveryInput): 'wait-for-claim' | 'idle' | 'backoff' | 'issue' | 'wait-for-delivery' | 'deliver' {
  if (input.wake.phase === 'published') return 'wait-for-claim'
  if (input.wake.phase === 'none' && !input.wake.pending) return 'idle'
  if (!input.retryDue) return 'backoff'
  if (input.wake.phase === 'none') return 'issue'
  if (input.wake.state !== 'ready' || !input.wake.available) return 'wait-for-delivery'
  return 'deliver'
}
export function decideCompletion(input: {
  readonly deleted: boolean; readonly currentWake: string | null; readonly wakeId: string
  readonly leaseHeld: boolean; readonly currentClaim: string | null; readonly claim: string
  readonly ok: boolean; readonly done: boolean
}): 'ignore' | 'retry' | 'complete' | 'await-callback' {
  if (input.deleted || input.currentWake !== input.wakeId || !input.leaseHeld || input.currentClaim !== input.claim) return 'ignore'
  if (!input.ok) return 'retry'
  return input.done ? 'complete' : 'await-callback'
}
export function decideMembership(input: {
  readonly streamId: string; readonly activeId: string | undefined; readonly full: boolean
  readonly member: { readonly stream_id: string | null; readonly explicit_link: boolean } | undefined
}): 'ignore' | 'retire' | 'defer' | 'insert' | 'replace' | 'advance' {
  if (input.activeId !== input.streamId) return input.member?.stream_id === input.streamId ? 'retire' : 'ignore'
  if (!input.member) return input.full ? 'defer' : 'insert'
  return input.member.stream_id === input.streamId ? 'advance' : 'replace'
}
