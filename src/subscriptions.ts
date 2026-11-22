import type { SubscriptionKeyring } from './subscription-crypto.js'
export { initializeSubscriptions, parseSubscriptionKeyring } from './subscription-crypto.js'
export type { SubscriptionKeyring, InitializeSubscriptionsOptions } from './subscription-crypto.js'
export type { SubscriptionConfig } from './subscription-protocol.js'
export interface SubscriptionOptions {
  readonly keyring: SubscriptionKeyring
  readonly allowLocalhostWebhooks?: boolean
  readonly maxStreams?: number
  readonly maxSubscriptionsPerTenant?: number
  readonly webhookTimeoutMs?: number
}
