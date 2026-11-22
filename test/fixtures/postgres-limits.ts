// A CI coordinator may be an ocean away from the Neon branch. Preserve every
// concurrency assertion while allowing its serialized SQL round trips to finish.
// Deployed Function qualification continues to use the production defaults.
export const postgresTestLimits = { transactionTimeoutMs: 120_000, lockTimeoutMs: 90_000 } as const
