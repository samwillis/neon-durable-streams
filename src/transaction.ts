import type { Pool, PoolClient } from 'pg'

export interface TransactionOptions {
  readonly readOnly?: boolean
  /** Snapshot replay can write its pins while preserving one MVCC snapshot. */
  readonly repeatableRead?: boolean
  readonly timeoutMs?: number
  readonly lockTimeoutMs?: number
}

/** Never retry here: a lost COMMIT response has an ambiguous outcome. */
export async function transaction<T>(
  pool: Pool,
  options: TransactionOptions,
  work: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const timeout = options.timeoutMs ?? 10_000
  const lockTimeout = options.lockTimeoutMs ?? 5_000
  if (![timeout, lockTimeout].every((n) => Number.isSafeInteger(n) && n > 0 && n <= 300_000)) throw new RangeError('Invalid transaction timeout')
  const client = await pool.connect()
  let releaseError: Error | undefined
  let phase: 'begin' | 'work' | 'commit' = 'begin'
  try {
    const begin = options.readOnly ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'
      : options.repeatableRead ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE'
      : 'BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE'
    // Both interpolated values were validated as bounded positive integers.
    // One simple-protocol round trip preserves explicit BEGIN/SET LOCAL while
    // avoiding a network round trip before any application SQL can execute.
    await client.query(`${begin}; SELECT set_config('synchronous_commit', 'on', true),
      set_config('statement_timeout', '${timeout}', true), set_config('lock_timeout', '${lockTimeout}', true),
      set_config('idle_in_transaction_session_timeout', '${timeout}', true), set_config('transaction_timeout', '${timeout}', true)`)
    phase = 'work'
    const result = await work(client)
    phase = 'commit'
    await client.query('COMMIT')
    return result
  } catch (error) {
    const original = error instanceof Error ? error : new Error('Transaction failed')
    // A fully rolled-back application conflict can reuse its connection.
    // A failed COMMIT/begin or rollback discards uncertain session state.
    if (phase !== 'work') releaseError = original
    try { await client.query('ROLLBACK') } catch { releaseError = original }
    throw error
  } finally {
    client.release(releaseError)
  }
}
