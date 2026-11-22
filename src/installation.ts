import type { Pool, PoolClient } from 'pg'
import { StorageCompatibilityError, StreamError } from './errors.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'
import type { TransactionOptions } from './transaction.js'

export interface InstallationIdentity {
  readonly id: string
  readonly serving_epoch: string
  readonly serving_state: 'active' | 'recovering'
  readonly environment_id: string | null
  readonly storage_identity: string | null
}

/** Pins a runtime to one serving incarnation. All writer locks follow this lock. */
export class InstallationGuard {
  private readonly schema: string
  private identity: Promise<InstallationIdentity> | undefined
  constructor(private readonly pool: Pool, schema: string, private readonly environmentId?: string,
    private readonly timeouts: Pick<TransactionOptions, 'timeoutMs' | 'lockTimeoutMs'> = {}) {
    this.schema = validateSchema(schema)
    if (environmentId !== undefined && (!environmentId || Buffer.byteLength(environmentId) > 2048)) throw new TypeError('Invalid environment identity')
    for (const timeout of [timeouts.timeoutMs, timeouts.lockTimeoutMs]) {
      if (timeout !== undefined && (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 300_000)) throw new RangeError('Invalid Postgres transaction limits')
    }
  }
  private async pin(): Promise<InstallationIdentity> {
    this.identity ??= this.pool.query<InstallationIdentity>(`SELECT id, serving_epoch, serving_state, environment_id, storage_identity FROM ${this.schema}.installation`)
      .then((result) => {
        const row = result.rows[0]
        if (result.rows.length !== 1 || !row) throw new StorageCompatibilityError('Missing installation identity')
        if (row.environment_id !== null && row.environment_id !== this.environmentId) throw new StorageCompatibilityError('Environment identity differs; explicit branch adoption is required')
        return row
      }).catch((error: unknown) => { this.identity = undefined; throw error })
    return this.identity
  }
  async check(client?: PoolClient, readOnly = true): Promise<InstallationIdentity> {
    const expected = await this.pin()
    const result = await (client ?? this.pool).query<InstallationIdentity>(`SELECT id, serving_epoch, serving_state, environment_id, storage_identity
      FROM ${this.schema}.installation${readOnly ? '' : ' FOR KEY SHARE'}`)
    const row = result.rows[0]
    if (!row || row.id !== expected.id || row.serving_epoch !== expected.serving_epoch || row.serving_state !== 'active'
      || row.environment_id !== expected.environment_id || row.storage_identity !== expected.storage_identity) {
      throw new StreamError(503, 'INSTALLATION_FENCED', 'Installation is quiesced or this runtime must be restarted after recovery', { 'Retry-After': '5' })
    }
    return row
  }
  async run<T>(options: TransactionOptions, work: (client: PoolClient) => Promise<T>): Promise<T> {
    // Pin outside the transaction: a pool with one connection must also work.
    await this.pin()
    return transaction(this.pool, { ...this.timeouts, ...options }, async (client) => {
      await this.check(client, options.readOnly ?? false)
      return work(client)
    })
  }
}
