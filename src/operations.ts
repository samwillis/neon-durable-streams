import { randomUUID } from 'node:crypto'
import type { Pool } from 'pg'
import { decodeChunk } from './chunk-codec.js'
import { StorageCompatibilityError } from './errors.js'
import type { InstallationIdentity } from './installation.js'
import { validateSchema } from './path.js'
import type { ChunkStorage } from './storage.js'
import { transaction } from './transaction.js'
import { SubscriptionSigner } from './subscription-crypto.js'
import type { SubscriptionKeyring } from './subscription-crypto.js'

export type { InstallationIdentity } from './installation.js'
export interface OperationTarget { readonly schema?: string }
export interface RecoveryOptions extends OperationTarget {
  /** Persist this UUID before calling; reuse it after a lost response. */
  readonly operationId: string
  readonly expectedInstallationId: string
  readonly expectedServingEpoch: string
  /** A stable deployment identity, normally the consumer's Neon branch ID. */
  readonly environmentId: string
  /** The target branch/bucket. Every sealed object must already exist there. */
  readonly storageIdentity: string | null
  /** Set when adopting to a new server URL; omit to retain the original URL. */
  readonly publicBaseUrl?: string
}
interface RecoveryRow extends InstallationIdentity {
  recovery_token: string | null
  recovery_source_epoch: string | null
  completed_recovery_token: string | null
  recovery_target_public_base_url: string | null
  recovery_target_environment: string | null
  recovery_target_storage: string | null
  recovery_cursor: string | null
  recovery_verified: string
  recovery_complete: boolean
}
function target(options: OperationTarget): string { return validateSchema(options.schema ?? 'durable_streams') }
function validRecovery(row: RecoveryRow | undefined, token: string): asserts row is RecoveryRow {
  if (!row || row.serving_state !== 'recovering' || row.recovery_token !== token) throw new StorageCompatibilityError('Recovery token is stale or installation is active')
}
/** Administrative data-plane operation; never expose through the stream API. */
export async function inspectInstallation(pool: Pool, options: OperationTarget = {}): Promise<InstallationIdentity> {
  const result = await pool.query<InstallationIdentity>(`SELECT id, serving_epoch, serving_state, environment_id, storage_identity FROM ${target(options)}.installation`)
  if (result.rows.length !== 1) throw new StorageCompatibilityError('Missing installation identity')
  return result.rows[0]!
}

export interface ReclamationPolicyOptions extends OperationTarget {
  /** An active, explicitly quiesced recovery operation from beginRecovery. */
  readonly token: string
  readonly enabled: boolean
  /** Include the entire supported PITR AND external-backup recovery window. */
  readonly recoveryWindowMs: number
  /** At least fifteen minutes in addition to the recovery window. */
  readonly safetyMarginMs?: number
}
/** Once per recovery operation; retry with identical settings after lost replies. */
export async function configureReclamation(pool: Pool, options: ReclamationPolicyOptions): Promise<void> {
  const schema = target(options), margin = options.safetyMarginMs ?? 900_000
  if (typeof options.enabled !== 'boolean' || !Number.isSafeInteger(options.recoveryWindowMs) || options.recoveryWindowMs < 0 || options.recoveryWindowMs > 315_576_000_000
    || !Number.isSafeInteger(margin) || margin < 900_000 || margin > 2_147_483_647) throw new RangeError('Invalid reclamation recovery policy')
  await transaction(pool, {}, async (client) => {
    const row = (await client.query<RecoveryRow>(`SELECT * FROM ${schema}.installation FOR UPDATE`)).rows[0]
    validRecovery(row, options.token)
    const policy = (await client.query<{ configured_recovery_token: string | null; enabled: boolean; recovery_window_ms: string; safety_margin_ms: number }>(`SELECT * FROM ${schema}.reclamation_policy FOR UPDATE`)).rows[0]!
    if (policy.configured_recovery_token === options.token) {
      if (policy.enabled !== options.enabled || BigInt(policy.recovery_window_ms) !== BigInt(options.recoveryWindowMs) || policy.safety_margin_ms !== margin) {
        throw new StorageCompatibilityError('This recovery operation already configured a different reclamation policy')
      }
      return
    }
    await client.query(`UPDATE ${schema}.reclamation_policy SET enabled=$1,recovery_window_ms=$2,safety_margin_ms=$3,configured_recovery_token=$4,
      inventory_cursor=NULL,hold_until=GREATEST(hold_until,clock_timestamp()+($2::bigint+$3::integer)*interval '1 millisecond')`, [options.enabled, options.recoveryWindowMs, margin, options.token])
    // Existing promises may be extended, never shortened by a policy change.
    // The global hold above covers a restore that discarded older promises.
  })
}

/** Waits for existing writer transactions, changes epoch, and stops admission. */
export async function beginRecovery(pool: Pool, options: RecoveryOptions): Promise<{ token: string; servingEpoch: string }> {
  const schema = target(options)
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(options.operationId)) throw new TypeError('Recovery operationId must be a UUID')
  if (!options.environmentId || Buffer.byteLength(options.environmentId) > 2048
    || (options.storageIdentity !== null && (!options.storageIdentity || options.storageIdentity.length > 2048))) throw new TypeError('Invalid recovery destination')
  let publicBase: string | null = null
  if (options.publicBaseUrl !== undefined) {
    const url = new URL(options.publicBaseUrl)
    if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname.endsWith('/')) throw new TypeError('Invalid recovery public base URL')
    publicBase = url.href
  }
  return transaction(pool, {}, async (client) => {
    // FOR UPDATE conflicts with all writer KEY SHARE guards. Do not replace
    // this with an ordinary UPDATE (which would take NO KEY UPDATE).
    const current = (await client.query<RecoveryRow>(`SELECT * FROM ${schema}.installation FOR UPDATE`)).rows[0]
    if (current?.serving_state === 'recovering' && current.id === options.expectedInstallationId && current.recovery_token === options.operationId
      && current.recovery_source_epoch === options.expectedServingEpoch && current.recovery_target_environment === options.environmentId
      && current.recovery_target_storage === options.storageIdentity && current.recovery_target_public_base_url === publicBase) {
      return { token: options.operationId, servingEpoch: current.serving_epoch }
    }
    if (!current || current.id !== options.expectedInstallationId || current.serving_epoch !== options.expectedServingEpoch || current.serving_state !== 'active') {
      throw new StorageCompatibilityError('Installation changed; inspect its current identity before starting recovery')
    }
    if (current.storage_identity !== null && options.storageIdentity === null) throw new StorageCompatibilityError('A tiered installation requires object storage during recovery')
    const token = options.operationId, servingEpoch = randomUUID()
    await client.query(`UPDATE ${schema}.installation SET serving_state = 'recovering', serving_epoch = $1, recovery_token = $2, recovery_source_epoch = $6,
      recovery_target_environment = $3, recovery_target_storage = $4, recovery_target_public_base_url = $5, recovery_cursor = NULL, recovery_verified = 0,
      recovery_complete = false, recovery_started_at = clock_timestamp()`, [servingEpoch, token, options.environmentId, options.storageIdentity, publicBase, options.expectedServingEpoch])
    return { token, servingEpoch }
  })
}

export interface VerifyRecoveryOptions extends OperationTarget {
  readonly token: string
  readonly objects?: ChunkStorage
  readonly keyring?: SubscriptionKeyring
  readonly maxChunks?: number
}
/** Resumable, checksum-validated reconciliation. Object I/O holds no SQL lock. */
export async function verifyRecovery(pool: Pool, options: VerifyRecoveryOptions): Promise<{ complete: boolean; verified: number }> {
  const schema = target(options), maximum = options.maxChunks ?? 16
  if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid recovery batch size')
  const before = (await pool.query<RecoveryRow>(`SELECT * FROM ${schema}.installation`)).rows[0]
  validRecovery(before, options.token)
  if ((options.objects?.identity ?? null) !== before.recovery_target_storage) throw new StorageCompatibilityError('Recovery object storage differs from the recorded target')
  const signing = await pool.query(`SELECT 1 FROM ${schema}.webhook_signing_keys WHERE active`)
  if (signing.rowCount) {
    if (!options.keyring) throw new StorageCompatibilityError('Recovery requires the subscription wrapping keyring')
    await new SubscriptionSigner(pool, options.schema ?? 'durable_streams', options.keyring).signature(Buffer.alloc(0))
  }
  if (before.recovery_complete) return { complete: true, verified: Number(before.recovery_verified) }
  const reclamation = await pool.query(`SELECT 1 FROM ${schema}.migrations WHERE id='0006_reclamation'`)
  const manifests = await pool.query<{ id: string; stream_id: string; object_key: string; sha256: string; encoded_bytes: number; frame_count: number; payload_bytes: string; start_offset: string; end_offset: string }>(
    `SELECT * FROM ${schema}.chunks WHERE state = 'sealed' ${reclamation.rowCount ? 'AND retired_at IS NULL' : ''}
      AND ($1::uuid IS NULL OR id > $1) ORDER BY id LIMIT $2`, [before.recovery_cursor, maximum])
  for (const chunk of manifests.rows) {
    if (!options.objects) throw new StorageCompatibilityError('Sealed manifests require target object storage')
    const bytes = await options.objects.get(chunk.object_key, chunk.encoded_bytes)
    if (bytes.length !== chunk.encoded_bytes) throw new StorageCompatibilityError('Recovery object length differs from manifest')
    const frames = decodeChunk(bytes, { installationId: before.id, streamId: chunk.stream_id, chunkId: chunk.id }, chunk.sha256).frames
    if (frames.length !== chunk.frame_count || frames[0]?.start !== chunk.start_offset || frames.at(-1)?.end !== chunk.end_offset
      || BigInt(frames.reduce((n, frame) => n + frame.payload.length, 0)) !== BigInt(chunk.payload_bytes)) throw new StorageCompatibilityError('Recovery object frames differ from manifest')
  }
  return transaction(pool, {}, async (client) => {
    const current = (await client.query<RecoveryRow>(`SELECT * FROM ${schema}.installation FOR UPDATE`)).rows[0]
    validRecovery(current, options.token)
    if (current.recovery_cursor !== before.recovery_cursor || current.recovery_verified !== before.recovery_verified) throw new StorageCompatibilityError('Recovery batch advanced concurrently; retry from its persisted cursor')
    const complete = manifests.rows.length < maximum
    const verified = Number(before.recovery_verified) + manifests.rows.length
    await client.query(`UPDATE ${schema}.installation SET recovery_cursor = $1, recovery_verified = $2, recovery_complete = $3`,
      [manifests.rows.at(-1)?.id ?? before.recovery_cursor, verified, complete])
    return { complete, verified }
  })
}

/** Activates only a completed reconciliation and fences all restored wakes. */
export async function finishRecovery(pool: Pool, options: OperationTarget & { readonly token: string }): Promise<InstallationIdentity> {
  const schema = target(options)
  return transaction(pool, { timeoutMs: 60_000 }, async (client) => {
    const row = (await client.query<RecoveryRow>(`SELECT * FROM ${schema}.installation FOR UPDATE`)).rows[0]
    if (row?.serving_state === 'active' && row.completed_recovery_token === options.token) {
      return { id: row.id, serving_epoch: row.serving_epoch, serving_state: row.serving_state, environment_id: row.environment_id, storage_identity: row.storage_identity }
    }
    validRecovery(row, options.token)
    if (!row.recovery_complete) throw new StorageCompatibilityError('Verify every sealed manifest before activating this installation')
    const readers = await client.query(`SELECT 1 FROM ${schema}.live_leases WHERE expires_at > clock_timestamp() AND serving_epoch <> $1 LIMIT 1`, [row.serving_epoch])
    if (readers.rowCount) throw new StorageCompatibilityError('Drain active live readers or wait for their bounded leases before activating recovery')
    const reclamation = await client.query(`SELECT 1 FROM ${schema}.migrations WHERE id='0006_reclamation'`)
    if (reclamation.rowCount) {
      const replays = await client.query(`SELECT 1 FROM ${schema}.replay_pins WHERE expires_at>clock_timestamp() AND serving_epoch<>$1 LIMIT 1`, [row.serving_epoch])
      if (replays.rowCount) throw new StorageCompatibilityError('Drain active replay readers or wait for their bounded pins before activating recovery')
      await client.query(`DELETE FROM ${schema}.replay_pins`)
      await client.query(`UPDATE ${schema}.chunk_objects SET claim_token=NULL, claim_until=NULL, claim_epoch=claim_epoch+1 WHERE retired_at IS NOT NULL`)
      await client.query(`UPDATE ${schema}.reclamation_policy SET inventory_cursor=NULL,
        hold_until=GREATEST(hold_until,clock_timestamp()+(recovery_window_ms+safety_margin_ms)*interval '1 millisecond')`)
    }
    // Process eviction can leave durable claims and leases behind. UUID tokens
    // invalidate callbacks issued on either side of a point-in-time rollback.
    await client.query(`UPDATE ${schema}.chunks SET claim_token = NULL, claim_until = NULL, claim_epoch = claim_epoch + 1 WHERE state = 'frozen'`)
    await client.query(`UPDATE ${schema}.subscription_wakes SET state = 'fenced', delivery_claim = NULL, delivery_until = NULL WHERE state IN ('ready', 'delivered')`)
    await client.query(`UPDATE ${schema}.subscriptions SET current_wake = NULL, holder = NULL, lease_until = NULL,
      retry_count = 0, retry_at = NULL, next_work_at = clock_timestamp(),
      public_base_url = COALESCE((SELECT recovery_target_public_base_url FROM ${schema}.installation), public_base_url) WHERE deleted_at IS NULL`)
    await client.query(`DELETE FROM ${schema}.live_leases`)
    const activated = await client.query<InstallationIdentity>(`UPDATE ${schema}.installation SET serving_state = 'active',
      environment_id = recovery_target_environment, storage_identity = recovery_target_storage, completed_recovery_token = recovery_token, recovery_token = NULL
      RETURNING id, serving_epoch, serving_state, environment_id, storage_identity`)
    return activated.rows[0]!
  })
}
