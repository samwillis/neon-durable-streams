import { randomUUID } from 'node:crypto'
import type { Pool, PoolClient, QueryResultRow } from 'pg'
import { StorageCompatibilityError, StreamError } from './errors.js'
import { advanceOffset, parseOffset, ZERO_OFFSET } from './offset.js'
import { formatPayloads, mediaType, preparePayload } from './payload.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'

export interface Producer {
  readonly id: string
  readonly epoch: number
  readonly seq: number
}

export interface StreamMetadata extends QueryResultRow {
  id: string
  tenant_id: string
  path: string
  content_type: string
  tail_offset: string
  change_version: string
  closed: boolean
  closed_by: Producer | null
  last_seq: string | null
  ttl_seconds: number | null
  expires_at: Date | null
}

interface LockedStream extends StreamMetadata { expired: boolean }
export interface CreateOptions {
  readonly contentType: string
  readonly body: Uint8Array
  readonly closed: boolean
  readonly ttlSeconds?: number
  readonly expiresAt?: Date
}

export interface AppendOptions {
  readonly body: Uint8Array
  readonly contentType: string | null
  readonly close: boolean
  readonly producer?: Producer
  readonly sequence?: string
}

export interface AppendResult {
  readonly stream: StreamMetadata
  readonly status: 200 | 204
  readonly producer?: { epoch: number; seq: number }
}

/** Internal hot-log storage. Sealing/forks/subscriptions are later milestones. */
export class PostgresStreamStore {
  readonly schema: string
  private ready: Promise<void> | undefined

  constructor(
    private readonly pool: Pool,
    schema: string,
    private readonly maxStoredBytes = 1024 * 1024,
    private readonly maxPageBytes = 2 * 1024 * 1024,
  ) {
    this.schema = validateSchema(schema)
    if (!Number.isSafeInteger(maxStoredBytes) || maxStoredBytes < 1 || maxStoredBytes > 16 * 1024 * 1024
      || !Number.isSafeInteger(maxPageBytes) || maxPageBytes < maxStoredBytes + 2 || maxPageBytes > 64 * 1024 * 1024) {
      throw new RangeError('Invalid payload/page limits')
    }
  }

  async assertReady(): Promise<void> {
    this.ready ??= this.pool.query<{ storage_version: number }>(`SELECT storage_version FROM ${this.schema}.installation`)
      .then(async (result) => {
        if (result.rows.length !== 1 || result.rows[0]!.storage_version !== 1) throw new StorageCompatibilityError('Unsupported storage version')
        const migrations = await this.pool.query<{ id: string }>(`SELECT id FROM ${this.schema}.migrations ORDER BY id`)
        if (migrations.rows.length !== 1 || migrations.rows[0]!.id !== '0001_core') throw new StorageCompatibilityError('Unsupported database migrations')
      })
      .catch((error: unknown) => { this.ready = undefined; throw error })
    return this.ready
  }

  private async lock(client: PoolClient, tenant: string, path: string): Promise<LockedStream | undefined> {
    const claimed = await client.query<{ active_stream_id: string | null }>(`SELECT active_stream_id FROM ${this.schema}.paths
      WHERE tenant_id = $1 AND path = $2 FOR UPDATE`, [tenant, path])
    const id = claimed.rows[0]?.active_stream_id
    if (!id) return undefined
    const result = await client.query<LockedStream>(`SELECT s.*,
      COALESCE(s.expires_at <= clock_timestamp() OR
        s.accessed_at + s.ttl_seconds * interval '1 second' <= clock_timestamp(), false) AS expired
      FROM ${this.schema}.streams s
      WHERE s.id = $1 AND s.deleted_at IS NULL FOR UPDATE`, [id])
    return result.rows[0]
  }

  private async change(client: PoolClient, stream: StreamMetadata, before: string, kind: string): Promise<void> {
    await client.query(`INSERT INTO ${this.schema}.stream_changes
      (stream_id, version, before_offset, after_offset, kind) VALUES ($1, $2, $3, $4, $5)`,
    [stream.id, stream.change_version, before, stream.tail_offset, kind])
  }

  private async retire(client: PoolClient, stream: StreamMetadata, kind: 'deleted' | 'expired'): Promise<void> {
    const result = await client.query<StreamMetadata>(`UPDATE ${this.schema}.streams
      SET deleted_at = clock_timestamp(), change_version = change_version + 1 WHERE id = $1 RETURNING *`, [stream.id])
    await client.query(`UPDATE ${this.schema}.paths SET active_stream_id = NULL WHERE tenant_id = $1 AND path = $2 AND active_stream_id = $3`,
      [stream.tenant_id, stream.path, stream.id])
    await this.change(client, result.rows[0]!, stream.tail_offset, kind)
  }

  private async withStream<T>(tenant: string, path: string, work: (client: PoolClient, stream: LockedStream) => Promise<T>): Promise<T> {
    await this.assertReady()
    const outcome = await transaction(this.pool, {}, async (client) => {
      const stream = await this.lock(client, tenant, path)
      if (!stream) return { found: false } as const
      if (stream.expired) {
        await this.retire(client, stream, 'expired')
        return { found: false } as const
      }
      return { found: true, value: await work(client, stream) } as const
    })
    // Missing/expired is reported after committing an expiry transition.
    if (!outcome.found) throw new StreamError(404, 'STREAM_NOT_FOUND', 'Stream not found')
    return outcome.value
  }

  async create(tenant: string, path: string, options: CreateOptions): Promise<{ stream: StreamMetadata; created: boolean }> {
    await this.assertReady()
    const payload = preparePayload(options.body, options.contentType, true)
    this.checkPayload(payload.bytes)
    return transaction(this.pool, {}, async (client) => {
      await client.query(`INSERT INTO ${this.schema}.paths (tenant_id, path) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [tenant, path])
      await client.query(`SELECT path FROM ${this.schema}.paths WHERE tenant_id = $1 AND path = $2 FOR UPDATE`, [tenant, path])
      let existing = await this.lock(client, tenant, path)
      if (existing?.expired) { await this.retire(client, existing, 'expired'); existing = undefined }
      if (existing) {
        if (mediaType(existing.content_type) !== mediaType(options.contentType) || existing.closed !== options.closed
          || existing.ttl_seconds !== (options.ttlSeconds ?? null)
          || (existing.expires_at?.toISOString() ?? null) !== (options.expiresAt?.toISOString() ?? null)) {
          throw new StreamError(409, 'CONFIGURATION_MISMATCH', 'Stream configuration differs')
        }
        return { stream: existing, created: false }
      }
      const id = randomUUID()
      const tail = payload.bytes.length ? advanceOffset(ZERO_OFFSET, payload.bytes.length) : ZERO_OFFSET
      const result = await client.query<StreamMetadata>(`INSERT INTO ${this.schema}.streams
        (id, tenant_id, path, content_type, tail_offset, change_version, closed, ttl_seconds, expires_at)
        VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8) RETURNING *`,
      [id, tenant, path, options.contentType, tail, options.closed, options.ttlSeconds ?? null, options.expiresAt ?? null])
      const stream = result.rows[0]!
      await client.query(`UPDATE ${this.schema}.paths SET active_stream_id = $3 WHERE tenant_id = $1 AND path = $2`, [tenant, path, id])
      if (payload.bytes.length) await this.insertAppend(client, id, ZERO_OFFSET, tail, payload.bytes, payload.messageEnds)
      await this.change(client, stream, ZERO_OFFSET, 'created')
      return { stream, created: true }
    })
  }

  private checkPayload(bytes: Uint8Array): void {
    if (bytes.byteLength > this.maxStoredBytes) throw new StreamError(413, 'PAYLOAD_TOO_LARGE', 'Stored append exceeds configured limit')
  }

  private async insertAppend(client: PoolClient, id: string, start: string, end: string, bytes: Uint8Array, ends: readonly number[] | null): Promise<void> {
    await client.query(`INSERT INTO ${this.schema}.appends (stream_id, start_offset, end_offset, payload, message_ends)
      VALUES ($1, $2, $3, $4, $5)`, [id, start, end, bytes, ends])
  }

  async append(tenant: string, path: string, options: AppendOptions): Promise<AppendResult> {
    return this.withStream(tenant, path, async (client, stream) => {
      const producer = options.producer
      if (stream.closed) {
        const duplicate = producer && stream.closed_by?.id === producer.id && stream.closed_by.epoch === producer.epoch && stream.closed_by.seq === producer.seq
        if (duplicate || (!producer && options.close && options.body.length === 0)) {
          return { stream, status: 204, ...(producer ? { producer: { epoch: producer.epoch, seq: producer.seq } } : {}) }
        }
        throw new StreamError(409, 'STREAM_CLOSED', 'Stream is closed', { 'Stream-Closed': 'true', 'Stream-Next-Offset': stream.tail_offset })
      }
      if (options.body.length && (!options.contentType || mediaType(options.contentType) !== mediaType(stream.content_type))) {
        throw new StreamError(options.contentType ? 409 : 400, 'CONTENT_TYPE_MISMATCH', 'Content-Type must match the stream')
      }
      if (producer) {
        const result = await client.query<{ epoch: string; last_seq: string }>(`SELECT epoch, last_seq FROM ${this.schema}.producers WHERE stream_id = $1 AND producer_id = $2`, [stream.id, producer.id])
        const prior = result.rows[0]
        const epoch = prior ? Number(prior.epoch) : -1
        const last = prior ? Number(prior.last_seq) : -1
        if (producer.epoch < epoch) throw new StreamError(403, 'STALE_PRODUCER', 'Producer epoch is fenced', { 'Producer-Epoch': String(epoch) })
        if (producer.epoch === epoch && producer.seq <= last) return { stream, status: 204, producer: { epoch, seq: last } }
        const expected = producer.epoch === epoch ? last + 1 : 0
        if (producer.seq !== expected) {
          if (prior && producer.epoch > epoch) throw new StreamError(400, 'NEW_EPOCH_SEQUENCE', 'A new epoch must start at sequence zero')
          throw new StreamError(409, 'PRODUCER_SEQUENCE_GAP', 'Producer sequence gap', { 'Producer-Expected-Seq': String(expected), 'Producer-Received-Seq': String(producer.seq) })
        }
      }
      if (options.sequence !== undefined && stream.last_seq !== null && Buffer.compare(Buffer.from(options.sequence), Buffer.from(stream.last_seq)) <= 0) {
        throw new StreamError(409, 'SEQUENCE_REGRESSION', 'Stream-Seq must increase')
      }
      if (!options.body.length && !options.close) throw new StreamError(400, 'EMPTY_APPEND', 'Append body must not be empty')
      const payload = options.body.length ? preparePayload(options.body, stream.content_type) : { bytes: Buffer.alloc(0), messageEnds: null }
      this.checkPayload(payload.bytes)
      const next = payload.bytes.length ? advanceOffset(stream.tail_offset, payload.bytes.length) : stream.tail_offset
      if (payload.bytes.length) await this.insertAppend(client, stream.id, stream.tail_offset, next, payload.bytes, payload.messageEnds)
      if (producer) await client.query(`INSERT INTO ${this.schema}.producers (stream_id, producer_id, epoch, last_seq)
        VALUES ($1, $2, $3, $4) ON CONFLICT (stream_id, producer_id) DO UPDATE SET epoch = EXCLUDED.epoch, last_seq = EXCLUDED.last_seq`,
      [stream.id, producer.id, producer.epoch, producer.seq])
      const updated = await client.query<StreamMetadata>(`UPDATE ${this.schema}.streams SET tail_offset = $2,
        change_version = change_version + 1, closed = $3, closed_by = $4, last_seq = COALESCE($5, last_seq), accessed_at = clock_timestamp()
        WHERE id = $1 RETURNING *`, [stream.id, next, options.close, options.close && producer ? producer : null, options.sequence ?? null])
      const value = updated.rows[0]!
      await this.change(client, value, stream.tail_offset, options.close ? 'closed' : 'appended')
      return { stream: value, status: producer ? 200 : 204, ...(producer ? { producer: { epoch: producer.epoch, seq: producer.seq } } : {}) }
    })
  }

  async head(tenant: string, path: string): Promise<StreamMetadata> {
    return this.withStream(tenant, path, async (_client, stream) => stream)
  }

  async read(tenant: string, path: string, offset: string): Promise<{ stream: StreamMetadata; body: Buffer; next: string; upToDate: boolean }> {
    if (!['-1', 'now'].includes(offset)) parseOffset(offset)
    const accessed = await this.withStream(tenant, path, async (client, stream) => {
      if (stream.ttl_seconds !== null) await client.query(`UPDATE ${this.schema}.streams SET accessed_at = clock_timestamp() WHERE id = $1`, [stream.id])
      return stream
    })
    return transaction(this.pool, { readOnly: true }, async (client) => {
      const metadata = await client.query<StreamMetadata>(`SELECT * FROM ${this.schema}.streams WHERE id = $1 AND deleted_at IS NULL`, [accessed.id])
      const stream = metadata.rows[0]
      if (!stream) throw new StreamError(404, 'STREAM_NOT_FOUND', 'Stream no longer exists')
      const start = offset === '-1' ? ZERO_OFFSET : offset === 'now' ? stream.tail_offset : offset
      if (start > stream.tail_offset) throw new StreamError(400, 'OFFSET_BEYOND_TAIL', 'Offset exceeds stream tail')
      if (start !== ZERO_OFFSET && start !== stream.tail_offset) {
        const boundary = await client.query(`SELECT 1 FROM ${this.schema}.appends WHERE stream_id = $1 AND end_offset = $2`, [stream.id, start])
        if (!boundary.rowCount) throw new StreamError(400, 'OFFSET_NOT_BOUNDARY', 'Offset is not a returned boundary')
      }
      const result = await client.query<{ end_offset: string; payload: Buffer }>(`SELECT end_offset, payload FROM ${this.schema}.appends
        WHERE stream_id = $1 AND end_offset > $2 ORDER BY end_offset LIMIT 16`, [stream.id, start])
      const payloads: Buffer[] = []
      let length = 2
      let next = start
      for (const row of result.rows) {
        if (length + row.payload.length > this.maxPageBytes) break
        payloads.push(row.payload)
        length += row.payload.length
        next = row.end_offset
      }
      return { stream, body: formatPayloads(payloads, stream.content_type), next, upToDate: next === stream.tail_offset }
    })
  }

  async delete(tenant: string, path: string): Promise<void> {
    await this.withStream(tenant, path, (client, stream) => this.retire(client, stream, 'deleted'))
  }
}
