import { randomUUID } from 'node:crypto'
import type { Pool, PoolClient, QueryResultRow } from 'pg'
import { StorageCompatibilityError, StreamError } from './errors.js'
import { advanceOffset, parseOffset, ZERO_OFFSET } from './offset.js'
import { formatPayloads, mediaType, preparePayload } from './payload.js'
import { validateSchema } from './path.js'
import { transaction } from './transaction.js'
import { MIGRATION_IDS } from './migrations.js'
import { ChunkManager } from './storage.js'
import type { StorageOptions, StoredFrame } from './storage.js'
import type { ChunkFrame } from './chunk-codec.js'

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
  deleted_at: Date | null
  source_id: string | null
  fork_offset: string | null
  fork_sub_offset: string
  fork_depth: number
  edge_active: boolean
  ref_count: number
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
export interface ForkOptions extends Omit<CreateOptions, 'contentType'> {
  readonly contentType?: string
  readonly sourcePath: string
  readonly offset?: string
  readonly subOffset: number
}

export interface AppendResult {
  readonly stream: StreamMetadata
  readonly status: 200 | 204
  readonly producer?: { epoch: number; seq: number }
}
export interface ReadResult {
  readonly stream: StreamMetadata
  readonly body: Buffer
  readonly next: string
  readonly upToDate: boolean
  readonly hasData: boolean
}

/** Internal hot-log storage. Sealing/forks/subscriptions are later milestones. */
export class PostgresStreamStore {
  readonly schema: string
  private ready: Promise<void> | undefined
  private readonly chunks: ChunkManager | undefined

  constructor(
    private readonly pool: Pool,
    schema: string,
    private readonly maxStoredBytes = 1024 * 1024,
    private readonly maxPageBytes = 2 * 1024 * 1024,
    storage?: StorageOptions,
  ) {
    this.schema = validateSchema(schema)
    this.chunks = storage ? new ChunkManager(pool, schema, storage) : undefined
    if (!Number.isSafeInteger(maxStoredBytes) || maxStoredBytes < 1 || maxStoredBytes > 16 * 1024 * 1024
      || !Number.isSafeInteger(maxPageBytes) || maxPageBytes < maxStoredBytes + 2 || maxPageBytes > 64 * 1024 * 1024) {
      throw new RangeError('Invalid payload/page limits')
    }
  }

  async assertReady(): Promise<void> {
    this.ready ??= this.pool.query<{ storage_version: number; storage_identity: string | null }>(`SELECT storage_version, storage_identity FROM ${this.schema}.installation`)
      .then(async (result) => {
        if (result.rows.length !== 1 || result.rows[0]!.storage_version !== 1) throw new StorageCompatibilityError('Unsupported storage version')
        const migrations = await this.pool.query<{ id: string }>(`SELECT id FROM ${this.schema}.migrations ORDER BY id`)
        if (JSON.stringify(migrations.rows.map((row) => row.id)) !== JSON.stringify(MIGRATION_IDS)) throw new StorageCompatibilityError('Unsupported database migrations')
        if (result.rows[0]!.storage_identity !== null && !this.chunks) throw new StorageCompatibilityError('This installation requires its configured object storage')
        await this.chunks?.assertBinding()
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
      WHERE s.id = $1 FOR NO KEY UPDATE`, [id])
    return result.rows[0]
  }

  private async change(client: PoolClient, stream: StreamMetadata, before: string, kind: string): Promise<void> {
    await client.query(`INSERT INTO ${this.schema}.stream_changes
      (stream_id, version, before_offset, after_offset, kind) VALUES ($1, $2, $3, $4, $5)`,
    [stream.id, stream.change_version, before, stream.tail_offset, kind])
  }

  private async retire(client: PoolClient, stream: StreamMetadata, kind: 'deleted' | 'expired'): Promise<void> {
    const result = await client.query<StreamMetadata>(`UPDATE ${this.schema}.streams
      SET deleted_at = clock_timestamp(), change_version = change_version + 1, cleanup_pending = ref_count > 0 WHERE id = $1 RETURNING *`, [stream.id])
    if (stream.ttl_seconds !== null || stream.expires_at !== null) await client.query(`DELETE FROM ${this.schema}.stream_expirations WHERE stream_id = $1`, [stream.id])
    if (!stream.ref_count) await this.detach(client, result.rows[0]!)
    await this.change(client, result.rows[0]!, stream.tail_offset, kind)
  }

  /** Own path/stream are locked. Ancestor updates only follow the acyclic edge;
   * ancestor path cleanup is a separate transaction, never a reverse path lock. */
  private async detach(client: PoolClient, stream: StreamMetadata): Promise<void> {
    await client.query(`UPDATE ${this.schema}.paths SET active_stream_id = NULL WHERE tenant_id = $1 AND path = $2 AND active_stream_id = $3`, [stream.tenant_id, stream.path, stream.id])
    await client.query(`UPDATE ${this.schema}.streams SET cleanup_pending = false, edge_active = false WHERE id = $1`, [stream.id])
    if (stream.source_id && stream.edge_active) {
      const parent = await client.query(`UPDATE ${this.schema}.streams SET ref_count = ref_count - 1 WHERE id = $1 AND ref_count > 0 RETURNING id`, [stream.source_id])
      if (!parent.rowCount) throw new Error('Fork reference invariant failed')
    }
  }

  private async scheduleExpiry(client: PoolClient, stream: StreamMetadata): Promise<void> {
    if (stream.ttl_seconds === null && stream.expires_at === null) return
    await client.query(`INSERT INTO ${this.schema}.stream_expirations (stream_id, due_at)
      SELECT id, COALESCE(expires_at, accessed_at + ttl_seconds * interval '1 second') FROM ${this.schema}.streams WHERE id = $1
      ON CONFLICT (stream_id) DO UPDATE SET due_at = EXCLUDED.due_at`, [stream.id])
  }

  private async withStream<T>(tenant: string, path: string, work: (client: PoolClient, stream: LockedStream) => Promise<T>): Promise<T> {
    await this.assertReady()
    const outcome = await transaction(this.pool, {}, async (client) => {
      const stream = await this.lock(client, tenant, path)
      if (!stream) return { found: false, status: 404 } as const
      if (stream.deleted_at) return { found: false, status: 410 } as const
      if (stream.expired) {
        await this.retire(client, stream, 'expired')
        return { found: false, status: stream.ref_count ? 410 : 404 } as const
      }
      return { found: true, value: await work(client, stream) } as const
    })
    // Missing/expired is reported after committing an expiry transition.
    if (!outcome.found) throw new StreamError(outcome.status, outcome.status === 410 ? 'STREAM_GONE' : 'STREAM_NOT_FOUND', 'Stream is unavailable')
    return outcome.value
  }

  async create(tenant: string, path: string, options: CreateOptions): Promise<{ stream: StreamMetadata; created: boolean }> {
    await this.assertReady()
    const payload = preparePayload(options.body, options.contentType, true)
    this.checkPayload(payload.bytes)
    const outcome = await transaction(this.pool, {}, async (client) => {
      await client.query(`INSERT INTO ${this.schema}.paths (tenant_id, path) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [tenant, path])
      await client.query(`SELECT path FROM ${this.schema}.paths WHERE tenant_id = $1 AND path = $2 FOR UPDATE`, [tenant, path])
      let existing = await this.lock(client, tenant, path)
      if (existing?.deleted_at) throw new StreamError(409, 'STREAM_RETAINED', 'Stream has active forks')
      if (existing?.expired) {
        await this.retire(client, existing, 'expired')
        if (existing.ref_count) return { error: new StreamError(409, 'STREAM_RETAINED', 'Stream has active forks') } as const
        existing = undefined
      }
      if (existing) {
        if (existing.source_id !== null || mediaType(existing.content_type) !== mediaType(options.contentType) || existing.closed !== options.closed
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
      await this.scheduleExpiry(client, stream)
      await this.chunks?.freeze(client, id)
      return { stream, created: true }
    })
    if ('error' in outcome) throw outcome.error
    return outcome
  }

  private checkPayload(bytes: Uint8Array): void {
    if (bytes.byteLength > this.maxStoredBytes) throw new StreamError(413, 'PAYLOAD_TOO_LARGE', 'Stored append exceeds configured limit')
  }

  /** Each ancestor contributes only its fixed inherited range. No path lookup
   * or current source lifecycle state can change an existing fork's view. */
  private lineage(): string {
    return `WITH RECURSIVE lineage AS (
      SELECT id, tail_offset AS cap, source_id, fork_offset, 0 AS depth FROM ${this.schema}.streams WHERE id = $1
      UNION ALL
      SELECT p.id, LEAST(l.cap, l.fork_offset), p.source_id, p.fork_offset, l.depth + 1
      FROM lineage l JOIN ${this.schema}.streams p ON p.id = l.source_id WHERE l.depth < 128
    )`
  }

  private async boundary(client: PoolClient, stream: StreamMetadata, offset: string): Promise<void> {
    if (offset > stream.tail_offset) throw new StreamError(400, 'OFFSET_BEYOND_TAIL', 'Offset exceeds stream tail')
    if (offset === ZERO_OFFSET || offset === stream.tail_offset) return
    const result = await client.query(`${this.lineage()}
      SELECT 1 FROM lineage l JOIN ${this.schema}.appends a ON a.stream_id = l.id
      WHERE a.end_offset = $2 AND a.end_offset <= l.cap LIMIT 1`, [stream.id, offset])
    if (!result.rowCount) throw new StreamError(400, 'OFFSET_NOT_BOUNDARY', 'Offset is not a returned boundary')
  }

  private async framePlan(client: PoolClient, id: string, start: string, limit = 16, bytes = this.maxPageBytes - 2): Promise<readonly StoredFrame[]> {
    const result = await client.query<StoredFrame>(`${this.lineage()}, candidates AS (
        SELECT f.* FROM lineage l CROSS JOIN LATERAL (
          SELECT stream_id, end_offset, payload_bytes FROM ${this.schema}.appends
          WHERE stream_id = l.id AND end_offset > $2 AND end_offset <= l.cap ORDER BY end_offset LIMIT $4
        ) f ORDER BY end_offset LIMIT $4
      ), bounded AS (SELECT stream_id, end_offset, sum(payload_bytes) OVER (ORDER BY end_offset) AS bytes FROM candidates)
      SELECT a.stream_id, a.start_offset, a.end_offset, a.payload, a.payload_bytes, a.message_ends, a.chunk_id, c.object_key, c.sha256, c.encoded_bytes
      FROM bounded b JOIN ${this.schema}.appends a ON a.stream_id = b.stream_id AND a.end_offset = b.end_offset
      LEFT JOIN ${this.schema}.chunks c ON c.id = a.chunk_id WHERE b.bytes <= $3 ORDER BY a.end_offset`, [id, start, bytes, limit])
    return result.rows
  }

  private async materialize(frames: readonly StoredFrame[]): Promise<readonly ChunkFrame[]> {
    if (this.chunks) return this.chunks.materialize(frames)
    return frames.map((frame) => {
      if (!frame.payload) throw new StorageCompatibilityError('Sealed replay requires object storage')
      return { start: frame.start_offset, end: frame.end_offset, payload: frame.payload, messageEnds: frame.message_ends }
    })
  }

  async createFork(tenant: string, path: string, options: ForkOptions): Promise<{ stream: StreamMetadata; created: boolean }> {
    await this.assertReady()
    if (options.offset !== undefined) parseOffset(options.offset)
    if (!Number.isSafeInteger(options.subOffset) || options.subOffset < 0) throw new StreamError(400, 'INVALID_SUB_OFFSET', 'Invalid sub-offset')
    type Prefix = { sourceId: string; anchor: string; frame: ChunkFrame }
    type Outcome = { stream: StreamMetadata; created: boolean } | { error: StreamError }
      | { prepare: { sourceId: string; anchor: string; contentType: string; frames: readonly StoredFrame[] } }
    const execute = (prefix?: Prefix): Promise<Outcome> => transaction(this.pool, {}, async (client) => {
      // Lock every namespace claim before locking any stream row. Inserts obey
      // the same byte ordering, including when both paths are initially absent.
      const paths = [...new Set([path, options.sourcePath])].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
      for (const claim of paths) await client.query(`INSERT INTO ${this.schema}.paths (tenant_id, path) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [tenant, claim])
      await client.query(`SELECT path FROM ${this.schema}.paths WHERE tenant_id = $1 AND path = ANY($2::text[]) ORDER BY path FOR UPDATE`, [tenant, paths])
      let existing = await this.lock(client, tenant, path)
      if (existing?.deleted_at) throw new StreamError(409, 'STREAM_RETAINED', 'Target stream has active forks')
      if (existing?.expired) {
        await this.retire(client, existing, 'expired')
        if (existing.ref_count) return { error: new StreamError(409, 'STREAM_RETAINED', 'Target stream has active forks') }
        existing = undefined
      }
      const source = await this.lock(client, tenant, options.sourcePath)
      if (existing) {
        const ttl = options.ttlSeconds ?? (options.expiresAt ? null : existing.ttl_seconds)
        const expiry = options.expiresAt ?? (options.ttlSeconds !== undefined ? null : existing.expires_at)
        if (!source || existing.source_id !== source.id || existing.closed !== options.closed
          || mediaType(existing.content_type) !== mediaType(options.contentType ?? existing.content_type)
          || existing.ttl_seconds !== ttl || (existing.expires_at?.toISOString() ?? null) !== (expiry?.toISOString() ?? null)
          || (options.offset !== undefined && existing.fork_offset !== options.offset) || BigInt(existing.fork_sub_offset) !== BigInt(options.subOffset)) {
          throw new StreamError(409, 'CONFIGURATION_MISMATCH', 'Stream configuration differs')
        }
        return { stream: existing, created: false }
      }
      if (!source) throw new StreamError(404, 'SOURCE_NOT_FOUND', 'Source stream not found')
      if (source.deleted_at) throw new StreamError(409, 'SOURCE_RETAINED', 'Source stream is deleted')
      if (source.expired) {
        await this.retire(client, source, 'expired')
        return { error: new StreamError(source.ref_count ? 409 : 404, 'SOURCE_EXPIRED', 'Source stream expired') }
      }
      if (path === options.sourcePath) throw new StreamError(409, 'SELF_FORK', 'A stream cannot fork itself')
      if (source.fork_depth >= 128) throw new StreamError(413, 'FORK_DEPTH', 'Maximum fork depth reached')
      if (options.contentType && mediaType(options.contentType) !== mediaType(source.content_type)) throw new StreamError(409, 'CONTENT_TYPE_MISMATCH', 'Fork content type must match its source')
      const anchor = options.offset ?? source.tail_offset
      await this.boundary(client, source, anchor)
      const payload = preparePayload(options.body, source.content_type, true)
      this.checkPayload(payload.bytes)
      if (prefix && (prefix.sourceId !== source.id || prefix.anchor !== anchor)) throw new StreamError(409, 'SOURCE_CHANGED', 'Source generation changed; retry fork creation')
      if (options.subOffset && !prefix) {
        const frames = await this.framePlan(client, source.id, anchor, 1, this.maxStoredBytes)
        if (!frames[0] || frames[0].start_offset !== anchor) throw new StreamError(400, 'SUB_OFFSET_BEYOND_TAIL', 'Sub-offset has no following source frame')
        return { prepare: { sourceId: source.id, anchor, contentType: source.content_type, frames } }
      }
      const ttl = options.ttlSeconds ?? (options.expiresAt ? null : source.ttl_seconds)
      const expiry = options.expiresAt ?? (options.ttlSeconds !== undefined ? null : source.expires_at)
      const id = randomUUID()
      await client.query(`INSERT INTO ${this.schema}.streams (id, tenant_id, path, content_type, tail_offset, change_version, closed, ttl_seconds, expires_at, source_id, fork_offset, fork_sub_offset, fork_depth, edge_active)
        VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $5, $10, $11, true)`,
      [id, tenant, path, source.content_type, anchor, options.closed, ttl, expiry, source.id, options.subOffset, source.fork_depth + 1])
      let next = anchor
      for (const frame of [...(prefix ? [prefix.frame] : []), ...(payload.bytes.length ? [{ payload: payload.bytes, messageEnds: payload.messageEnds }] : [])]) {
        const end = advanceOffset(next, frame.payload.length)
        await this.insertAppend(client, id, next, end, frame.payload, frame.messageEnds)
        next = end
      }
      const updated = await client.query<StreamMetadata>(`UPDATE ${this.schema}.streams SET tail_offset = $2 WHERE id = $1 RETURNING *`, [id, next])
      await client.query(`UPDATE ${this.schema}.paths SET active_stream_id = $3 WHERE tenant_id = $1 AND path = $2`, [tenant, path, id])
      await client.query(`UPDATE ${this.schema}.streams SET ref_count = ref_count + 1 WHERE id = $1`, [source.id])
      const stream = updated.rows[0]!
      await this.change(client, stream, ZERO_OFFSET, 'created')
      await this.scheduleExpiry(client, stream)
      await this.chunks?.freeze(client, id)
      return { stream, created: true }
    })
    let outcome = await execute()
    if ('prepare' in outcome) {
      const plan = outcome.prepare
      const frame = (await this.materialize(plan.frames))[0]!
      const json = mediaType(plan.contentType) === 'application/json'
      const units = json ? frame.messageEnds?.length ?? 0 : frame.payload.length
      if (options.subOffset > units) throw new StreamError(400, 'SUB_OFFSET_OVERSHOOT', 'Sub-offset exceeds the next source frame')
      const length = json ? frame.messageEnds![options.subOffset - 1]! : options.subOffset
      const prefix = { ...frame, payload: Buffer.from(frame.payload.subarray(0, length)), messageEnds: json ? frame.messageEnds!.slice(0, options.subOffset) : null }
      outcome = await execute({ sourceId: plan.sourceId, anchor: plan.anchor, frame: prefix })
    }
    if ('error' in outcome) throw outcome.error
    if ('prepare' in outcome) throw new Error('Fork prefix was not resolved')
    return outcome
  }

  private async insertAppend(client: PoolClient, id: string, start: string, end: string, bytes: Uint8Array, ends: readonly number[] | null): Promise<void> {
    await client.query(`INSERT INTO ${this.schema}.appends (stream_id, start_offset, end_offset, payload, message_ends, payload_bytes)
      VALUES ($1, $2, $3, $4, $5, $6)`, [id, start, end, bytes, ends, bytes.byteLength])
    await client.query(`UPDATE ${this.schema}.streams SET hot_bytes = hot_bytes + $2, hot_frames = hot_frames + 1,
      seal_after = CASE WHEN hot_frames = 0 THEN clock_timestamp() ELSE seal_after END WHERE id = $1`, [id, bytes.byteLength])
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
      if (value.ttl_seconds !== null) await this.scheduleExpiry(client, value)
      await this.chunks?.freeze(client, stream.id)
      return { stream: value, status: producer && payload.bytes.length ? 200 : 204, ...(producer ? { producer: { epoch: producer.epoch, seq: producer.seq } } : {}) }
    })
  }

  async head(tenant: string, path: string): Promise<StreamMetadata> {
    return this.withStream(tenant, path, async (_client, stream) => stream)
  }

  /** Only the subscription coordinator calls this inside its own transaction.
   * Wake publication and the subscription's generation commit atomically. */
  async appendWake(client: PoolClient, tenant: string, path: string, events: readonly Record<string, unknown>[]): Promise<void> {
    const stream = await this.lock(client, tenant, path)
    if (!stream || stream.deleted_at || stream.expired || stream.closed || mediaType(stream.content_type) !== 'application/json') throw new StreamError(409, 'WAKE_STREAM_UNAVAILABLE', 'Create an open JSON wake stream before issuing wakes')
    let next = stream.tail_offset
    let parts: Buffer[] = [], ends: number[] = [], bytes = 0
    const flush = async () => {
      if (!bytes) return
      const end = advanceOffset(next, bytes)
      await this.insertAppend(client, stream.id, next, end, Buffer.concat(parts, bytes), ends)
      next = end
      parts = []; ends = []; bytes = 0
    }
    for (const event of events) {
      const fragment = Buffer.from(`${JSON.stringify(event)},`)
      this.checkPayload(fragment)
      if (bytes + fragment.length > this.maxStoredBytes) await flush()
      parts.push(fragment); bytes += fragment.length; ends.push(bytes)
    }
    await flush()
    if (next === stream.tail_offset) return
    const result = await client.query<StreamMetadata>(`UPDATE ${this.schema}.streams SET tail_offset = $2, change_version = change_version + 1,
      accessed_at = clock_timestamp() WHERE id = $1 RETURNING *`, [stream.id, next])
    await this.change(client, result.rows[0]!, stream.tail_offset, 'appended')
    if (stream.ttl_seconds !== null) await this.scheduleExpiry(client, stream)
    await this.chunks?.freeze(client, stream.id)
  }

  async validateAckBoundaries(client: PoolClient, values: readonly { stream_id: string; offset: string }[]): Promise<void> {
    if (!values.length) return
    const invalid = await client.query(`${this.lineageForAcks()} SELECT r.stream_id FROM roots r WHERE r.offset <> $2 AND NOT EXISTS (
      SELECT 1 FROM lineage l JOIN ${this.schema}.appends a ON a.stream_id = l.id WHERE l.root = r.stream_id AND a.end_offset = r.offset AND a.end_offset <= l.cap
    ) LIMIT 1`, [JSON.stringify(values), ZERO_OFFSET])
    if (invalid.rowCount) throw new StreamError(409, 'INVALID_OFFSET', 'Ack offset is not a returned boundary')
  }

  private lineageForAcks(): string {
    return `WITH RECURSIVE roots AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(stream_id uuid, "offset" text)), lineage AS (
      SELECT r.stream_id AS root, s.id, s.source_id, s.fork_offset, s.tail_offset AS cap, 0 AS depth FROM roots r JOIN ${this.schema}.streams s ON s.id = r.stream_id
      UNION ALL SELECT l.root, p.id, p.source_id, p.fork_offset, LEAST(l.cap, l.fork_offset), l.depth + 1
      FROM lineage l JOIN ${this.schema}.streams p ON p.id = l.source_id WHERE l.depth < 128
    )`
  }

  async read(tenant: string, path: string, offset: string): Promise<ReadResult> {
    if (!['-1', 'now'].includes(offset)) parseOffset(offset)
    const accessed = await this.withStream(tenant, path, async (client, stream) => {
      if (stream.ttl_seconds !== null) {
        await client.query(`UPDATE ${this.schema}.streams SET accessed_at = clock_timestamp() WHERE id = $1`, [stream.id])
        await this.scheduleExpiry(client, stream)
      }
      return stream
    })
    return this.readGeneration(tenant, accessed.id, offset)
  }

  /** An active live read must never switch to a recreated path generation. */
  async readGeneration(tenant: string, id: string, offset: string): Promise<ReadResult> {
    if (!['-1', 'now'].includes(offset)) parseOffset(offset)
    const plan = await transaction(this.pool, { readOnly: true }, async (client) => {
      const metadata = await client.query<LockedStream>(`SELECT s.*, COALESCE(expires_at <= clock_timestamp()
        OR accessed_at + ttl_seconds * interval '1 second' <= clock_timestamp(), false) AS expired
        FROM ${this.schema}.streams s WHERE tenant_id = $1 AND id = $2`, [tenant, id])
      const stream = metadata.rows[0]
      if (!stream) throw new StreamError(404, 'STREAM_NOT_FOUND', 'Stream no longer exists')
      if (stream.deleted_at || stream.expired) throw new StreamError(stream.ref_count ? 410 : 404, 'STREAM_UNAVAILABLE', 'Stream is unavailable')
      const start = offset === '-1' ? ZERO_OFFSET : offset === 'now' ? stream.tail_offset : offset
      await this.boundary(client, stream, start)
      const frames = await this.framePlan(client, stream.id, start)
      const next = frames.at(-1)?.end_offset ?? start
      return { stream, frames, next, upToDate: next === stream.tail_offset, hasData: frames.length > 0 }
    })
    const frames = await this.materialize(plan.frames)
    return { stream: plan.stream, body: formatPayloads(frames.map((frame) => frame.payload), plan.stream.content_type), next: plan.next, upToDate: plan.upToDate, hasData: plan.hasData }
  }

  async maintain(options: { readonly maxChunks?: number } = {}): Promise<{ frozen: number; sealed: number; retired: number }> {
    await this.assertReady()
    const maximum = options.maxChunks ?? 8
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 128) throw new RangeError('Invalid maintenance batch')
    let retired = 0
    for (let i = 0; i < maximum; i++) {
      const cleaned = await transaction(this.pool, {}, async (client) => {
        const candidate = await client.query<{ tenant_id: string; path: string }>(`WITH candidates AS (
          (SELECT s.id, s.tenant_id, s.path, e.due_at FROM ${this.schema}.stream_expirations e
            JOIN ${this.schema}.streams s ON s.id = e.stream_id WHERE e.due_at <= clock_timestamp() ORDER BY e.due_at, e.stream_id LIMIT 128)
          UNION ALL
          (SELECT id, tenant_id, path, deleted_at AS due_at FROM ${this.schema}.streams WHERE cleanup_pending AND ref_count = 0 ORDER BY deleted_at, id LIMIT 128)
        ) SELECT p.tenant_id, p.path FROM candidates c JOIN ${this.schema}.paths p
          ON p.tenant_id = c.tenant_id AND p.path = c.path AND p.active_stream_id = c.id
          ORDER BY c.due_at, c.id LIMIT 1 FOR UPDATE OF p SKIP LOCKED`)
        if (!candidate.rows[0]) return false
        const row = candidate.rows[0]
        const stream = await this.lock(client, row.tenant_id, row.path)
        if (stream?.deleted_at && !stream.ref_count) { await this.detach(client, stream); return true }
        if (stream?.expired && !stream.deleted_at) { await this.retire(client, stream, 'expired'); return true }
        return false
      })
      if (!cleaned) break
      retired++
    }
    const chunks = this.chunks ? await this.chunks.maintain(options) : { frozen: 0, sealed: 0 }
    return { ...chunks, retired }
  }

  /** One bounded query per watched generation, shared by all local readers. */
  async headVersion(tenant: string, id: string): Promise<string | null> {
    const result = await this.pool.query<{ change_version: string }>(`SELECT change_version FROM ${this.schema}.streams WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL
      AND (expires_at IS NULL OR expires_at > clock_timestamp())
      AND (ttl_seconds IS NULL OR accessed_at + ttl_seconds * interval '1 second' > clock_timestamp())`, [tenant, id])
    return result.rows[0]?.change_version ?? null
  }

  async delete(tenant: string, path: string): Promise<void> {
    await this.withStream(tenant, path, (client, stream) => this.retire(client, stream, 'deleted'))
  }
}
