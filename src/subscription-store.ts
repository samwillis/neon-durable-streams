import { createHash, randomBytes, randomInt, randomUUID, timingSafeEqual } from 'node:crypto'
import type { Pool, PoolClient } from 'pg'
import { StorageCompatibilityError, StreamError } from './errors.js'
import { parseOffset, ZERO_OFFSET } from './offset.js'
import { validateSchema } from './path.js'
import type { AuthorizationContext, Operation, Principal } from './server.js'
import type { PostgresStreamStore } from './store.js'
import { SubscriptionCipher, SubscriptionSigner } from './subscription-crypto.js'
import { globMatches, normalizeSubscription, relativeStreamPath } from './subscription-protocol.js'
import type { ControlRoute, SubscriptionConfig } from './subscription-protocol.js'
import type { SubscriptionOptions } from './subscriptions.js'
import { deliverWebhook, webhookTarget } from './webhook.js'

interface Sub {
  id: string; tenant_id: string; name: string; config: SubscriptionConfig; config_hash: string; pattern_regex: string | null
  public_base_url: string; generation: string; current_wake: string | null; holder: string | null; lease_until: Date | null
  retry_count: number; retry_at: Date | null; next_work_at: Date | null; member_count: number; blocked_members: number
  created_at: Date; deleted_at: Date | null; lease_valid?: boolean
}
interface Member {
  path: string; stream_id: string | null; explicit_link: boolean; glob_link: boolean; acked_offset: string; tail_offset: string
}
interface Snapshot { path: string; stream_id: string | null; link_type: 'explicit' | 'glob'; acked_offset: string; tail_offset: string }
interface Wake {
  id: string; subscription_id: string; tenant_id: string; generation: string; kind: 'webhook' | 'pull-wake'
  token_hash: Buffer | null; token_cipher: Buffer | null; state: string; delivery_claim: string | null; delivery_until: Date | null; pull_published: boolean; created_at: Date
}
interface WebhookJob { sub: Sub; wake: Wake; streams: readonly Snapshot[]; claim: string }
interface Change { stream_id: string; version: string; before_offset: string; after_offset: string; kind: string; fanout_after: string | null; tenant_id: string; path: string; active_stream_id: string | null }
const fenced = () => new StreamError(409, 'FENCED', 'Wake generation is stale')
const jsonBody = (bytes: Buffer): Record<string, unknown> => {
  try { const value: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(); return value as Record<string, unknown> }
  catch { throw new StreamError(400, 'INVALID_JSON', 'Expected a JSON object') }
}
const hashToken = (token: string) => createHash('sha256').update(token).digest()

export class PostgresSubscriptions {
  private readonly schema: string
  private readonly cipher: SubscriptionCipher
  private readonly signer: SubscriptionSigner
  private readonly maxStreams: number
  private readonly maxSubscriptions: number
  private readonly webhookTimeout: number
  private installationId?: string
  private ready: Promise<void> | undefined
  private readonly shutdown = new AbortController()
  constructor(private readonly pool: Pool, schema: string, private readonly streams: PostgresStreamStore,
    private readonly options: SubscriptionOptions, private readonly report: (error: unknown) => void) {
    this.schema = validateSchema(schema)
    this.cipher = new SubscriptionCipher(options.keyring)
    this.signer = new SubscriptionSigner(pool, schema, options.keyring)
    this.maxStreams = options.maxStreams ?? 1000
    this.maxSubscriptions = options.maxSubscriptionsPerTenant ?? 10_000
    this.webhookTimeout = options.webhookTimeoutMs ?? 5000
    for (const [n, min, max] of [[this.maxStreams, 1, 10_000], [this.maxSubscriptions, 1, 100_000], [this.webhookTimeout, 100, 60_000]]) {
      if (!Number.isSafeInteger(n) || n! < min! || n! > max!) throw new RangeError('Invalid subscription limits')
    }
  }
  async assertReady(): Promise<void> {
    this.ready ??= (async () => {
      await this.streams.assertReady()
      const result = await this.pool.query<{ id: string }>(`SELECT id FROM ${this.schema}.installation`)
      if (result.rows.length !== 1) throw new StorageCompatibilityError('Missing installation identity')
      this.installationId = result.rows[0]!.id
      await this.signer.active()
      await this.signer.signature(Buffer.alloc(0))
    })().catch((error: unknown) => { this.ready = undefined; throw error })
    await this.ready
  }
  private registry(client: PoolClient, tenant: string, exclusive: boolean) {
    return client.query(`SELECT pg_advisory_xact_lock${exclusive ? '' : '_shared'}(hashtextextended($1, 0))`, [`durable-streams:subscription-registry:${this.schema}:${tenant}`])
  }
  private async find(client: PoolClient, tenant: string, name: string): Promise<Sub> {
    const result = await client.query<Sub>(`WITH locked AS MATERIALIZED (SELECT * FROM ${this.schema}.subscriptions WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL FOR UPDATE)
      SELECT *, lease_until > clock_timestamp() AS lease_valid FROM locked`, [tenant, name])
    if (!result.rows[0]) throw new StreamError(404, 'SUBSCRIPTION_NOT_FOUND', 'Subscription not found')
    return result.rows[0]
  }
  private async infos(client: PoolClient, sub: Sub): Promise<readonly Member[]> {
    const result = await client.query<Member>(`SELECT m.*, CASE WHEN p.active_stream_id = s.id AND s.deleted_at IS NULL
      AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp()) AND (s.ttl_seconds IS NULL OR s.accessed_at + s.ttl_seconds * interval '1 second' > clock_timestamp())
      THEN s.tail_offset ELSE m.acked_offset END AS tail_offset FROM ${this.schema}.subscription_members m
      LEFT JOIN ${this.schema}.streams s ON s.id = m.stream_id LEFT JOIN ${this.schema}.paths p ON p.tenant_id = m.tenant_id AND p.path = m.path
      WHERE m.subscription_id = $1 ORDER BY m.path LIMIT $2`, [sub.id, this.maxStreams + 1])
    if (result.rows.length > this.maxStreams) throw new StreamError(503, 'SUBSCRIPTION_CAPACITY', 'Configured stream capacity is below existing subscription membership')
    return result.rows
  }
  private snapshot(members: readonly Member[]): readonly Snapshot[] {
    return members.map((member) => ({ path: member.path, stream_id: member.stream_id, link_type: member.explicit_link ? 'explicit' : 'glob', acked_offset: member.acked_offset, tail_offset: member.tail_offset }))
  }
  private async pending(client: PoolClient, sub: Sub): Promise<boolean> {
    return (await this.infos(client, sub)).some((member) => member.tail_offset > member.acked_offset)
  }
  private async invalidate(client: PoolClient, sub: Sub, state: 'completed' | 'expired' | 'fenced'): Promise<void> {
    if (sub.current_wake) await client.query(`UPDATE ${this.schema}.subscription_wakes SET state = $2, delivery_claim = NULL, delivery_until = NULL WHERE id = $1`, [sub.current_wake, state])
    await client.query(`UPDATE ${this.schema}.subscriptions SET current_wake = NULL, holder = NULL, lease_until = NULL WHERE id = $1`, [sub.id])
    sub.current_wake = null; sub.holder = null; sub.lease_until = null; sub.lease_valid = false
  }
  private async queue(client: PoolClient, sub: Sub): Promise<boolean> {
    const pending = await this.pending(client, sub)
    await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = CASE WHEN $2 THEN COALESCE(retry_at, clock_timestamp()) ELSE NULL END WHERE id = $1`, [sub.id, pending])
    return pending
  }
  private async insertMember(client: PoolClient, sub: Sub, path: string, streamId: string | null, offset: string, explicit: boolean, glob: boolean): Promise<void> {
    const queued = streamId ? (await client.query<{ before_offset: string }>(`SELECT before_offset FROM ${this.schema}.subscription_pending_links WHERE subscription_id = $1 AND stream_id = $2`, [sub.id, streamId])).rows[0] : undefined
    const baseline = queued && queued.before_offset < offset ? queued.before_offset : offset
    await client.query(`INSERT INTO ${this.schema}.subscription_members (subscription_id, tenant_id, path, stream_id, explicit_link, glob_link, acked_offset)
      VALUES ($1,$2,$3,$4,$5,$6,$7)`, [sub.id, sub.tenant_id, path, streamId, explicit, glob, baseline])
    await client.query(`UPDATE ${this.schema}.subscriptions SET member_count = member_count + 1 WHERE id = $1`, [sub.id])
    sub.member_count++
    if (queued) {
      await client.query(`DELETE FROM ${this.schema}.subscription_pending_links WHERE subscription_id = $1 AND stream_id = $2`, [sub.id, streamId])
      await client.query(`UPDATE ${this.schema}.subscriptions SET blocked_members = blocked_members - 1 WHERE id = $1`, [sub.id])
    }
  }
  private async activePath(client: PoolClient, tenant: string, path: string): Promise<{ id: string; tail_offset: string } | undefined> {
    const result = await client.query<{ id: string; tail_offset: string }>(`SELECT s.id, s.tail_offset FROM ${this.schema}.paths p JOIN ${this.schema}.streams s ON s.id = p.active_stream_id
      WHERE p.tenant_id = $1 AND p.path = $2 AND s.deleted_at IS NULL AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
      AND (s.ttl_seconds IS NULL OR s.accessed_at + s.ttl_seconds * interval '1 second' > clock_timestamp())`, [tenant, path])
    return result.rows[0]
  }
  async jwks(): Promise<Response> {
    await this.assertReady()
    await this.streams.guard.check()
    return Response.json(await this.signer.jwks(), { headers: { 'Content-Type': 'application/jwk-set+json', 'Cache-Control': 'public, max-age=300' } })
  }
  async handle(request: Request, route: ControlRoute, principal: Principal, authorize: (context: AuthorizationContext) => boolean | Promise<boolean>, readBody: () => Promise<Buffer>, publicBase: string): Promise<Response> {
    const allowed = async (operation: Operation, selector?: { pattern?: string; streams: readonly string[] }, subscription?: SubscriptionConfig) => {
      if (!await authorize({ principal, operation, path: route.path, request, ...(selector ? { selector } : {}), ...(subscription ? { subscription } : {}) })) throw new StreamError(403, 'FORBIDDEN', 'Subscription operation is not authorized')
    }
    if (route.action === 'subscription' && request.method === 'PUT') {
      const normalized = normalizeSubscription(jsonBody(await readBody()))
      const { config } = normalized
      if (config.streams.length > this.maxStreams) throw new StreamError(413, 'SUBSCRIPTION_CAPACITY', 'Too many explicit subscription streams')
      await allowed('subscribe', { ...(config.pattern ? { pattern: config.pattern } : {}), streams: config.streams }, config)
      if (config.wake_stream && !await authorize({ principal, operation: 'append', path: config.wake_stream, request })) throw new StreamError(403, 'FORBIDDEN', 'Wake-stream writes are not authorized')
      if (config.webhook) await webhookTarget(config.webhook.url, this.options.allowLocalhostWebhooks ?? false)
      await this.assertReady()
      const created = await this.streams.transaction({}, async (client) => {
        await this.registry(client, principal.tenantId, true)
        const existing = await client.query<Sub>(`SELECT * FROM ${this.schema}.subscriptions WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL FOR UPDATE`, [principal.tenantId, route.name])
        if (existing.rows[0]) {
          if (existing.rows[0].config_hash !== normalized.hash) throw new StreamError(409, 'CONFIGURATION_MISMATCH', 'Subscription configuration differs')
          return false
        }
        const count = await client.query<{ count: string; generation: string }>(`SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS count, COALESCE(max(generation) FILTER (WHERE name = $2),0) AS generation
          FROM ${this.schema}.subscriptions WHERE tenant_id = $1`, [principal.tenantId, route.name])
        if (Number(count.rows[0]!.count) >= this.maxSubscriptions) throw new StreamError(429, 'SUBSCRIPTION_CAPACITY', 'Tenant subscription capacity reached')
        const result = await client.query<Sub>(`INSERT INTO ${this.schema}.subscriptions (id, tenant_id, name, config, config_hash, pattern_regex, public_base_url, generation)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`, [randomUUID(), principal.tenantId, route.name, config, normalized.hash, normalized.regex, publicBase, count.rows[0]!.generation])
        const sub = result.rows[0]!
        const existingStreams = await client.query<{ path: string; id: string; tail_offset: string }>(`SELECT s.path, s.id, s.tail_offset FROM ${this.schema}.streams s JOIN ${this.schema}.paths p ON p.active_stream_id = s.id
          WHERE s.tenant_id = $1 AND s.deleted_at IS NULL AND (s.expires_at IS NULL OR s.expires_at > clock_timestamp())
          AND (s.ttl_seconds IS NULL OR s.accessed_at + s.ttl_seconds * interval '1 second' > clock_timestamp())
          AND (($2::text IS NOT NULL AND s.path ~ $2) OR s.path = ANY($3::text[])) ORDER BY s.path LIMIT $4`, [principal.tenantId, normalized.regex, config.streams, this.maxStreams + 1])
        const paths = new Map(existingStreams.rows.map((row) => [row.path, row]))
        const members = [...new Set([...paths.keys(), ...config.streams])]
        if (members.length > this.maxStreams) throw new StreamError(413, 'SUBSCRIPTION_CAPACITY', 'Subscription matches too many streams')
        // One bounded insert, preserving an at-tail baseline captured while the
        // exclusive registry barrier prevents dispatch from overtaking backfill.
        const rows = members.map((path) => ({ path, stream_id: paths.get(path)?.id ?? null, acked_offset: paths.get(path)?.tail_offset ?? ZERO_OFFSET,
          explicit_link: config.streams.includes(path), glob_link: config.pattern !== null && globMatches(config.pattern, path) }))
        if (rows.length) await client.query(`INSERT INTO ${this.schema}.subscription_members (subscription_id, tenant_id, path, stream_id, acked_offset, explicit_link, glob_link)
          SELECT $1,$2,x.path,x.stream_id,x.acked_offset,x.explicit_link,x.glob_link FROM jsonb_to_recordset($3::jsonb) x(path text,stream_id uuid,acked_offset text,explicit_link boolean,glob_link boolean)`, [sub.id, sub.tenant_id, JSON.stringify(rows)])
        await client.query(`UPDATE ${this.schema}.subscriptions SET member_count = $2 WHERE id = $1`, [sub.id, rows.length])
        return true
      })
      return this.serialize(principal.tenantId, route.name!, created ? 201 : 200)
    }
    if (route.action === 'subscription' && request.method === 'GET') { await allowed('read'); await this.assertReady(); return this.serialize(principal.tenantId, route.name!) }
    if (route.action === 'subscription' && request.method === 'DELETE') {
      await allowed('delete')
      await this.assertReady()
      await this.streams.transaction({}, async (client) => {
        const sub = await this.find(client, principal.tenantId, route.name!)
        await this.invalidate(client, sub, 'fenced')
        await client.query(`DELETE FROM ${this.schema}.subscription_pending_links WHERE subscription_id = $1`, [sub.id])
        await client.query(`DELETE FROM ${this.schema}.subscription_members WHERE subscription_id = $1`, [sub.id])
        await client.query(`UPDATE ${this.schema}.subscriptions SET deleted_at = clock_timestamp(), next_work_at = NULL, blocked_members = 0, member_count = 0 WHERE id = $1`, [sub.id])
      })
      return new Response(null, { status: 204 })
    }
    if ((route.action === 'streams' && request.method === 'POST') || (route.action === 'member' && request.method === 'DELETE')) {
      const body = route.action === 'streams' ? jsonBody(await readBody()) : null
      if (body && !Array.isArray(body.streams)) throw new StreamError(400, 'INVALID_STREAMS', 'Expected a streams array')
      const paths = route.action === 'member' ? [route.member!] : [...new Set((body!.streams as unknown[]).map(relativeStreamPath))]
      if (paths.length > this.maxStreams) throw new StreamError(413, 'SUBSCRIPTION_CAPACITY', 'Too many explicit streams')
      await allowed('subscribe', { streams: paths })
      await this.assertReady()
      await this.membership(principal.tenantId, route.name!, paths, route.action === 'streams')
      return new Response(null, { status: 204 })
    }
    if (route.action === 'claim' && request.method === 'POST') {
      await allowed('claim')
      await this.assertReady()
      const body = jsonBody(await readBody())
      if (typeof body.worker !== 'string' || !body.worker || Buffer.byteLength(body.worker) > 512) throw new StreamError(400, 'INVALID_WORKER', 'Expected a worker name')
      return this.claim(principal.tenantId, route.name!, body.worker)
    }
    throw new StreamError(405, 'METHOD_NOT_ALLOWED', 'Unsupported subscription method or route')
  }
  private async serialize(tenant: string, name: string, status = 200): Promise<Response> {
    const value = await this.streams.transaction({ readOnly: true }, async (client) => {
      const result = await client.query<Sub>(`SELECT * FROM ${this.schema}.subscriptions WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL`, [tenant, name])
      const sub = result.rows[0]
      if (!sub) throw new StreamError(404, 'SUBSCRIPTION_NOT_FOUND', 'Subscription not found')
      return { sub, members: await this.infos(client, sub) }
    })
    const sub = value.sub, config = sub.config
    return Response.json({ id: name, subscription_id: name, type: config.type, pattern: config.pattern,
      streams: value.members.map((member) => ({ path: member.path, link_type: member.explicit_link ? 'explicit' : 'glob', acked_offset: member.acked_offset })),
      webhook: config.webhook ? { url: config.webhook.url, signing: { alg: 'ed25519', kid: await this.signer.active(), jwks_url: `${sub.public_base_url}/__ds/jwks.json` } } : null,
      wake_stream: config.wake_stream, lease_ttl_ms: config.lease_ttl_ms, created_at: sub.created_at.toISOString(),
      status: sub.retry_count || sub.blocked_members ? 'failed' : 'active', description: config.description }, { status })
  }
  private async membership(tenant: string, name: string, paths: readonly string[], add: boolean): Promise<void> {
    await this.streams.transaction({}, async (client) => {
      await this.registry(client, tenant, true)
      const sub = await this.find(client, tenant, name)
      let changed = false
      for (const path of paths) {
        const found = await client.query<Member>(`SELECT * FROM ${this.schema}.subscription_members WHERE subscription_id = $1 AND path = $2`, [sub.id, path])
        const member = found.rows[0]
        if (add) {
          if (member?.explicit_link) continue
          if (member) await client.query(`UPDATE ${this.schema}.subscription_members SET explicit_link = true WHERE subscription_id = $1 AND path = $2`, [sub.id, path])
          else {
            if (sub.member_count >= this.maxStreams) throw new StreamError(413, 'SUBSCRIPTION_CAPACITY', 'Subscription stream capacity reached')
            const stream = await this.activePath(client, tenant, path)
            await this.insertMember(client, sub, path, stream?.id ?? null, stream?.tail_offset ?? ZERO_OFFSET, true, sub.config.pattern !== null && globMatches(sub.config.pattern, path))
          }
        } else {
          if (!member?.explicit_link) continue
          if (member.glob_link) await client.query(`UPDATE ${this.schema}.subscription_members SET explicit_link = false WHERE subscription_id = $1 AND path = $2`, [sub.id, path])
          else { await client.query(`DELETE FROM ${this.schema}.subscription_members WHERE subscription_id = $1 AND path = $2`, [sub.id, path]); await client.query(`UPDATE ${this.schema}.subscriptions SET member_count = member_count - 1 WHERE id = $1`, [sub.id]); sub.member_count-- }
        }
        changed = true
      }
      if (changed) { await this.invalidate(client, sub, 'fenced'); await this.queue(client, sub) }
    })
  }
  private async issue(client: PoolClient, sub: Sub, deliveryClaim?: string): Promise<{ wake: Wake; streams: readonly Snapshot[] }> {
    const members = await this.infos(client, sub)
    const generation = BigInt(sub.generation) + 1n
    if (generation > BigInt(Number.MAX_SAFE_INTEGER)) throw new StreamError(503, 'GENERATION_CAPACITY', 'Subscription generation capacity reached')
    const id = randomUUID()
    const token = sub.config.type === 'webhook' ? `dsw.${id}.${randomBytes(32).toString('base64url')}` : null
    const result = await client.query<Wake>(`INSERT INTO ${this.schema}.subscription_wakes (id, subscription_id, tenant_id, generation, kind, token_hash, token_cipher, state, delivery_claim, delivery_until)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'ready',$8,CASE WHEN $8::uuid IS NOT NULL THEN clock_timestamp() + $9 * interval '1 millisecond' ELSE NULL END) RETURNING *`, [id, sub.id, sub.tenant_id, String(generation), sub.config.type,
      token ? hashToken(token) : null, token ? this.cipher.encrypt(Buffer.from(token), `${this.installationId}:wake:${id}`) : null,
      sub.config.type === 'webhook' ? deliveryClaim ?? null : null, this.webhookTimeout + 10_000])
    const snapshots = this.snapshot(members)
    if (snapshots.length) await client.query(`INSERT INTO ${this.schema}.subscription_wake_streams (wake_id, tenant_id, path, stream_id, link_type, acked_offset, tail_offset)
      SELECT $1,$2,x.path,x.stream_id,x.link_type,x.acked_offset,x.tail_offset FROM jsonb_to_recordset($3::jsonb) x(path text,stream_id uuid,link_type text,acked_offset text,tail_offset text)`, [id, sub.tenant_id, JSON.stringify(snapshots)])
    if (sub.config.type === 'pull-wake') {
      await this.streams.appendWake(client, sub.tenant_id, sub.config.wake_stream!, members.filter((member) => member.tail_offset > member.acked_offset).map((member) => ({
        type: 'wake', subscription_id: sub.name, stream: member.path, generation: Number(generation), ts: result.rows[0]!.created_at.getTime(),
      })))
      await client.query(`UPDATE ${this.schema}.subscription_wakes SET pull_published = true WHERE id = $1`, [id])
    }
    const updated = await client.query<Sub>(`UPDATE ${this.schema}.subscriptions SET generation = $2, current_wake = $3,
      holder = CASE WHEN $4 THEN 'webhook' ELSE NULL END,
      lease_until = CASE WHEN $4 THEN clock_timestamp() + $5 * interval '1 millisecond' ELSE NULL END,
      next_work_at = CASE WHEN $4 THEN clock_timestamp() + LEAST($5,$6) * interval '1 millisecond' ELSE NULL END WHERE id = $1 RETURNING *`, [sub.id, String(generation), id, sub.config.type === 'webhook', sub.config.lease_ttl_ms, this.webhookTimeout + 10_000])
    Object.assign(sub, updated.rows[0])
    return { wake: { ...result.rows[0]!, pull_published: sub.config.type === 'pull-wake' }, streams: snapshots }
  }
  private async claim(tenant: string, name: string, worker: string): Promise<Response> {
    const outcome = await this.streams.transaction({}, async (client) => {
      const sub = await this.find(client, tenant, name)
      if (sub.config.type !== 'pull-wake') throw new StreamError(400, 'INVALID_REQUEST', 'Only pull-wake subscriptions can be claimed')
      if (sub.holder && sub.lease_valid) return { busy: { code: 'ALREADY_CLAIMED', current_holder: sub.holder, generation: Number(sub.generation) } }
      if (sub.holder) await this.invalidate(client, sub, 'expired')
      if (!await this.pending(client, sub)) throw new StreamError(409, 'NO_PENDING_WORK', 'Subscription has no pending work')
      if (!sub.current_wake) await this.issue(client, sub)
      const id = sub.current_wake!
      const token = `dsw.${id}.${randomBytes(32).toString('base64url')}`
      const snapshots = this.snapshot(await this.infos(client, sub))
      await client.query(`DELETE FROM ${this.schema}.subscription_wake_streams WHERE wake_id = $1`, [id])
      if (snapshots.length) await client.query(`INSERT INTO ${this.schema}.subscription_wake_streams (wake_id, tenant_id, path, stream_id, link_type, acked_offset, tail_offset)
        SELECT $1,$2,x.path,x.stream_id,x.link_type,x.acked_offset,x.tail_offset FROM jsonb_to_recordset($3::jsonb) x(path text,stream_id uuid,link_type text,acked_offset text,tail_offset text)`, [id, sub.tenant_id, JSON.stringify(snapshots)])
      await client.query(`UPDATE ${this.schema}.subscription_wakes SET token_hash = $2, token_cipher = $3 WHERE id = $1`, [id, hashToken(token), this.cipher.encrypt(Buffer.from(token), `${this.installationId}:wake:${id}`)])
      await client.query(`UPDATE ${this.schema}.subscriptions SET holder = $2, lease_until = clock_timestamp() + $3 * interval '1 millisecond',
        next_work_at = clock_timestamp() + $3 * interval '1 millisecond' WHERE id = $1`, [sub.id, worker, sub.config.lease_ttl_ms])
      return { claimed: { wake_id: `w_${id}`, generation: Number(sub.generation), token,
        streams: snapshots.map((row) => ({ path: row.path, link_type: row.link_type, acked_offset: row.acked_offset, tail_offset: row.tail_offset, has_pending: row.tail_offset > row.acked_offset })), lease_ttl_ms: sub.config.lease_ttl_ms } }
    })
    return 'busy' in outcome ? Response.json({ error: outcome.busy }, { status: 409 }) : Response.json(outcome.claimed)
  }
  async scoped(request: Request, route: ControlRoute, readBody: () => Promise<Buffer>): Promise<Response> {
    if (request.method !== 'POST') throw new StreamError(405, 'METHOD_NOT_ALLOWED', 'Subscription tokens require POST')
    const authorization = request.headers.get('authorization') ?? ''
    const match = /^Bearer (dsw\.([0-9a-f-]{36})\.[A-Za-z0-9_-]{43})$/i.exec(authorization)
    if (!match || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(match[2]!)) throw new StreamError(401, 'INVALID_TOKEN', 'A subscription-scoped token is required')
    await this.assertReady()
    const token = match[1]!, wakeId = match[2]!
    const authenticated = await this.pool.query<{ token_hash: Buffer | null; subscription_id: string; name: string }>(`SELECT w.token_hash, w.subscription_id, s.name
      FROM ${this.schema}.subscription_wakes w JOIN ${this.schema}.subscriptions s ON s.id = w.subscription_id WHERE w.id = $1`, [wakeId])
    const owner = authenticated.rows[0]
    if (!owner?.token_hash || owner.name !== route.name || !timingSafeEqual(owner.token_hash, hashToken(token))) throw new StreamError(401, 'INVALID_TOKEN', 'Subscription token is invalid')
    const body = jsonBody(await readBody())
    const response = await this.streams.transaction({}, async (client) => {
      const result = await client.query<Sub>(`WITH locked AS MATERIALIZED (SELECT * FROM ${this.schema}.subscriptions WHERE id = $1 FOR UPDATE)
        SELECT *, lease_until > clock_timestamp() AS lease_valid FROM locked`, [owner.subscription_id])
      const sub = result.rows[0]
      const wake = (await client.query<Wake>(`SELECT * FROM ${this.schema}.subscription_wakes WHERE id = $1`, [wakeId])).rows[0]!
      if (!sub || sub.deleted_at || sub.current_wake !== wakeId || !sub.lease_valid || !wake.token_hash || !timingSafeEqual(wake.token_hash, hashToken(token))
        || body.wake_id !== `w_${wakeId}` || body.generation !== Number(sub.generation)
        || (route.action === 'callback' ? sub.config.type !== 'webhook' : sub.config.type !== 'pull-wake')) throw fenced()
      if (route.action !== 'release') await this.applyAcks(client, sub, body.acks)
      if (route.action === 'release' || body.done === true) {
        await this.invalidate(client, sub, 'completed')
        await client.query(`UPDATE ${this.schema}.subscriptions SET retry_count = 0, retry_at = NULL WHERE id = $1`, [sub.id])
        sub.retry_at = null
        return { next: await this.queue(client, sub) }
      }
      await client.query(`UPDATE ${this.schema}.subscriptions SET lease_until = clock_timestamp() + $2 * interval '1 millisecond',
        next_work_at = clock_timestamp() + $2 * interval '1 millisecond' WHERE id = $1`, [sub.id, sub.config.lease_ttl_ms])
      return { next: false }
    })
    return route.action === 'release' ? new Response(null, { status: 204 }) : Response.json({ ok: true, next_wake: response.next })
  }
  private async applyAcks(client: PoolClient, sub: Sub, input: unknown): Promise<void> {
    if (input === undefined) return
    if (!Array.isArray(input) || input.length > this.maxStreams) throw new StreamError(400, 'INVALID_ACKS', 'Invalid ack list')
    const acks = input.map((item: unknown) => {
      if (!item || typeof item !== 'object' || Array.isArray(item)) throw new StreamError(400, 'INVALID_ACKS', 'Invalid ack')
      const raw = item as Record<string, unknown>
      const path = relativeStreamPath(raw.stream ?? raw.path)
      if (typeof raw.offset !== 'string') throw new StreamError(409, 'INVALID_OFFSET', 'Ack requires a returned offset')
      try { parseOffset(raw.offset) } catch { throw new StreamError(409, 'INVALID_OFFSET', 'Ack requires a returned offset') }
      return { path, offset: raw.offset }
    })
    if (new Set(acks.map((ack) => ack.path)).size !== acks.length) throw new StreamError(400, 'INVALID_ACKS', 'A stream may be acked only once per request')
    const members = await client.query<Member>(`SELECT m.*, COALESCE(s.tail_offset,$3) AS tail_offset FROM ${this.schema}.subscription_members m
      LEFT JOIN ${this.schema}.streams s ON s.id = m.stream_id WHERE m.subscription_id = $1 AND m.path = ANY($2::text[])`, [sub.id, acks.map((ack) => ack.path), ZERO_OFFSET])
    const byPath = new Map(members.rows.map((member) => [member.path, member]))
    const boundaries: { stream_id: string; offset: string }[] = []
    for (const ack of acks) {
      const member = byPath.get(ack.path)
      if (!member || ack.offset < member.acked_offset || ack.offset > member.tail_offset) throw new StreamError(409, 'INVALID_OFFSET', 'Ack regresses the cursor, exceeds the tail, or names an unknown stream')
      if (member.stream_id && ack.offset !== member.tail_offset && ack.offset !== ZERO_OFFSET) boundaries.push({ stream_id: member.stream_id, offset: ack.offset })
    }
    await this.streams.validateAckBoundaries(client, boundaries)
    if (acks.length) await client.query(`UPDATE ${this.schema}.subscription_members m SET acked_offset = x.offset FROM jsonb_to_recordset($2::jsonb) x(path text,"offset" text)
      WHERE m.subscription_id = $1 AND m.path = x.path`, [sub.id, JSON.stringify(acks)])
  }
  private async markPending(client: PoolClient, sub: Sub): Promise<void> {
    if (!sub.current_wake) await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = COALESCE(retry_at, clock_timestamp()) WHERE id = $1`, [sub.id])
  }
  private async applyChange(client: PoolClient, sub: Sub, change: Change, active: { id: string; tail_offset: string } | undefined): Promise<void> {
    const found = await client.query<Member>(`SELECT * FROM ${this.schema}.subscription_members WHERE subscription_id = $1 AND path = $2`, [sub.id, change.path])
    let member = found.rows[0]
    if (active?.id !== change.stream_id) {
      if (member?.stream_id === change.stream_id) {
        await this.invalidate(client, sub, 'fenced')
        if (member.explicit_link) await client.query(`UPDATE ${this.schema}.subscription_members SET stream_id = NULL, acked_offset = $3, glob_link = false WHERE subscription_id = $1 AND path = $2`, [sub.id, change.path, ZERO_OFFSET])
        else { await client.query(`DELETE FROM ${this.schema}.subscription_members WHERE subscription_id = $1 AND path = $2`, [sub.id, change.path]); await client.query(`UPDATE ${this.schema}.subscriptions SET member_count = member_count - 1 WHERE id = $1`, [sub.id]); sub.member_count-- }
        await this.queue(client, sub)
      }
      return
    }
    if (!member && sub.member_count >= this.maxStreams) {
      const inserted = await client.query(`INSERT INTO ${this.schema}.subscription_pending_links (subscription_id, tenant_id, stream_id, before_offset)
        VALUES ($1,$2,$3,$4) ON CONFLICT (subscription_id,stream_id) DO UPDATE SET before_offset = LEAST(${this.schema}.subscription_pending_links.before_offset, EXCLUDED.before_offset) RETURNING (xmax = 0) AS inserted`, [sub.id, sub.tenant_id, change.stream_id, change.before_offset])
      if (inserted.rows[0]?.inserted) await client.query(`UPDATE ${this.schema}.subscriptions SET blocked_members = blocked_members + 1 WHERE id = $1`, [sub.id])
      return
    }
    if (!member) {
      await this.insertMember(client, sub, change.path, change.stream_id, change.before_offset, false, true)
      // insertMember may adopt an earlier quota-blocked baseline. Querying the
      // pending predicate avoids replacing that durable intent with this change.
      if (await this.pending(client, sub)) await this.markPending(client, sub)
      return
    } else if (member.stream_id !== change.stream_id) {
      if (member.stream_id !== null) await this.invalidate(client, sub, 'fenced')
      await client.query(`UPDATE ${this.schema}.subscription_members SET stream_id = $3, acked_offset = $4 WHERE subscription_id = $1 AND path = $2`, [sub.id, change.path, change.stream_id, change.before_offset])
      member.acked_offset = change.before_offset
    }
    if (active.tail_offset > member.acked_offset) await this.markPending(client, sub)
  }
  private async dispatch(): Promise<number> {
    return this.streams.transaction({}, async (client) => {
      const first = await client.query<Change>(`SELECT c.*, s.tenant_id, s.path FROM ${this.schema}.stream_changes c JOIN ${this.schema}.streams s ON s.id = c.stream_id
        WHERE NOT c.processed AND NOT EXISTS (SELECT 1 FROM ${this.schema}.stream_changes prior WHERE prior.stream_id = c.stream_id AND prior.version < c.version AND NOT prior.processed)
        ORDER BY c.created_at, c.stream_id, c.version LIMIT 1 FOR UPDATE OF c SKIP LOCKED`)
      const change = first.rows[0]
      if (!change) return 0
      await this.registry(client, change.tenant_id, false)
      const any = await client.query(`SELECT 1 FROM ${this.schema}.subscriptions WHERE tenant_id = $1 AND deleted_at IS NULL LIMIT 1`, [change.tenant_id])
      if (!any.rowCount) {
        // The shared registry lock makes this bulk fast path safe against a
        // registration whose at-tail backfill has not committed yet.
        const processed = await client.query(`WITH batch AS (SELECT c.stream_id, c.version FROM ${this.schema}.stream_changes c JOIN ${this.schema}.streams s ON s.id = c.stream_id
          WHERE NOT c.processed AND s.tenant_id = $1 ORDER BY c.created_at, c.stream_id, c.version LIMIT 128 FOR UPDATE OF c SKIP LOCKED)
          DELETE FROM ${this.schema}.stream_changes c USING batch b WHERE c.stream_id = b.stream_id AND c.version = b.version`, [change.tenant_id])
        return processed.rowCount ?? 0
      }
      const subscribers = await client.query<Sub>(`SELECT s.* FROM ${this.schema}.subscriptions s WHERE s.tenant_id = $1 AND s.deleted_at IS NULL
        AND ($2::uuid IS NULL OR s.id > $2) AND ((s.pattern_regex IS NOT NULL AND $3 ~ s.pattern_regex)
          OR EXISTS (SELECT 1 FROM ${this.schema}.subscription_members m WHERE m.subscription_id = s.id AND m.path = $3))
        ORDER BY s.id LIMIT 16 FOR UPDATE`, [change.tenant_id, change.fanout_after, change.path])
      // A complete fan-out in this transaction can cover a bounded committed
      // prefix of the same stream. The registry lock excludes registration,
      // and pinning the cutoff before member updates preserves later append intent.
      // Multi-transaction fan-out retains its original one-record checkpoint.
      let through = change.version
      if (change.fanout_after === null && subscribers.rows.length < 16) {
        const prefix = await client.query<{ version: string }>(`SELECT version FROM ${this.schema}.stream_changes
          WHERE stream_id = $1 AND version >= $2 AND NOT processed ORDER BY version LIMIT 128`, [change.stream_id, change.version])
        through = prefix.rows.at(-1)?.version ?? through
      }
      const active = await this.activePath(client, change.tenant_id, change.path)
      for (const sub of subscribers.rows) await this.applyChange(client, sub, change, active)
      if (subscribers.rows.length < 16) {
        // Member cursors/pending wake intent and job consumption commit
        // together. Registration cannot depend on a completed job: it holds
        // the exclusive registry barrier and establishes its own tail baseline.
        // Stream versions live on the stream, independently of outbox retention.
        const processed = await client.query(`DELETE FROM ${this.schema}.stream_changes
          WHERE stream_id = $1 AND version >= $2 AND version <= $3 AND NOT processed`, [change.stream_id, change.version, through])
        return processed.rowCount ?? 0
      }
      await client.query(`UPDATE ${this.schema}.stream_changes SET fanout_after = $3 WHERE stream_id = $1 AND version = $2`,
        [change.stream_id, change.version, subscribers.rows.at(-1)!.id])
      return 1
    })
  }
  private async recoverLink(): Promise<boolean> {
    return this.streams.transaction({}, async (client) => {
      const next = await client.query<{ subscription_id: string; stream_id: string; tenant_id: string; path: string; before_offset: string }>(`SELECT q.*, s.path FROM ${this.schema}.subscription_pending_links q JOIN ${this.schema}.streams s ON s.id = q.stream_id
        WHERE q.due_at <= clock_timestamp() ORDER BY q.due_at, q.subscription_id, q.stream_id LIMIT 1`)
      const row = next.rows[0]
      if (!row) return false
      await this.registry(client, row.tenant_id, false)
      const sub = (await client.query<Sub>(`SELECT * FROM ${this.schema}.subscriptions WHERE id = $1 FOR UPDATE`, [row.subscription_id])).rows[0]!
      const still = await client.query(`SELECT 1 FROM ${this.schema}.subscription_pending_links WHERE subscription_id = $1 AND stream_id = $2`, [sub.id, row.stream_id])
      if (!still.rowCount) return false
      const active = sub.deleted_at ? undefined : await this.activePath(client, sub.tenant_id, row.path)
      const member = await client.query(`SELECT 1 FROM ${this.schema}.subscription_members WHERE subscription_id = $1 AND path = $2`, [sub.id, row.path])
      if (active?.id === row.stream_id && sub.member_count >= this.maxStreams && !member.rowCount) {
        await client.query(`UPDATE ${this.schema}.subscription_pending_links SET due_at = clock_timestamp() + interval '60 seconds' WHERE subscription_id = $1 AND stream_id = $2`, [sub.id, row.stream_id])
        return true
      }
      if (active?.id === row.stream_id) await this.applyChange(client, sub, { stream_id: row.stream_id, tenant_id: row.tenant_id, path: row.path, before_offset: row.before_offset, after_offset: active.tail_offset, version: '0', kind: 'appended', fanout_after: null, active_stream_id: active.id }, active)
      const removed = await client.query(`DELETE FROM ${this.schema}.subscription_pending_links WHERE subscription_id = $1 AND stream_id = $2`, [sub.id, row.stream_id])
      if (removed.rowCount) await client.query(`UPDATE ${this.schema}.subscriptions SET blocked_members = blocked_members - 1 WHERE id = $1`, [sub.id])
      return true
    })
  }
  private async prepareDelivery(): Promise<{ job?: WebhookJob; worked: boolean }> {
    let attempted: string | undefined
    try { return await this.streams.transaction({}, async (client) => {
      const selected = await client.query<Sub>(`WITH locked AS MATERIALIZED (SELECT * FROM ${this.schema}.subscriptions WHERE deleted_at IS NULL AND next_work_at <= clock_timestamp()
        ORDER BY next_work_at, id LIMIT 1 FOR UPDATE SKIP LOCKED) SELECT *, lease_until > clock_timestamp() AS lease_valid FROM locked`)
      const sub = selected.rows[0]
      if (!sub) return { worked: false }
      attempted = sub.id
      if (sub.holder && !sub.lease_valid) await this.invalidate(client, sub, 'expired')
      if (sub.config.type === 'pull-wake' && sub.current_wake) {
        // A published wake waits indefinitely for a claim; a held lease is due
        // only on expiration. New appends do not flood the wake channel.
        await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = lease_until WHERE id = $1`, [sub.id])
        return { worked: true }
      }
      if (!sub.current_wake && !await this.pending(client, sub)) {
        await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = NULL WHERE id = $1`, [sub.id])
        return { worked: true }
      }
      const due = await client.query<{ due: boolean }>('SELECT $1::timestamptz IS NULL OR $1::timestamptz <= clock_timestamp() AS due', [sub.retry_at])
      if (!due.rows[0]!.due) {
        await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = LEAST(retry_at,lease_until) WHERE id = $1`, [sub.id])
        return { worked: true }
      }
      if (!sub.current_wake) {
        const claim = randomUUID()
        const issued = await this.issue(client, sub, claim)
        if (sub.config.type === 'pull-wake') {
          await client.query(`UPDATE ${this.schema}.subscriptions SET retry_count = 0, retry_at = NULL WHERE id = $1`, [sub.id])
          return { worked: true }
        }
        return { worked: true, job: { sub, wake: issued.wake, streams: issued.streams, claim } }
      }
      const wake = (await client.query<Wake>(`SELECT * FROM ${this.schema}.subscription_wakes WHERE id = $1`, [sub.current_wake])).rows[0]!
      const available = await client.query<{ available: boolean }>('SELECT $1::timestamptz IS NULL OR $1::timestamptz <= clock_timestamp() AS available', [wake.delivery_until])
      if (wake.state === 'delivered' || !available.rows[0]!.available) {
        await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = LEAST(lease_until,$2::timestamptz) WHERE id = $1`, [sub.id, wake.delivery_until ?? sub.lease_until])
        return { worked: true }
      }
      const claim = randomUUID()
      await client.query(`UPDATE ${this.schema}.subscription_wakes SET delivery_claim = $2, delivery_until = clock_timestamp() + $3 * interval '1 millisecond' WHERE id = $1`, [wake.id, claim, this.webhookTimeout + 10_000])
      await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = LEAST(lease_until,clock_timestamp() + $2 * interval '1 millisecond') WHERE id = $1`, [sub.id, this.webhookTimeout + 10_000])
      const snapshots = await client.query<Snapshot>(`SELECT * FROM ${this.schema}.subscription_wake_streams WHERE wake_id = $1 ORDER BY path`, [wake.id])
      return { worked: true, job: { sub, wake, claim, streams: snapshots.rows } }
    }) } catch (error) {
      if (attempted) await this.streams.transaction({}, async (client) => {
        const found = await client.query<Sub>(`WITH locked AS MATERIALIZED (SELECT * FROM ${this.schema}.subscriptions WHERE id = $1 AND deleted_at IS NULL FOR UPDATE)
          SELECT *, lease_until > clock_timestamp() AS lease_valid FROM locked`, [attempted])
        const sub = found.rows[0]
        if (!sub) return
        if (sub.holder && !sub.lease_valid) await this.invalidate(client, sub, 'expired')
        const delay = this.retryDelay(sub.retry_count)
        await client.query(`UPDATE ${this.schema}.subscriptions SET retry_count = retry_count + 1, retry_at = clock_timestamp() + $2 * interval '1 millisecond',
          next_work_at = LEAST(lease_until,clock_timestamp() + $2 * interval '1 millisecond') WHERE id = $1`, [sub.id, delay])
      }).catch(() => {})
      throw error
    }
  }
  private retryDelay(count: number): number {
    const base = Math.min(60_000, 1000 * 2 ** Math.min(count, 16))
    return Math.min(60_000, Math.round(base * randomInt(800, 1201) / 1000))
  }
  private async finishDelivery(job: WebhookJob, result: { ok: boolean; done: boolean }): Promise<void> {
    await this.streams.transaction({}, async (client) => {
      const found = await client.query<Sub>(`WITH locked AS MATERIALIZED (SELECT * FROM ${this.schema}.subscriptions WHERE id = $1 FOR UPDATE)
        SELECT *, lease_until > clock_timestamp() AS lease_valid FROM locked`, [job.sub.id])
      const sub = found.rows[0]!
      const wake = (await client.query<Wake>(`SELECT * FROM ${this.schema}.subscription_wakes WHERE id = $1`, [job.wake.id])).rows[0]!
      if (sub.deleted_at || sub.current_wake !== wake.id || !sub.lease_valid || wake.delivery_claim !== job.claim) return
      if (!result.ok) {
        const delay = this.retryDelay(sub.retry_count)
        await client.query(`UPDATE ${this.schema}.subscription_wakes SET delivery_claim = NULL, delivery_until = NULL WHERE id = $1`, [wake.id])
        await client.query(`UPDATE ${this.schema}.subscriptions SET retry_count = retry_count + 1, retry_at = clock_timestamp() + $2 * interval '1 millisecond',
          next_work_at = LEAST(lease_until,clock_timestamp() + $2 * interval '1 millisecond') WHERE id = $1`, [sub.id, delay])
        return
      }
      await client.query(`UPDATE ${this.schema}.subscriptions SET retry_count = 0, retry_at = NULL WHERE id = $1`, [sub.id])
      sub.retry_at = null
      if (result.done) {
        // Ack only the immutable wake snapshot. Later appends stay pending.
        await client.query(`UPDATE ${this.schema}.subscription_members m SET acked_offset = GREATEST(m.acked_offset,w.tail_offset)
          FROM ${this.schema}.subscription_wake_streams w WHERE w.wake_id = $1 AND m.subscription_id = $2 AND m.path = w.path AND m.stream_id IS NOT DISTINCT FROM w.stream_id`, [wake.id, sub.id])
        await this.invalidate(client, sub, 'completed')
        await this.queue(client, sub)
      } else {
        await client.query(`UPDATE ${this.schema}.subscription_wakes SET state = 'delivered', delivery_claim = NULL, delivery_until = NULL WHERE id = $1`, [wake.id])
        await client.query(`UPDATE ${this.schema}.subscriptions SET next_work_at = lease_until WHERE id = $1`, [sub.id])
      }
    })
  }
  async maintain(maximum = 8): Promise<{ changes: number; deliveries: number; links: number; worked: boolean }> {
    await this.assertReady()
    if (this.shutdown.signal.aborted) return { changes: 0, deliveries: 0, links: 0, worked: false }
    let changes = 0, deliveries = 0, links = 0
    let preparedAny = false
    for (let i = 0; i < maximum; i++) { const n = await this.dispatch(); changes += n; if (!n) break }
    for (let i = 0; i < maximum; i++) { if (!await this.recoverLink()) break; links++ }
    for (let i = 0; i < maximum; i++) {
      let prepared: { job?: WebhookJob; worked: boolean }
      try { prepared = await this.prepareDelivery() } catch (error) { this.report(error); break }
      if (!prepared.worked) break
      preparedAny = true
      const job = prepared.job
      if (!job) continue
      let result = { ok: false, done: false }
      try {
        const token = this.cipher.decrypt(job.wake.token_cipher!, `${this.installationId}:wake:${job.wake.id}`).toString('utf8')
        const body = Buffer.from(JSON.stringify({ subscription_id: job.sub.name, tenant_id: job.sub.tenant_id, wake_id: `w_${job.wake.id}`, generation: Number(job.wake.generation),
          streams: job.streams.map((row) => ({ path: row.path, link_type: row.link_type, acked_offset: row.acked_offset, tail_offset: row.tail_offset, has_pending: row.tail_offset > row.acked_offset })),
          callback_url: `${job.sub.public_base_url}/__ds/subscriptions/${encodeURIComponent(job.sub.name)}/callback`, callback_token: token }))
        result = await deliverWebhook(job.sub.config.webhook!.url, body, await this.signer.signature(body), { allowLocalhost: this.options.allowLocalhostWebhooks ?? false, timeoutMs: this.webhookTimeout, signal: this.shutdown.signal })
      } catch (error) { if (!this.shutdown.signal.aborted) this.report(error) }
      await this.finishDelivery(job, result)
      deliveries++
    }
    return { changes, deliveries, links, worked: preparedAny || changes > 0 || links > 0 }
  }
  close(): void { this.shutdown.abort() }
}
