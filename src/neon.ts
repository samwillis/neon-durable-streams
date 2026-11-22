import { observe } from './observer.js'
import { createRealtime, rawSql } from '@neon/realtime/server'
import type { RawSqlQuery, RealtimeDirectServer } from '@neon/realtime/server'
import type { MaterializedLiveQuerySubscription, RawLiveQuerySubscription } from '@neon/realtime/client'
import { validateSchema } from './path.js'
import type { AppendUpdate, RealtimeAppend } from './live-payload.js'
import type { InstallationIdentity } from './installation.js'
export type { AppendUpdate, RealtimeAppend } from './live-payload.js'

export interface StreamHead {
  readonly streamId: string
  readonly offset: string
  readonly version: string
  readonly closed: boolean
  readonly deleted: boolean
}
export interface HeadWatch {
  /** Resolves only when an initial authoritative result is available. */
  readonly ready: Promise<void>
  readonly close: () => void
}
export interface StreamNotifier {
  readonly watch: (input: { readonly schema: string; readonly tenantId: string; readonly streamId: string },
    listener: (head: StreamHead | null) => void) => HeadWatch
  /** Stable UUIDs must already be authorized by the server. No path/tenant join. */
  readonly watchAppends?: (input: { readonly schema: string; readonly tenantId: string; readonly streamId: string; readonly after: string; readonly installation: InstallationIdentity },
    listener: (update: AppendUpdate) => void) => HeadWatch
}
export interface NeonRealtimeOptions {
  readonly url: string
  readonly secret: string
  readonly database: string
  readonly maxWatches?: number
  readonly readyTimeoutMs?: number
  readonly onError?: (error: unknown) => void
}
interface Row { id: string; tail_offset: string; change_version: string; closed: boolean; deleted_at: Date | null }
interface Entry {
  listeners: Set<(head: StreamHead | null) => void>
  subscription?: MaterializedLiveQuerySubscription<Row>
  ready: Promise<void>
  fingerprint?: string
  head?: StreamHead | null
  disposed: boolean
}
interface AppendDbRow { stream_id: string; start_offset: string; end_offset: string; payload: Uint8Array | null; message_ends: readonly number[] | null }
interface IdentityWatch extends HeadWatch { readonly isCurrent: () => boolean }
interface AppendEntry {
  readonly listeners: Set<(update: AppendUpdate) => void>
  subscription?: RawLiveQuerySubscription<AppendDbRow>
  ready: Promise<void>
  disposed: boolean
  identityWatch?: IdentityWatch
}
interface IdentityEntry {
  listeners: Set<(valid: boolean) => void>
  subscription?: MaterializedLiveQuerySubscription<InstallationIdentity>
  ready: Promise<void>
  valid: boolean
  disposed: boolean
}

/** Direct server-side subscriptions; secrets and query parameters stay on the backend. */
export function createNeonRealtimeNotifier(options: NeonRealtimeOptions): StreamNotifier & { close(): void } {
  const maximum = options.maxWatches ?? 1000
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError('Invalid maxWatches')
  let client: RealtimeDirectServer<RawSqlQuery<unknown>> | undefined
  const entries = new Map<string, Entry>()
  const appendEntries = new Map<string, AppendEntry>()
  const identities = new Map<string, IdentityEntry>()
  let closed = false
  const report = (error: unknown) => observe(options.onError, error)
  const retire = (key: string, entry: Entry) => {
    entry.disposed = true
    entry.subscription?.unsubscribe()
    entries.delete(key)
    if (!entries.size && !appendEntries.size && !identities.size) { client?.close(); client = undefined }
  }
  const retireAppends = (key: string, entry: AppendEntry) => {
    entry.disposed = true; entry.subscription?.unsubscribe(); entry.identityWatch?.close(); appendEntries.delete(key)
    if (!entries.size && !appendEntries.size && !identities.size) { client?.close(); client = undefined }
  }
  const watchIdentity = (schema: string, expected: InstallationIdentity, listener: (valid: boolean) => void): IdentityWatch => {
    const key = JSON.stringify([schema, expected.id, expected.serving_epoch, expected.environment_id, expected.storage_identity])
    let entry = identities.get(key)
    if (!entry) {
      if (entries.size + appendEntries.size + identities.size >= maximum) throw new Error('Realtime watch limit reached')
      const current: IdentityEntry = { listeners: new Set(), ready: Promise.resolve(), valid: false, disposed: false }
      identities.set(key, current)
      client ??= createRealtime({ url: options.url, secret: options.secret, db: options.database })
      const active = client
      current.ready = (async () => {
        const subscription = await active.subscribe(rawSql<InstallationIdentity>(`SELECT id, serving_epoch, serving_state, environment_id, storage_identity
          FROM ${schema}.installation WHERE singleton = true`))
        if (current.disposed) { subscription.unsubscribe(); return }
        current.subscription = subscription
        let everValid = false
        const publish = () => {
          if (current.disposed) return
          const snapshot = subscription.getSnapshot(), row = snapshot.data?.[0]
          current.valid = subscription.getState().status === 'live' && snapshot.status === 'live' && snapshot.data?.length === 1 && !!row && row.id === expected.id
            && row.serving_epoch === expected.serving_epoch && row.serving_state === 'active'
            && row.environment_id === expected.environment_id && row.storage_identity === expected.storage_identity
          if (current.valid) everValid = true
          if (!everValid) return
          for (const callback of current.listeners) observe(callback, current.valid, report)
        }
        subscription.onChange(publish)
        subscription.onStateChange((state) => { if (state.status !== 'live') publish() })
        await subscription.awaitRows(() => true, options.readyTimeoutMs ?? 20_000)
        publish()
        if (!current.valid) throw new Error('Realtime installation identity differs from the serving database')
      })().catch((error: unknown) => { if (!current.disposed) throw error })
      void current.ready.catch(report)
      entry = current
    }
    const watched = entry
    watched.listeners.add(listener)
    return { isCurrent: () => watched.valid && !watched.disposed, ready: watched.ready.then(() => { if (!watched.valid && !watched.disposed) throw new Error('Realtime installation identity is not current') }), close() {
      watched.listeners.delete(listener)
      if (!watched.listeners.size && !watched.disposed) {
        watched.disposed = true; watched.subscription?.unsubscribe(); identities.delete(key)
      }
    } }
  }
  return {
    watchAppends(input, listener) {
      if (closed) throw new Error('Realtime notifier is closed')
      const schema = validateSchema(input.schema)
      const key = JSON.stringify([schema, input.tenantId, input.streamId, input.after, input.installation])
      let entry = appendEntries.get(key)
      if (!entry) {
        if (entries.size + appendEntries.size + identities.size >= maximum) throw new Error('Realtime watch limit reached')
        const current: AppendEntry = { listeners: new Set(), ready: Promise.resolve(), disposed: false }
        client ??= createRealtime({ url: options.url, secret: options.secret, db: options.database })
        appendEntries.set(key, current)
        const active = client
        current.ready = (async () => {
          current.identityWatch = watchIdentity(schema, input.installation, (valid) => {
            if (!valid && !current.disposed) for (const callback of current.listeners) observe(callback, { type: 'invalidate' }, report)
          })
          await current.identityWatch.ready
          if (current.disposed) return
          const subscription = await active.subscribe(rawSql<AppendDbRow>(`SELECT stream_id, start_offset, end_offset, payload, message_ends
            FROM ${schema}.appends WHERE stream_id = $1::uuid AND end_offset > $2`, [input.streamId, input.after]), { materialize: false })
          if (current.disposed) { subscription.unsubscribe(); return }
          current.subscription = subscription
          const publish = (update: AppendUpdate) => { if (!current.disposed) for (const callback of current.listeners) observe(callback, update, report) }
          const row = (rowId: string, value: AppendDbRow): RealtimeAppend => ({ rowId, streamId: value.stream_id, start: value.start_offset, end: value.end_offset, payload: value.payload, messageEnds: value.message_ends })
          subscription.onUpdate((update) => {
            if (!current.identityWatch?.isCurrent()) { publish({ type: 'invalidate' }); return }
            if ((update.type === 'rows' ? update.rows.length : update.changes.length) > 4096) { publish({ type: 'invalidate' }); return }
            if (update.type === 'rows') publish({ type: 'replace', rows: update.rows.map((value) => row(value.rowId, value.row)), removed: [] })
            else publish({ type: 'changes', rows: update.changes.flatMap((change) => change.type === 'upsert' ? [row(change.rowId, change.row)] : []),
              removed: update.changes.flatMap((change) => change.type === 'remove' ? [change.rowId] : []) })
          })
          let wasLive = false
          await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('Realtime append snapshot deadline exceeded')), options.readyTimeoutMs ?? 20_000)
            const state = (value: ReturnType<typeof subscription.getState>) => {
              if (current.disposed) { clearTimeout(timer); resolve(); return }
              if (value.status === 'live') { wasLive = true; clearTimeout(timer); resolve() }
              else if (value.status === 'error') { clearTimeout(timer); publish({ type: 'invalidate' }); reject(value.error); report(value.error) }
              else if (wasLive) publish({ type: 'invalidate' })
            }
            subscription.onStateChange(state); state(subscription.getState())
          })
        })().catch((error: unknown) => { if (!current.disposed) throw error })
        void current.ready.catch(report)
        entry = current
      }
      const watched = entry
      watched.listeners.add(listener)
      let disposed = false
      return { ready: watched.ready, close() {
        if (disposed) return
        disposed = true; watched.listeners.delete(listener)
        if (!watched.listeners.size) retireAppends(key, watched)
      } }
    },
    watch(input, listener) {
      if (closed) throw new Error('Realtime notifier is closed')
      const schema = validateSchema(input.schema)
      const key = JSON.stringify([schema, input.tenantId, input.streamId])
      let entry = entries.get(key)
      if (!entry) {
        if (entries.size + appendEntries.size + identities.size >= maximum) throw new Error('Realtime watch limit reached')
        const current: Entry = { listeners: new Set(), ready: Promise.resolve(), disposed: false }
        client ??= createRealtime({ url: options.url, secret: options.secret, db: options.database })
        entries.set(key, current)
        const active = client
        current.ready = (async () => {
          const subscription = await active.subscribe(rawSql<Row>(`SELECT id, tail_offset, change_version, closed, deleted_at
            FROM ${schema}.streams WHERE tenant_id = $1 AND id = $2::uuid`, [input.tenantId, input.streamId]))
          if (current.disposed) { subscription.unsubscribe(); return }
          current.subscription = subscription
          const publish = () => {
            if (current.disposed) return
            const snapshot = subscription.getSnapshot()
            if (snapshot.data === undefined || snapshot.status !== 'live') return
            const row = snapshot.data[0]
            const head = row ? { streamId: row.id, offset: row.tail_offset, version: String(row.change_version), closed: row.closed, deleted: row.deleted_at !== null } : null
            const fingerprint = JSON.stringify(head)
            if (current.fingerprint === fingerprint) return
            current.fingerprint = fingerprint
            current.head = head
            for (const callback of current.listeners) observe(callback, head, report)
          }
          subscription.onChange(publish)
          subscription.onStateChange((state) => { if (!current.disposed && state.status === 'error') report(state.error) })
          await subscription.awaitRows(() => true, options.readyTimeoutMs ?? 20_000)
          publish()
        })().catch((error: unknown) => { if (!current.disposed) throw error })
        void current.ready.catch(report)
        entry = current
      }
      const watched = entry
      watched.listeners.add(listener)
      if (watched.head !== undefined) queueMicrotask(() => {
        if (!watched.disposed && watched.listeners.has(listener)) observe(listener, watched.head ?? null, report)
      })
      let disposed = false
      return { ready: watched.ready, close() {
        if (disposed) return
        disposed = true
        watched.listeners.delete(listener)
        if (!watched.listeners.size) retire(key, watched)
      } }
    },
    close() {
      closed = true
      for (const [key, entry] of entries) retire(key, entry)
      for (const [key, entry] of appendEntries) retireAppends(key, entry)
    },
  }
}
