import { createRealtime, rawSql } from '@neon/realtime/server'
import type { RawSqlQuery, RealtimeDirectServer } from '@neon/realtime/server'
import type { MaterializedLiveQuerySubscription } from '@neon/realtime/client'
import { validateSchema } from './path.js'

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

/** Direct server-side subscriptions; secrets and query parameters stay on the backend. */
export function createNeonRealtimeNotifier(options: NeonRealtimeOptions): StreamNotifier & { close(): void } {
  const maximum = options.maxWatches ?? 1000
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError('Invalid maxWatches')
  let client: RealtimeDirectServer<RawSqlQuery<unknown>> | undefined
  const entries = new Map<string, Entry>()
  let closed = false
  const report = (error: unknown) => { try { options.onError?.(error) } catch { /* observer only */ } }
  const retire = (key: string, entry: Entry) => {
    entry.disposed = true
    entry.subscription?.unsubscribe()
    entries.delete(key)
    if (!entries.size) { client?.close(); client = undefined }
  }
  return {
    watch(input, listener) {
      if (closed) throw new Error('Realtime notifier is closed')
      const schema = validateSchema(input.schema)
      const key = JSON.stringify([schema, input.tenantId, input.streamId])
      let entry = entries.get(key)
      if (!entry) {
        if (entries.size >= maximum) throw new Error('Realtime watch limit reached')
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
            for (const callback of current.listeners) { try { callback(head) } catch (error) { report(error) } }
          }
          subscription.onChange(publish)
          subscription.onStateChange((state) => { if (state.status === 'error') report(state.error) })
          await subscription.awaitRows(() => true, options.readyTimeoutMs ?? 20_000)
          publish()
        })()
        void current.ready.catch(report)
        entry = current
      }
      const watched = entry
      watched.listeners.add(listener)
      if (watched.head !== undefined) queueMicrotask(() => {
        if (!watched.disposed && watched.listeners.has(listener)) { try { listener(watched.head ?? null) } catch (error) { report(error) } }
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
    },
  }
}
