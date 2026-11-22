import { advanceOffset, parseOffset } from './offset.js'
import type { HeadWatch, StreamNotifier } from './neon.js'
import type { ReadResult } from './store.js'
import type { InstallationIdentity } from './installation.js'

export interface RealtimeAppend {
  readonly rowId: string
  readonly streamId: string
  readonly start: string
  readonly end: string
  readonly payload: Uint8Array | null
  readonly messageEnds: readonly number[] | null
}
export type AppendUpdate =
  | { readonly type: 'replace' | 'changes'; readonly rows: readonly RealtimeAppend[]; readonly removed: readonly string[] }
  | { readonly type: 'invalidate' }
export interface BufferedAppend { readonly start: string; readonly end: string; readonly payload: Buffer; readonly messageEnds: readonly number[] | null }

/** Unordered committed rows become only a contiguous, bounded byte prefix. */
export class AppendBuffer {
  private readonly frames = new Map<string, BufferedAppend & { rowId: string }>()
  private readonly rowIds = new Map<string, string>()
  private bytes = 0
  private generation = 0
  private dirty = true
  constructor(private readonly streamId: string, private readonly maxBytes: number, private readonly maxFrames: number) {}
  get revision(): number { return this.generation }
  get needsReplay(): boolean { return this.dirty }
  get size(): number { return this.frames.size }
  requireReplay(): void { this.dirty = true; this.generation++ }
  invalidate(): void {
    this.frames.clear(); this.rowIds.clear(); this.bytes = 0; this.dirty = true; this.generation++
  }
  reconcile(cursor: string, revision: number): boolean {
    if (revision !== this.generation) return false
    for (const [start, frame] of this.frames) if (frame.end <= cursor) this.remove(start)
    this.dirty = false
    return true
  }
  ingest(update: AppendUpdate, cursor: string): boolean {
    if (update.type === 'invalidate') { this.invalidate(); return false }
    if (update.type === 'replace') this.invalidate()
    if (update.removed.length) { this.invalidate(); return false }
    for (const row of update.rows) {
      try {
        parseOffset(row.start); parseOffset(row.end)
        if (row.streamId !== this.streamId || row.start >= row.end) throw new Error('Invalid Realtime frame identity')
        if (row.end <= cursor) continue
        if (!row.payload || row.start < cursor || advanceOffset(row.start, row.payload.byteLength) !== row.end) throw new Error('Realtime frame needs durable replay')
        if (row.messageEnds) {
          let previous = 0
          for (const end of row.messageEnds) {
            if (!Number.isSafeInteger(end) || end <= previous || end > row.payload.byteLength || row.payload[end - 1] !== 44) throw new Error('Invalid JSON frame index')
            previous = end
          }
          if (previous !== row.payload.byteLength) throw new Error('Incomplete JSON frame index')
        }
        const previous = this.frames.get(row.start)
        if (previous) {
          if (previous.end !== row.end || !previous.payload.equals(row.payload)
            || JSON.stringify(previous.messageEnds) !== JSON.stringify(row.messageEnds)) throw new Error('Conflicting Realtime frame')
          continue
        }
        if (this.frames.size >= this.maxFrames || this.bytes + row.payload.byteLength > this.maxBytes) throw new Error('Realtime buffer capacity reached')
        const oldStart = this.rowIds.get(row.rowId)
        if (oldStart && oldStart !== row.start) throw new Error('Realtime row identity changed')
        for (const frame of this.frames.values()) if (row.start < frame.end && row.end > frame.start) throw new Error('Overlapping Realtime frames')
        const payload = Buffer.from(row.payload)
        this.frames.set(row.start, { rowId: row.rowId, start: row.start, end: row.end, payload, messageEnds: row.messageEnds ? [...row.messageEnds] : null })
        this.rowIds.set(row.rowId, row.start); this.bytes += payload.length
      } catch { this.invalidate(); return false }
    }
    return true
  }
  private remove(start: string): void {
    const frame = this.frames.get(start)
    if (!frame) return
    this.frames.delete(start); this.rowIds.delete(frame.rowId); this.bytes -= frame.payload.length
  }
  take(cursor: string, maxBytes: number, maxFrames: number, through?: string): { frames: readonly BufferedAppend[]; next: string; ahead: boolean } | undefined {
    if (this.dirty) return undefined
    const frames: BufferedAppend[] = []
    let next = cursor, bytes = 0
    for (;;) {
      const frame = this.frames.get(next)
      if (!frame || (through !== undefined && frame.end > through) || frames.length >= maxFrames || bytes + frame.payload.length > maxBytes) break
      frames.push(frame); bytes += frame.payload.length; next = frame.end
      this.remove(frame.start)
    }
    const ahead = this.frames.size > 0
    if (!frames.length && ahead && !this.frames.has(cursor)) { this.invalidate(); return undefined }
    return { frames, next, ahead }
  }
}

export interface PayloadWindowOptions {
  readonly notifier: StreamNotifier
  readonly schema: string
  readonly tenantId: string
  readonly streamId: string
  readonly installation: InstallationIdentity
  readonly maxBytes: number
  readonly maxFrames: number
  readonly windowMs: number
  readonly wake: () => void
  readonly report: (error: unknown) => void
}
type WindowReplay = Pick<ReadResult, 'next' | 'upToDate'> & { readonly stream: { readonly closed: boolean } }

/** A raw live view is replaced at a bounded cursor/lifetime or buffer limit. */
export class PayloadWindow {
  private readonly buffer: AppendBuffer
  private watch: HeadWatch | undefined
  private timer: ReturnType<typeof setTimeout> | undefined
  private generation = 0
  private ready = false
  private restart = true
  private disposed = false
  private cursor: string
  private retryAfter = 0
  constructor(private readonly options: PayloadWindowOptions, initial: WindowReplay) {
    this.cursor = initial.next
    this.buffer = new AppendBuffer(options.streamId, options.maxBytes, options.maxFrames)
    if (initial.upToDate && !initial.stream.closed) this.start(initial.next)
  }
  get revision(): number { return this.buffer.revision }
  get needsReplay(): boolean { return this.restart || (this.ready && this.buffer.needsReplay) }
  get starting(): boolean { return !!this.watch && !this.ready }
  get verified(): boolean { return this.ready && !this.restart }
  private stop(): void {
    this.generation++; clearTimeout(this.timer); this.timer = undefined
    this.watch?.close(); this.watch = undefined; this.ready = false
  }
  invalidate(): void {
    this.stop(); this.restart = true; this.buffer.invalidate(); this.options.wake()
  }
  private start(cursor: string): void {
    if (performance.now() < this.retryAfter) { this.restart = true; return }
    this.stop(); this.cursor = cursor; this.restart = false; this.buffer.invalidate()
    const generation = this.generation
    let sawSnapshot = false
    try {
      const watch = this.options.notifier.watchAppends!({ schema: this.options.schema, tenantId: this.options.tenantId, streamId: this.options.streamId, after: cursor, installation: this.options.installation }, (update) => {
        if (this.disposed || generation !== this.generation) return
        if (update.type === 'invalidate' || (update.type === 'replace' && sawSnapshot)) { this.invalidate(); return }
        if (update.type === 'replace') sawSnapshot = true
        if (!this.buffer.ingest(update, this.cursor)) { this.invalidate(); return }
        this.options.wake()
      })
      this.watch = watch
      void watch.ready.then(() => {
        if (this.disposed || generation !== this.generation) return
        this.ready = true; this.buffer.requireReplay(); this.options.wake()
      }, (error: unknown) => {
        if (this.disposed || generation !== this.generation) return
        this.retryAfter = performance.now() + 1000; this.options.report(error); this.invalidate()
      })
      this.timer = setTimeout(() => this.invalidate(), this.options.windowMs).unref()
    } catch (error) { this.retryAfter = performance.now() + 1000; this.options.report(error); this.invalidate() }
  }
  reconcile(result: WindowReplay, revision: number): void {
    this.cursor = result.next
    if (!result.upToDate || result.stream.closed) { this.stop(); this.restart = true; this.buffer.invalidate(); return }
    if (this.restart || !this.watch) { this.start(result.next); return }
    this.buffer.reconcile(result.next, revision)
  }
  take(cursor: string, maxBytes: number, maxFrames: number, through: string): ReturnType<AppendBuffer['take']> {
    this.cursor = cursor
    if (!this.ready || this.restart) return undefined
    const result = this.buffer.take(cursor, maxBytes, maxFrames, through)
    if (this.buffer.needsReplay && !result) this.invalidate()
    if (result) this.cursor = result.next
    return result
  }
  close(): void { this.disposed = true; this.stop(); this.buffer.invalidate() }
}
