import type { Chunk } from './chunk-codec.js'
import { StreamError } from './errors.js'

export interface ChunkRead {
  readonly key: string
  readonly encodedBytes: number
}
interface PendingChunk extends ChunkRead {
  readonly group: ReadGroup
  readonly promise: Promise<Chunk>
  readonly resolve: (chunk: Chunk) => void
  readonly reject: (error: unknown) => void
}
interface ReadGroup {
  readonly controller: AbortController
  readonly entries: PendingChunk[]
  readonly bytes: number
  readonly finished: Promise<void>
  readonly finish: () => void
  interests: number
  done: boolean
}

/** Shares bounded immutable reads. Each waiting reader owns its own interest. */
export class VerifiedChunkCache {
  private readonly entries = new Map<string, { chunk: Chunk; charge: number }>()
  private readonly pending = new Map<string, PendingChunk>()
  private readonly groups = new Set<ReadGroup>()
  private used = 0
  private loadingBytes = 0
  private closed = false
  constructor(private readonly budget: number, private readonly maximumLoadingBytes = 128 * 1024 * 1024) {
    if (!Number.isSafeInteger(budget) || budget < 0 || budget > 256 * 1024 * 1024
      || !Number.isSafeInteger(maximumLoadingBytes) || maximumLoadingBytes < 1) throw new RangeError('Invalid verified cache budget')
  }
  get inFlightBytes(): number { return this.loadingBytes }
  peek(key: string): Chunk | undefined {
    const found = this.entries.get(key)
    if (!found) return undefined
    this.entries.delete(key)
    this.entries.set(key, found)
    return found.chunk
  }
  async get(key: string, load: (signal: AbortSignal) => Promise<Chunk>, encodedBytes: number, signal?: AbortSignal): Promise<Chunk> {
    const chunks = await this.getMany([{ key, encodedBytes }], async (_missing, sharedSignal) => [await load(sharedSignal)], signal)
    return chunks[0]!
  }
  async getMany(requests: readonly ChunkRead[], load: (missing: readonly ChunkRead[], signal: AbortSignal) => Promise<readonly Chunk[]>, signal?: AbortSignal): Promise<readonly Chunk[]> {
    signal?.throwIfAborted()
    if (this.closed) throw new Error('Verified cache is closed')
    if (requests.some((request) => !Number.isSafeInteger(request.encodedBytes) || request.encodedBytes < 1)) throw new RangeError('Invalid immutable read size')
    const missing = [...new Map(requests.filter((request) => !this.entries.has(request.key) && !this.pending.has(request.key)).map((request) => [request.key, request])).values()]
    const bytes = missing.reduce((sum, request) => sum + request.encodedBytes, 0)
    if (this.pending.size + missing.length > 4096 || (missing.length && (this.groups.size >= 128 || this.loadingBytes + bytes > this.maximumLoadingBytes))) {
      throw new StreamError(503, 'REPLAY_BUSY', 'Object read capacity reached; retry from the durable checkpoint', { 'Retry-After': '1' })
    }
    let group: ReadGroup | undefined
    if (missing.length) {
      let finish!: () => void
      const finished = new Promise<void>((resolve) => { finish = resolve })
      group = { controller: new AbortController(), entries: [], bytes, finished, finish, interests: 0, done: false }
      this.groups.add(group)
      this.loadingBytes += bytes
      for (const request of missing) {
        let resolve!: (chunk: Chunk) => void, reject!: (error: unknown) => void
        const promise = new Promise<Chunk>((accept, fail) => { resolve = accept; reject = fail })
        const entry = { ...request, group, promise, resolve, reject }
        group.entries.push(entry)
        this.pending.set(request.key, entry)
      }
    }
    const interests: PendingChunk[] = []
    const values = requests.map((request) => {
      const cached = this.peek(request.key)
      if (cached) return Promise.resolve(cached)
      const entry = this.pending.get(request.key)!
      interests.push(entry)
      entry.group.interests++
      return entry.promise
    })
    const result = this.wait(values, interests, signal)
    if (group) void this.start(group, load)
    return result
  }
  private wait(values: readonly Promise<Chunk>[], interests: readonly PendingChunk[], signal?: AbortSignal): Promise<readonly Chunk[]> {
    let released = false
    const release = () => {
      if (released) return []
      released = true
      const abandoned = new Set<ReadGroup>()
      for (const entry of interests) {
        const group = entry.group
        group.interests--
        if (!group.done && group.interests === 0) {
          group.controller.abort(new Error('No readers remain for the object request'))
          abandoned.add(group)
          for (const owned of group.entries) if (this.pending.get(owned.key) === owned) this.pending.delete(owned.key)
        }
      }
      return [...abandoned].map((group) => group.finished)
    }
    return new Promise((resolve, reject) => {
      let aborted = false
      const abort = () => {
        aborted = true
        const settling = release()
        // A last reader retains its replay lease until canceled I/O settles.
        void Promise.allSettled(settling).then(() => reject(signal!.reason))
      }
      signal?.addEventListener('abort', abort, { once: true })
      if (signal?.aborted) abort()
      void Promise.all(values).then((chunks) => { if (!aborted) resolve(chunks) }, (error: unknown) => { if (!aborted) reject(error) }).finally(() => {
        signal?.removeEventListener('abort', abort)
        release()
      })
    })
  }
  private async start(group: ReadGroup, load: (missing: readonly ChunkRead[], signal: AbortSignal) => Promise<readonly Chunk[]>): Promise<void> {
    try {
      group.controller.signal.throwIfAborted()
      const chunks = await load(group.entries, group.controller.signal)
      group.controller.signal.throwIfAborted()
      if (chunks.length !== group.entries.length) throw new Error('Immutable read returned an incomplete group')
      for (const [index, entry] of group.entries.entries()) {
        this.retain(entry.key, chunks[index]!, entry.encodedBytes)
        entry.resolve(chunks[index]!)
      }
    } catch (error) {
      for (const entry of group.entries) entry.reject(error)
    } finally {
      group.done = true
      this.loadingBytes -= group.bytes
      this.groups.delete(group)
      for (const entry of group.entries) if (this.pending.get(entry.key) === entry) this.pending.delete(entry.key)
      group.finish()
    }
  }
  private retain(key: string, chunk: Chunk, encodedBytes: number): void {
    const charge = encodedBytes * 2 + key.length * 2 + chunk.frames.reduce((sum, frame) => sum + 512 + (frame.messageEnds?.length ?? 0) * 16, 0)
    if (this.closed || charge > this.budget) return
    while (this.entries.size >= 256 || this.used + charge > this.budget) {
      const oldest = this.entries.entries().next().value!
      this.used -= oldest[1].charge
      this.entries.delete(oldest[0])
    }
    this.entries.set(key, { chunk, charge })
    this.used += charge
  }
  close(): void {
    this.closed = true
    this.entries.clear()
    this.used = 0
    for (const group of this.groups) group.controller.abort(new Error('Verified cache is closed'))
  }
}
