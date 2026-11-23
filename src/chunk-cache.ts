import type { Chunk } from './chunk-codec.js'
import { StreamError } from './errors.js'

/** Per-server bounded LRU of verified immutable objects. Response pages copy
 * their frames; cache hits never substitute for replay pins or authorization. */
export class VerifiedChunkCache {
  private readonly entries = new Map<string, { chunk: Chunk; charge: number }>()
  private readonly pending = new Map<string, Promise<Chunk>>()
  private used = 0
  private closed = false
  constructor(private readonly budget: number) {
    if (!Number.isSafeInteger(budget) || budget < 0 || budget > 256 * 1024 * 1024) throw new RangeError('Invalid verified chunk cache budget')
  }
  async get(key: string, load: () => Promise<Chunk>, encodedBytes: number): Promise<Chunk> {
    const found = this.entries.get(key)
    if (found) {
      this.entries.delete(key)
      this.entries.set(key, found)
      return found.chunk
    }
    const pending = this.pending.get(key)
    if (pending) return pending
    if (this.pending.size >= 128)
      throw new StreamError(503, 'REPLAY_BUSY', 'Too many object reads; retry from the durable checkpoint', { 'Retry-After': '1' })
    const work = Promise.resolve()
      .then(load)
      .then((chunk) => {
        // Charge buffers, decoded frame/offset objects and JSON index entries.
        // Bound entry count too, independently of the encoded payload budget.
        const charge = encodedBytes * 2 + key.length * 2 + chunk.frames.reduce((sum, frame) => sum + 512 + (frame.messageEnds?.length ?? 0) * 16, 0)
        if (!this.closed && charge <= this.budget) {
          while (this.entries.size >= 256 || this.used + charge > this.budget) {
            const oldest = this.entries.entries().next().value!
            this.used -= oldest[1].charge
            this.entries.delete(oldest[0])
          }
          this.entries.set(key, { chunk, charge })
          this.used += charge
        }
        return chunk
      })
      .finally(() => {
        this.pending.delete(key)
      })
    this.pending.set(key, work)
    return work
  }
  close(): void {
    this.closed = true
    this.entries.clear()
    this.used = 0
  }
}
