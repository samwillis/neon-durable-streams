import { StreamError } from './errors.js'

/** Accounts for private replay work and response bytes until the body drains. */
export class ReplayBudget {
  private bytes = 0
  private requests = 0
  private closed = false
  constructor(private readonly maximumBytes: number, private readonly maximumRequests = 128) {
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || !Number.isSafeInteger(maximumRequests) || maximumRequests < 1) throw new RangeError('Invalid replay budget')
  }
  get activeBytes(): number { return this.bytes }
  get activeRequests(): number { return this.requests }
  acquire(bytes: number): () => void {
    if (!Number.isSafeInteger(bytes) || bytes < 1) throw new RangeError('Invalid replay reservation')
    if (this.closed || this.requests >= this.maximumRequests || this.bytes + bytes > this.maximumBytes) {
      throw new StreamError(503, 'REPLAY_BUSY', 'Replay capacity reached; retry from the durable checkpoint', { 'Retry-After': '1' })
    }
    this.bytes += bytes
    this.requests++
    let released = false
    return () => {
      if (released) return
      released = true
      this.bytes -= bytes
      this.requests--
    }
  }
  close(): void { this.closed = true }
}
