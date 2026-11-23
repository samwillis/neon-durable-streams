import { StreamError } from './errors.js'

/** Stream an owned, verified page without Fetch's BufferSource body copy. */
export function ownedResponseBody(bytes: Uint8Array, options: {
  readonly signal: AbortSignal
  readonly release: () => void
  readonly timeoutMs: number
  readonly expiresAt?: number
}): ReadableStream<Uint8Array> {
  let payload: Uint8Array | undefined = bytes
  let position = 0
  const deadline = performance.now() + options.timeoutMs
  let controller: ReadableStreamDefaultController<Uint8Array>
  const finish = () => {
    if (!payload) return
    payload = undefined
    clearTimeout(timer)
    options.signal.removeEventListener('abort', abort)
    options.release()
  }
  const abort = () => {
    if (!payload) return
    finish()
    controller.error(options.signal.reason ?? new StreamError(408, 'REQUEST_ABORTED', 'Request aborted'))
  }
  const lifetime = options.expiresAt === undefined ? options.timeoutMs : Math.min(options.timeoutMs, Math.max(1, options.expiresAt - Date.now()))
  const timer = setTimeout(() => {
    if (!payload) return
    finish()
    controller.error(new StreamError(503, 'RESPONSE_DEADLINE', 'Response deadline exceeded; resume from the durable checkpoint'))
  }, lifetime).unref()
  return new ReadableStream<Uint8Array>({
    start(value) {
      controller = value
      options.signal.addEventListener('abort', abort, { once: true })
      if (options.signal.aborted) abort()
    },
    pull(value) {
      if (!payload) return
      if (performance.now() >= deadline || (options.expiresAt !== undefined && Date.now() >= options.expiresAt)) {
        finish()
        value.error(new StreamError(408, 'RESPONSE_EXPIRED', 'Response expired; resume from the durable checkpoint'))
        return
      }
      if (position === payload.byteLength) { finish(); value.close(); return }
      const end = Math.min(payload.byteLength, position + 65536)
      value.enqueue(payload.subarray(position, end))
      position = end
    },
    cancel() { finish() },
  }, { highWaterMark: 0 })
}
