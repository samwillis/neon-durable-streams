import { createHash } from 'node:crypto'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { MAX_CHUNK_BYTES } from './chunk-codec.js'
import { StreamError } from './errors.js'
import type { ChunkStorage } from './storage.js'

export interface S3StorageOptions {
  readonly endpoint: string
  readonly region: string
  readonly bucket: string
  readonly credentials?: { readonly accessKeyId: string; readonly secretAccessKey: string; readonly sessionToken?: string }
  readonly timeoutMs?: number
  readonly maxConcurrentRequests?: number
  readonly maxQueuedRequests?: number
  readonly allowHttp?: boolean
}

/** Consumer-owned adapter; call close after all servers/workers have stopped. */
export function createS3ChunkStorage(options: S3StorageOptions): ChunkStorage & { close(): void } {
  const endpoint = new URL(options.endpoint)
  if ((!options.allowHttp && endpoint.protocol !== 'https:') || !['https:', 'http:'].includes(endpoint.protocol)
    || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError('Invalid object storage endpoint')
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(options.bucket) || !options.region) throw new TypeError('Invalid bucket or region')
  const timeout = options.timeoutMs ?? 30_000
  const maximum = options.maxConcurrentRequests ?? 4
  const queued = options.maxQueuedRequests ?? 128
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 300_000
    || !Number.isSafeInteger(maximum) || maximum < 1 || maximum > 256
    || !Number.isSafeInteger(queued) || queued < 0 || queued > 100_000) throw new RangeError('Invalid storage request bounds')
  const client = new S3Client({ endpoint: endpoint.href, region: options.region, forcePathStyle: true,
    ...(options.credentials ? { credentials: options.credentials } : {}),
    requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED', maxAttempts: 3 })
  const identity = `s3:${createHash('sha256').update(JSON.stringify([endpoint.href, options.region, options.bucket])).digest('hex')}`
  let active = 0, closed = false
  const waiters = new Set<{ grant(): void; reject(error: unknown): void }>()
  const controllers = new Set<AbortController>()
  async function run<T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (closed) throw new Error('Object storage adapter is closed')
    if (active >= maximum) {
      if (waiters.size >= queued) throw new StreamError(503, 'STORAGE_BUSY', 'Object storage capacity reached', { 'Retry-After': '1' })
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => { waiters.delete(waiter); reject(new StreamError(503, 'STORAGE_BUSY', 'Object storage queue deadline exceeded', { 'Retry-After': '1' })) }, Math.min(timeout, 5000))
        const waiter = { grant() { clearTimeout(timer); active++; resolve() }, reject(error: unknown) { clearTimeout(timer); reject(error) } }
        waiters.add(waiter)
      })
    } else active++
    const controller = new AbortController()
    controllers.add(controller)
    const timer = setTimeout(() => controller.abort(new Error('Object storage operation deadline exceeded')), timeout)
    try { return await work(controller.signal) }
    finally {
      clearTimeout(timer)
      controllers.delete(controller)
      active--
      const next = waiters.values().next().value
      if (next) { waiters.delete(next); next.grant() }
    }
  }
  return {
    identity,
    put(key, bytes) {
      if (!bytes.byteLength || bytes.byteLength > MAX_CHUNK_BYTES) throw new RangeError('Invalid encoded chunk size')
      return run(async (abortSignal) => {
        await client.send(new PutObjectCommand({ Bucket: options.bucket, Key: key, Body: bytes, ContentType: 'application/octet-stream', ContentLength: bytes.byteLength }), { abortSignal })
      })
    },
    get(key, maxBytes) {
      if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_CHUNK_BYTES) throw new RangeError('Invalid encoded chunk limit')
      return run(async (abortSignal) => {
        const response = await client.send(new GetObjectCommand({ Bucket: options.bucket, Key: key }), { abortSignal })
        if (!response.Body) throw new Error('Object storage returned no body')
        const body = response.Body as AsyncIterable<Uint8Array> & { destroy(error?: Error): void }
        if (response.ContentLength !== undefined && response.ContentLength > maxBytes) { body.destroy(); throw new Error('Object exceeds manifest size') }
        let size = 0
        const parts: Uint8Array[] = []
        try {
          for await (const part of body) {
            size += part.byteLength
            if (size > maxBytes) throw new Error('Object exceeds manifest size')
            parts.push(part)
          }
        } finally { body.destroy() }
        return Buffer.concat(parts, size)
      })
    },
    close() {
      closed = true
      for (const waiter of waiters) waiter.reject(new Error('Object storage adapter is closed'))
      waiters.clear()
      for (const controller of controllers) controller.abort(new Error('Object storage adapter is closed'))
      client.destroy()
    },
  }
}
