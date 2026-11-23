import { createHash } from 'node:crypto'
import { DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import type { GetObjectCommandInput, GetObjectCommandOutput } from '@aws-sdk/client-s3'
import { ChunkIntegrityError, MAX_CHUNK_BYTES } from './chunk-codec.js'
import { StreamError } from './errors.js'
import type { ChunkStorage, ChunkByteRange } from './storage.js'
import { ChunkNotFoundError } from './storage.js'

const UUID_KEY = '[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
const IMMUTABLE_KEY = new RegExp(`^v1/${UUID_KEY}/${UUID_KEY}/${UUID_KEY}/[0-9a-f]{64}\\.ndsc$`)
const INVENTORY_PREFIX = new RegExp(`^v1/${UUID_KEY}/$`)

async function readObjectBody(response: GetObjectCommandOutput, maximum: number, exactLength?: number): Promise<Buffer> {
  if (!response.Body) throw new ChunkIntegrityError('Object storage returned no body')
  const body = response.Body as AsyncIterable<Uint8Array> & { destroy(error?: Error): void }
  try {
    const length = response.ContentLength ?? exactLength ?? maximum
    if (!Number.isSafeInteger(length) || length < 0 || length > maximum || (exactLength !== undefined && length !== exactLength)) {
      throw new ChunkIntegrityError('Object length differs from the requested bound')
    }
    const bytes = Buffer.allocUnsafe(length)
    let size = 0
    for await (const part of body) {
      if (size + part.byteLength > length) throw new ChunkIntegrityError('Object exceeds declared size')
      bytes.set(part, size)
      size += part.byteLength
    }
    if ((response.ContentLength !== undefined || exactLength !== undefined) && size !== length) throw new ChunkIntegrityError('Object is shorter than declared size')
    return size === length ? bytes : Buffer.from(bytes.subarray(0, size))
  } finally { body.destroy() }
}

async function consumeRangeBody(response: GetObjectCommandOutput, length: number, consume: (part: Uint8Array) => void | Promise<void>): Promise<void> {
  if (!response.Body) throw new ChunkIntegrityError('Object storage returned no range body')
  const body = response.Body as AsyncIterable<Uint8Array> & { destroy(error?: Error): void }
  let size = 0
  try {
    if (response.ContentLength !== undefined && response.ContentLength !== length) throw new ChunkIntegrityError('Range length differs')
    for await (const part of body) {
      size += part.byteLength
      if (size > length) throw new ChunkIntegrityError('Range exceeds requested size')
      await consume(part)
    }
    if (size !== length) throw new ChunkIntegrityError('Range ended before its declared size')
  } finally { body.destroy() }
}

function validateRange(range: ChunkByteRange): void {
  const { start, end, totalBytes } = range
  if (![start, end, totalBytes].every(Number.isSafeInteger) || start < 0 || start >= end || end > totalBytes || totalBytes > MAX_CHUNK_BYTES) {
    throw new RangeError('Invalid immutable object range')
  }
}

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
export function createS3ChunkStorage(options: S3StorageOptions): ChunkStorage & {
  close(): void; delete: NonNullable<ChunkStorage['delete']>; list: NonNullable<ChunkStorage['list']>
  readRange: NonNullable<ChunkStorage['readRange']>
} {
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
  async function getObject(input: GetObjectCommandInput, signal: AbortSignal): Promise<GetObjectCommandOutput> {
    return client.send(new GetObjectCommand(input), { abortSignal: signal }).catch((error: unknown) => {
      if (typeof error === 'object' && error !== null && '$metadata' in error && (error.$metadata as { httpStatusCode?: number })?.httpStatusCode === 404) throw new ChunkNotFoundError('Immutable chunk not found')
      throw error
    })
  }
  async function rangeObject(key: string, range: ChunkByteRange, signal: AbortSignal): Promise<GetObjectCommandOutput> {
    const response = await getObject({ Bucket: options.bucket, Key: key, Range: `bytes=${range.start}-${range.end - 1}` }, signal)
    if (response.$metadata.httpStatusCode !== 206 || response.ContentRange !== `bytes ${range.start}-${range.end - 1}/${range.totalBytes}`) {
      ;(response.Body as { destroy?(): void } | undefined)?.destroy?.()
      throw new ChunkIntegrityError('Object storage returned a different range')
    }
    return response
  }
  async function run<T>(work: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (closed) throw new Error('Object storage adapter is closed')
    signal?.throwIfAborted()
    if (active >= maximum) {
      if (waiters.size >= queued) throw new StreamError(503, 'STORAGE_BUSY', 'Object storage capacity reached', { 'Retry-After': '1' })
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => { waiters.delete(waiter); waiter.reject(new StreamError(503, 'STORAGE_BUSY', 'Object storage queue deadline exceeded', { 'Retry-After': '1' })) }, Math.min(timeout, 5000))
        const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort) }
        const abort = () => { waiters.delete(waiter); waiter.reject(signal!.reason) }
        const waiter = {
          grant() { cleanup(); active++; resolve() },
          reject(error: unknown) { cleanup(); reject(error) },
        }
        waiters.add(waiter)
        signal?.addEventListener('abort', abort, { once: true })
      })
    } else active++
    const controller = new AbortController()
    controllers.add(controller)
    const abort = () => controller.abort(signal!.reason)
    if (signal?.aborted) abort()
    else signal?.addEventListener('abort', abort, { once: true })
    const timer = setTimeout(() => controller.abort(new Error('Object storage operation deadline exceeded')), timeout)
    try {
      // A waiter may be granted immediately before close(), then resume after it.
      if (closed) throw new Error('Object storage adapter is closed')
      controller.signal.throwIfAborted()
      const result = await work(controller.signal)
      controller.signal.throwIfAborted()
      return result
    }
    finally {
      clearTimeout(timer)
      controllers.delete(controller)
      signal?.removeEventListener('abort', abort)
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
    get(key, maxBytes, signal) {
      if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_CHUNK_BYTES) throw new RangeError('Invalid encoded chunk limit')
      return run(async (abortSignal) => {
        const response = await getObject({ Bucket: options.bucket, Key: key }, abortSignal)
        return readObjectBody(response, maxBytes)
      }, signal)
    },
    getRange(key, range, signal) {
      const { start, end, totalBytes } = range
      validateRange(range)
      return run(async (abortSignal) => {
        const response = await rangeObject(key, { start, end, totalBytes }, abortSignal)
        return readObjectBody(response, end - start, end - start)
      }, signal)
    },
    readRange(key, range, consume, signal) {
      validateRange(range)
      return run(async (abortSignal) => {
        const response = await rangeObject(key, range, abortSignal)
        await consumeRangeBody(response, range.end - range.start, consume)
      }, signal)
    },
    delete(key) {
      if (!IMMUTABLE_KEY.test(key)) throw new TypeError('Collection requires an exact immutable chunk key')
      return run(async (abortSignal) => { await client.send(new DeleteObjectCommand({ Bucket: options.bucket, Key: key }), { abortSignal }) })
    },
    list(prefix, cursor, limit) {
      if (!INVENTORY_PREFIX.test(prefix) || !Number.isSafeInteger(limit) || limit < 1 || limit > 128
        || (cursor !== null && (typeof cursor !== 'string' || !cursor || cursor.length > 8192))) throw new TypeError('Invalid bounded chunk inventory')
      return run(async (abortSignal) => {
        const response = await client.send(new ListObjectsV2Command({ Bucket: options.bucket, Prefix: prefix, MaxKeys: limit,
          ...(cursor === null ? {} : { ContinuationToken: cursor }) }), { abortSignal })
        const objects = (response.Contents ?? []).map((object) => {
          if (!object.Key?.startsWith(prefix) || !Number.isSafeInteger(object.Size) || object.Size! < 0) throw new Error('Invalid object inventory response')
          return { key: object.Key, size: object.Size! }
        })
        if (objects.length > limit || (response.IsTruncated && !response.NextContinuationToken)) throw new Error('Unbounded or incomplete object inventory')
        return { objects, cursor: response.IsTruncated ? response.NextContinuationToken! : null }
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
