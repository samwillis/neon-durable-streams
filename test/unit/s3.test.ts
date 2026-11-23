import { createServer } from 'node:http'
import { once } from 'node:events'
import type { AddressInfo } from 'node:net'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { createS3ChunkStorage } from '../../src/s3.js'
import { ChunkIntegrityError } from '../../src/chunk-codec.js'

const payload = Buffer.alloc(128 * 1024, 123)
const origin = createServer((request, response) => {
  if (request.url?.endsWith('/missing-length')) {
    response.write(payload.subarray(0, 1024))
    response.end(payload.subarray(1024))
    return
  }
  response.setHeader('Content-Length', payload.length)
  response.write(payload.subarray(0, 1024))
  response.end(payload.subarray(1024))
})
let storage: ReturnType<typeof createS3ChunkStorage>
beforeAll(async () => {
  origin.listen(0, '127.0.0.1')
  await once(origin, 'listening')
  storage = createS3ChunkStorage({ endpoint: `http://127.0.0.1:${(origin.address() as AddressInfo).port}`, allowHttp: true,
    region: 'fixture', bucket: 'fixture', credentials: { accessKeyId: 'fixture', secretAccessKey: 'fixture' }, timeoutMs: 2000 })
})
afterAll(async () => {
  storage?.close()
  origin.closeAllConnections()
  await new Promise<void>((resolve, reject) => { origin.close((error) => { if (error) reject(error); else resolve() }) })
})

test('fragmented S3 downloads return exact owned bytes within the declared bound', async () => {
  const first = await storage.get('object', payload.length)
  expect(first).toEqual(payload)
  first.fill(0)
  expect(await storage.get('object', payload.length)).toEqual(payload)
})

test('chunked S3 downloads without Content-Length return an exact bounded buffer', async () => {
  const bytes = await storage.get('missing-length', payload.length + 1024)
  expect(bytes.length).toBe(payload.length)
  expect(bytes).toEqual(payload)
})

test('S3 downloads reject oversized declared and chunked bodies', async () => {
  await expect(storage.get('object', 1024)).rejects.toBeInstanceOf(ChunkIntegrityError)
  await expect(storage.get('missing-length', 1024)).rejects.toBeInstanceOf(ChunkIntegrityError)
})
