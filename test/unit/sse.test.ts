import { expect, test } from 'vitest'
import fc from 'fast-check'
import { encodeSsePage } from '../../src/sse.js'

const control = { streamNextOffset: 'checkpoint', streamClosed: true }
function verify(bytes: Buffer, binary: boolean) {
  const chunks = [...encodeSsePage(bytes, true, binary, control)]
  const data = binary ? bytes.toString('base64') : bytes.toString('utf8').replace(/\r\n|\r/g, '\n').replace(/\n/g, '\ndata:')
  expect(Buffer.concat(chunks).toString()).toBe(`event: data\ndata:${data}\n\nevent: control\ndata:${JSON.stringify(control)}\n\n`)
  expect(Math.max(...chunks.map((chunk) => chunk.byteLength))).toBeLessThanOrEqual(65536)
}
test('SSE fragments preserve arbitrary binary and invalid UTF-8 payloads', () => {
  fc.assert(fc.property(fc.uint8Array({ maxLength: 180000 }), (value) => { verify(Buffer.from(value), false); verify(Buffer.from(value), true) }), { numRuns: 100 })
})
test('SSE handles CRLF, BOM and Unicode across fragment boundaries', () => {
  for (const prefix of [8190, 8191, 8192, 8193, 49151, 49152]) {
    for (const tail of ['\r\n\r\n', '🐘\r\n\n', '\r\r\n', 'é\n', '\ufeffx\n']) verify(Buffer.from(`${'x'.repeat(prefix)}${tail}${'😀'.repeat(10000)}`), false)
  }
  verify(Buffer.from('\ufeff[9007199254740993,1e400,-0,"🐘"]\r\n'), false)
  expect(Buffer.concat([...encodeSsePage(Buffer.alloc(0), false, true, control)]).toString()).toBe(`event: control\ndata:${JSON.stringify(control)}\n\n`)
})
