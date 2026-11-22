import { describe, expect, test } from 'vitest'
import fc from 'fast-check'
import { advanceOffset, parseOffset, ZERO_OFFSET } from '../../src/offset.js'
import { formatPayloads, preparePayload } from '../../src/payload.js'
import { streamPath } from '../../src/path.js'

describe('persistent offset contract', () => {
  test('increments exactly above the JavaScript safe integer range', () => {
    expect(advanceOffset('0000000000000000_9007199254740992', 1)).toBe('0000000000000000_9007199254740998')
    expect(() => advanceOffset('0000000000000000_9999999999999998', 1)).toThrow('capacity')
  })
  test('offset order and payload accounting survive arbitrary append batches', () => {
    fc.assert(fc.property(fc.array(fc.integer({ min: 1, max: 1_000_000 }), { maxLength: 200 }), (sizes) => {
      let offset = ZERO_OFFSET
      let bytes = 0n
      for (const size of sizes) {
        const next = advanceOffset(offset, size)
        bytes += BigInt(size) + 5n
        expect(next > offset).toBe(true)
        expect(parseOffset(next).position).toBe(bytes)
        offset = next
      }
    }))
  })
  test.each(['', 'now', '-1', '1_2', '0000000000000000_0000000000000000x'])('sentinels/invalid tokens are not stored offsets: %j', (value) => {
    expect(() => parseOffset(value)).toThrow()
  })
})

describe('JSON storage contract', () => {
  test('preserves number lexemes, flattens once, and records UTF-8 message ends', () => {
    const body = Buffer.from('[9007199254740993,1e400,{"emoji":"🍋","comma":","},[1,2]]')
    const prepared = preparePayload(body, 'Application/JSON; charset=utf-8')
    expect(prepared.bytes.toString()).toBe('9007199254740993,1e400,{"emoji":"🍋","comma":","},[1,2],')
    expect(prepared.messageEnds).toHaveLength(4)
    const ends = prepared.messageEnds!
    expect(prepared.bytes.subarray(ends[1], ends[2]).toString()).toBe('{"emoji":"🍋","comma":","},')
    expect(formatPayloads([prepared.bytes], 'application/json').toString()).toBe(body.toString())
  })
  test('replays arbitrary valid JSON values without changing their meaning', () => {
    fc.assert(fc.property(fc.jsonValue(), (value) => {
      const input = Buffer.from(JSON.stringify(value))
      const wireValue: unknown = JSON.parse(input.toString())
      if (Array.isArray(value) && !value.length) return
      const prepared = preparePayload(input, 'application/json')
      expect(JSON.parse(formatPayloads([prepared.bytes], 'application/json').toString())).toEqual(Array.isArray(wireValue) ? wireValue : [wireValue])
    }))
  })
  test('empty create differs from empty append; invalid UTF-8 is rejected', () => {
    expect(preparePayload(Buffer.from('[]'), 'application/json', true).bytes.length).toBe(0)
    expect(() => preparePayload(Buffer.from('[]'), 'application/json')).toThrow('Empty')
    expect(() => preparePayload(Uint8Array.of(34, 255, 34), 'application/json')).toThrow('UTF-8')
    expect(() => preparePayload(Buffer.from('[1,]'), 'application/json')).toThrow()
    expect(() => preparePayload(Buffer.from('\u00a0{}'), 'application/json')).toThrow()
    expect(preparePayload(Buffer.from('-0'), 'application/json').bytes.toString()).toBe('-0,')
  })
  test('binary bytes have no text conversion', () => {
    fc.assert(fc.property(fc.uint8Array(), (body) => {
      expect(formatPayloads([preparePayload(body, 'application/octet-stream').bytes], 'application/octet-stream')).toEqual(Buffer.from(body))
    }))
  })
})

describe('mount and path identity', () => {
  test('decodes once, preserves Unicode, and respects the mount boundary', () => {
    expect(streamPath(new URL('https://example.test/api/streams/caf%C3%A9'), '/api/streams')).toBe('café')
    expect(streamPath(new URL('https://example.test/api/streamsx/a'), '/api/streams')).toBeNull()
  })
  test.each(['a%2Fb', 'a%5Cb', 'a//b', '%252f', '%00', '%zz'])('rejects ambiguous paths: %s', (path) => {
    expect(() => streamPath(new URL(`https://example.test/api/streams/${path}`), '/api/streams')).toThrow()
  })
})
