import { expect, test } from 'vitest'
import fc from 'fast-check'
import { decodeReplayRow } from '../../src/replay-plan.js'
import type { ReplayRow } from '../../src/replay-plan.js'
import { advanceOffset, ZERO_OFFSET } from '../../src/offset.js'

const row: ReplayRow = { stream_id: 'stream', start_offset: ZERO_OFFSET, end_offset: advanceOffset(ZERO_OFFSET, 1), payload_bytes: 1,
  payload: Buffer.from('a'), message_ends: null, chunk_id: null, object_key: null, sha256: null, encoded_bytes: null,
  end_deltas: null, block_directory: null, chunk_start: null, first_frame: null, last_frame: null }
const sealed = { ...row, payload: null, chunk_id: 'chunk', object_key: 'key', sha256: '0'.repeat(64), encoded_bytes: 100,
  end_deltas: [6], block_directory: Buffer.from('directory'), chunk_start: ZERO_OFFSET, first_frame: 1, last_frame: 1 }
test('a replay plan has one complete storage representation and rejects incomplete joins', () => {
  expect(decodeReplayRow(row)).toMatchObject({ storage: 'postgres', payload: Buffer.from('a') })
  expect(decodeReplayRow(sealed)).toMatchObject({ storage: 'object', manifest: { id: 'chunk', key: 'key' } })
  for (const key of ['object_key', 'sha256', 'encoded_bytes', 'end_deltas', 'block_directory', 'chunk_start', 'first_frame', 'last_frame'] as const) {
    expect(() => decodeReplayRow({ ...sealed, [key]: null })).toThrow('manifest')
  }
  expect(() => decodeReplayRow({ ...row, payload: null })).toThrow('Postgres')
  expect(() => decodeReplayRow({ ...sealed, payload: Buffer.from('a') })).toThrow('manifest')
})
test('generated corrupt frame lengths cannot enter a Postgres replay plan', () => {
  fc.assert(fc.property(fc.uint8Array({ minLength: 1, maxLength: 4096 }), (bytes) => {
    const payload = Buffer.from(bytes), valid = { ...row, payload, payload_bytes: payload.length, end_offset: advanceOffset(ZERO_OFFSET, payload.length) }
    expect(decodeReplayRow(valid).storage).toBe('postgres')
    expect(() => decodeReplayRow({ ...valid, payload_bytes: payload.length + 1 })).toThrow()
    expect(() => decodeReplayRow({ ...valid, end_offset: advanceOffset(valid.end_offset, 1) })).toThrow()
  }))
})
