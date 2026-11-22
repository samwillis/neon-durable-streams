import { StreamError } from './errors.js'

export const ZERO_OFFSET = '0000000000000000_0000000000000000'
export const MAX_OFFSET_COMPONENT = 9_999_999_999_999_999n
const PATTERN = /^\d{16}_\d{16}$/

export function parseOffset(value: string): { epoch: bigint; position: bigint } {
  if (!PATTERN.test(value)) throw new StreamError(400, 'INVALID_OFFSET', 'Invalid offset')
  return { epoch: BigInt(value.slice(0, 16)), position: BigInt(value.slice(17)) }
}

export function advanceOffset(offset: string, storedBytes: number): string {
  if (!Number.isSafeInteger(storedBytes) || storedBytes < 0) throw new RangeError('Invalid stored length')
  const { epoch, position } = parseOffset(offset)
  const next = position + BigInt(storedBytes) + 5n
  if (next > MAX_OFFSET_COMPONENT) throw new StreamError(413, 'OFFSET_CAPACITY', 'Stream offset capacity reached')
  return `${epoch.toString().padStart(16, '0')}_${next.toString().padStart(16, '0')}`
}
