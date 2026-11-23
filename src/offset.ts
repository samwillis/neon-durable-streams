import { StreamError } from './errors.js'

/** Fixed-width append ordinal followed by the identity of its committed history. */
export const ZERO_OFFSET = '0000000000000000_00000000000000000000000000000000'
export const MAX_OFFSET_COMPONENT = 9_999_999_999_999_999n
const PATTERN = /^\d{16}_[0-9a-f]{32}$/
export function parseOffset(value: string): { ordinal: bigint; history: string } {
  if (!PATTERN.test(value)) throw new StreamError(400, 'INVALID_OFFSET', 'Invalid offset')
  return { ordinal: BigInt(value.slice(0, 16)), history: value.slice(17) }
}
export function ordinalOffset(ordinal: bigint, history: string): string {
  if (ordinal < 0n || ordinal > MAX_OFFSET_COMPONENT) throw new StreamError(413, 'OFFSET_CAPACITY', 'Stream offset capacity reached')
  if (!/^[0-9a-f]{32}$/.test(history)) throw new RangeError('Invalid history identity')
  return `${ordinal.toString().padStart(16, '0')}_${history}`
}
export function advanceOffset(offset: string, history = parseOffset(offset).history): string {
  return ordinalOffset(parseOffset(offset).ordinal + 1n, history)
}
/** Payload size never changes the ordinal. A transition may enter a new history. */
export function isSuccessor(start: string, end: string): boolean {
  return parseOffset(end).ordinal === parseOffset(start).ordinal + 1n
}
