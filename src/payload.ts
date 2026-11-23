import { StreamError } from './errors.js'

export interface PreparedPayload {
  readonly bytes: Buffer
  /** Byte ends including each trailing comma. Null means binary mode. */
  readonly messageEnds: readonly number[] | null
}

export function mediaType(value: string): string {
  return value.split(';', 1)[0]!.trim().toLowerCase()
}

/** Validate without round-tripping numeric values through JavaScript numbers. */
export function preparePayload(input: Uint8Array, contentType: string, initial = false): PreparedPayload {
  if (mediaType(contentType) !== 'application/json') return { bytes: Buffer.from(input), messageEnds: null }
  if (input.byteLength === 0 && initial) return { bytes: Buffer.alloc(0), messageEnds: [] }
  let text: string
  let isArray: boolean
  try {
    const decoded = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(input)
    isArray = Array.isArray(JSON.parse(decoded) as unknown)
    text = decoded.trim()
  } catch {
    throw new StreamError(400, 'INVALID_JSON', 'Body must be valid UTF-8 JSON')
  }
  const messages: string[] = []
  if (!isArray) {
    messages.push(text)
  } else if (text.slice(1, -1).trim() !== '') {
    let start = 1
    let depth = 0
    let quoted = false
    let escaped = false
    for (let i = 1; i < text.length - 1; i++) {
      const c = text[i]!
      if (quoted) {
        if (escaped) escaped = false
        else if (c === '\\') escaped = true
        else if (c === '"') quoted = false
      } else if (c === '"') quoted = true
      else if (c === '[' || c === '{') depth++
      else if (c === ']' || c === '}') depth--
      else if (c === ',' && depth === 0) {
        messages.push(text.slice(start, i).trim())
        start = i + 1
      }
    }
    messages.push(text.slice(start, -1).trim())
  }
  if (!messages.length && !initial) throw new StreamError(400, 'EMPTY_JSON_BATCH', 'Empty JSON batches cannot be appended')
  const fragments = messages.map((m) => Buffer.from(`${m},`))
  let length = 0
  const messageEnds = fragments.map((fragment) => (length += fragment.length))
  return { bytes: Buffer.concat(fragments), messageEnds }
}

export function formatPayloads(payloads: readonly Uint8Array[], contentType: string): Buffer<ArrayBuffer> {
  if (mediaType(contentType) !== 'application/json') return Buffer.concat(payloads)
  const length = payloads.reduce((sum, payload) => sum + payload.byteLength, 0)
  if (!length) return Buffer.from('[]')
  const last = payloads.findLast((payload) => payload.byteLength > 0)!
  if (last[last.byteLength - 1] !== 44) throw new Error('Invalid stored JSON frame')
  const bytes = Buffer.allocUnsafe(length + 1)
  bytes[0] = 91
  let position = 1
  for (const payload of payloads) {
    bytes.set(payload, position)
    position += payload.byteLength
  }
  bytes[length] = 93
  return bytes
}
