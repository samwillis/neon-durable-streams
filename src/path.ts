import { StreamError } from './errors.js'

export function validateSchema(schema: string): string {
  if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema) || schema.startsWith('pg_') || ['public', 'information_schema'].includes(schema)) {
    throw new TypeError('Use a dedicated schema with a lower-case ASCII identifier of at most 63 bytes')
  }
  return `"${schema}"`
}

export function normalizeBasePath(basePath: string): string {
  if (!/^\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_-]+$/.test(basePath)) throw new TypeError('Invalid basePath')
  return basePath
}

/** Reject ambiguous encodings instead of aliasing two stream URLs. */
export function streamPath(url: URL, basePath: string): string | null {
  if (!url.pathname.startsWith(`${basePath}/`)) return null
  const raw = url.pathname.slice(basePath.length + 1)
  const parts = raw.split('/')
  const decoded = parts.map((part) => {
    let value: string
    try { value = decodeURIComponent(part) } catch { throw new StreamError(400, 'INVALID_PATH', 'Invalid stream path') }
    if (!value || value === '.' || value === '..' || /[/\\\u0000-\u001f\u007f]/.test(value) || /%[0-9a-f]{2}/i.test(value)) {
      throw new StreamError(400, 'INVALID_PATH', 'Ambiguous stream path')
    }
    return value
  }).join('/')
  if (Buffer.byteLength(decoded) > 2048) throw new StreamError(414, 'PATH_TOO_LONG', 'Stream path is too long')
  return decoded
}
