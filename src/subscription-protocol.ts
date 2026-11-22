import { createHash } from 'node:crypto'
import { StreamError } from './errors.js'

export interface SubscriptionConfig {
  readonly type: 'webhook' | 'pull-wake'
  readonly pattern: string | null
  readonly streams: readonly string[]
  readonly webhook: { readonly url: string } | null
  readonly wake_stream: string | null
  readonly lease_ttl_ms: number
  readonly description: string | null
}
export function relativeStreamPath(input: unknown): string {
  if (typeof input !== 'string' || !input.isWellFormed() || Buffer.byteLength(input) > 2048 || input.split('/').some((part) => !part || part === '.' || part === '..' || /[\\\u0000-\u001f\u007f]/.test(part) || /%[a-f0-9]{2}/i.test(part)) || input.split('/')[0] === '__ds') throw new StreamError(400, 'INVALID_STREAM_PATH', 'Expected a canonical stream-root-relative application path')
  return input
}
export function globExpression(pattern: string): string {
  if (!pattern || !pattern.isWellFormed() || Buffer.byteLength(pattern) > 2048 || /[\\\u0000-\u001f\u007f]/.test(pattern)) throw new StreamError(400, 'INVALID_PATTERN', 'Invalid stream pattern')
  const segments = pattern.split('/')
  if (segments.some((part) => !part || part === '.' || part === '..' || (part.includes('**') && part !== '**'))) throw new StreamError(400, 'INVALID_PATTERN', 'Invalid stream pattern segments')
  const parts = segments.filter((part, i) => part !== '**' || segments[i - 1] !== '**')
  let expression = '^'
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]!
    if (part === '**') expression += i === 0 ? (parts.length === 1 ? '.*' : '(?:[^/]+/)*') : i === parts.length - 1 ? '(?:/[^/]+)*' : '(?:/[^/]+)*/'
    else {
      if (i && parts[i - 1] !== '**') expression += '/'
      expression += part.split('*').map((literal) => literal.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*')
    }
  }
  return `${expression}$`
}

/** Bounded dynamic programming avoids JavaScript regex backtracking on
 * caller-provided wildcard patterns. PostgreSQL uses the equivalent ARE. */
export function globMatches(pattern: string, path: string): boolean {
  const patterns = pattern.split('/'), paths = path.split('/')
  let previous = new Uint8Array(paths.length + 1)
  previous[0] = 1
  const segment = (pattern: string, value: string) => {
    let p = 0, v = 0, star = -1, checkpoint = 0
    while (v < value.length) {
      if (p < pattern.length && pattern[p] !== '*' && pattern[p] === value[v]) { p++; v++ }
      else if (pattern[p] === '*') { star = p++; checkpoint = v }
      else if (star >= 0) { p = star + 1; v = ++checkpoint }
      else return false
    }
    while (pattern[p] === '*') p++
    return p === pattern.length
  }
  for (const part of patterns) {
    const current = new Uint8Array(paths.length + 1)
    if (part === '**') {
      current[0] = previous[0]!
      for (let i = 1; i <= paths.length; i++) current[i] = previous[i]! || current[i - 1]!
    } else for (let i = 1; i <= paths.length; i++) if (previous[i - 1] && segment(part, paths[i - 1]!)) current[i] = 1
    previous = current
  }
  return previous[paths.length] === 1
}
export function normalizeSubscription(input: unknown): { config: SubscriptionConfig; hash: string; regex: string | null } {
  const invalid = () => new StreamError(400, 'INVALID_SUBSCRIPTION', 'Invalid subscription configuration')
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw invalid()
  const raw = input as Record<string, unknown>
  if (!['webhook', 'pull-wake'].includes(String(raw.type))) throw invalid()
  const pattern = raw.pattern === undefined ? null : typeof raw.pattern === 'string' ? raw.pattern : (() => { throw invalid() })()
  const regex = pattern === null ? null : globExpression(pattern)
  if (raw.streams !== undefined && !Array.isArray(raw.streams)) throw invalid()
  const streams = [...new Set((raw.streams as unknown[] | undefined ?? []).map(relativeStreamPath))].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
  if (pattern === null && !streams.length) throw invalid()
  const ttl = raw.lease_ttl_ms ?? 30_000
  if (typeof ttl !== 'number' || !Number.isSafeInteger(ttl) || ttl < 1000 || ttl > 600_000) throw invalid()
  if (raw.description !== undefined && (typeof raw.description !== 'string' || !raw.description.isWellFormed() || Buffer.byteLength(raw.description) > 4096 || raw.description.includes('\0'))) throw invalid()
  let webhook: { url: string } | null = null
  let wakeStream: string | null = null
  if (raw.type === 'webhook') {
    if (!raw.webhook || typeof raw.webhook !== 'object' || typeof (raw.webhook as Record<string, unknown>).url !== 'string') throw invalid()
    let url: URL
    try { url = new URL((raw.webhook as { url: string }).url) } catch { throw new StreamError(400, 'WEBHOOK_URL_REJECTED', 'Invalid webhook URL') }
    webhook = { url: url.href }
    if (raw.wake_stream !== undefined && raw.wake_stream !== null) throw invalid()
  } else {
    wakeStream = relativeStreamPath(raw.wake_stream)
    if (raw.webhook !== undefined && raw.webhook !== null) throw invalid()
    if (streams.includes(wakeStream) || (pattern && globMatches(pattern, wakeStream))) throw new StreamError(400, 'SELF_WAKE', 'A pull-wake stream cannot subscribe to its own wake channel')
  }
  const config: SubscriptionConfig = { type: raw.type as SubscriptionConfig['type'], pattern, streams, webhook, wake_stream: wakeStream, lease_ttl_ms: ttl, description: raw.description as string | undefined ?? null }
  return { config, regex, hash: createHash('sha256').update(JSON.stringify(config)).digest('hex') }
}
export interface ControlRoute { readonly name?: string; readonly action: 'jwks' | 'subscription' | 'streams' | 'member' | 'claim' | 'ack' | 'release' | 'callback' | 'unknown'; readonly member?: string; readonly path: string }
export function subscriptionRoute(url: URL, basePath: string): ControlRoute | null {
  const root = `${basePath}/__ds`
  if (url.pathname !== root && !url.pathname.startsWith(`${root}/`)) return null
  const parts = url.pathname.slice(root.length + 1).split('/')
  const unknown: ControlRoute = { action: 'unknown', path: '__ds' }
  if (parts.length === 1 && parts[0] === 'jwks.json') return { action: 'jwks', path: '__ds/jwks.json' }
  if (parts[0] !== 'subscriptions' || !parts[1]) return unknown
  let name: string
  try { name = decodeURIComponent(parts[1]) } catch { throw new StreamError(400, 'INVALID_SUBSCRIPTION_ID', 'Invalid subscription identifier') }
  if (!name || Buffer.byteLength(name) > 512 || /[/\\\u0000-\u001f\u007f]/.test(name)) throw new StreamError(400, 'INVALID_SUBSCRIPTION_ID', 'Invalid subscription identifier')
  const path = `__ds/subscriptions/${name}`
  if (parts.length === 2) return { name, action: 'subscription', path }
  if (parts[2] === 'streams' && parts.length > 3) {
    let member: string
    try { member = relativeStreamPath(decodeURIComponent(parts.slice(3).join('/'))) } catch { throw new StreamError(400, 'INVALID_STREAM_PATH', 'Invalid explicit stream path') }
    return { name, action: 'member', member, path: `${path}/streams/${member}` }
  }
  if (parts.length === 3 && ['streams', 'claim', 'ack', 'release', 'callback'].includes(parts[2]!)) return { name, action: parts[2] as ControlRoute['action'], path: `${path}/${parts[2]}` }
  return unknown
}
