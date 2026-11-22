import { lookup } from 'node:dns/promises'
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { isIP } from 'node:net'
import type { LookupFunction } from 'node:net'
import ipaddr from 'ipaddr.js'
import { StreamError } from './errors.js'

export function publicWebhookAddress(address: string): boolean {
  try {
    let parsed = ipaddr.parse(address)
    if (parsed.kind() === 'ipv6' && (parsed as ipaddr.IPv6).isIPv4MappedAddress()) parsed = (parsed as ipaddr.IPv6).toIPv4Address()
    return parsed.range() === 'unicast'
  } catch { return false }
}
export async function webhookTarget(value: string, allowLocalhost: boolean): Promise<{ url: URL; address: string; family: number }> {
  const rejected = () => new StreamError(400, 'WEBHOOK_URL_REJECTED', 'Webhook URL must target an allowed public HTTPS destination')
  let url: URL
  try { url = new URL(value) } catch { throw rejected() }
  const hostname = url.hostname.replace(/^\[|\]$/g, '')
  const localName = hostname === 'localhost' || /^127\.0\.0\.\d{1,3}$/.test(hostname)
  const development = allowLocalhost && localName
  if ((!development && url.protocol !== 'https:') || !['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.hash) throw rejected()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const addresses = isIP(hostname) ? [{ address: hostname, family: isIP(hostname) }] : await Promise.race([
      lookup(hostname, { all: true, verbatim: true }),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(rejected()), 3000) }),
    ])
    if (!addresses.length || addresses.some(({ address }) => !publicWebhookAddress(address) && !(development && (address === '::1' || /^127\.0\.0\.\d{1,3}$/.test(address))))) throw rejected()
    const selected = addresses.find((address) => address.family === 4) ?? addresses[0]!
    return { url, address: selected.address, family: selected.family }
  } finally { clearTimeout(timer) }
}

/** Validate on every attempt, pin DNS through connect, preserve TLS hostname
 * verification, never follow redirects, and bound request/response resources. */
export async function deliverWebhook(url: string, body: Buffer, signature: string, options: { allowLocalhost: boolean; timeoutMs: number; signal?: AbortSignal }): Promise<{ ok: boolean; done: boolean }> {
  const target = await webhookTarget(url, options.allowLocalhost)
  return new Promise((resolve, reject) => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(new Error('Webhook deadline exceeded')), options.timeoutMs)
    const pinned: LookupFunction = (_hostname, lookupOptions, callback) => {
      if (lookupOptions.all) callback(null, [{ address: target.address, family: target.family }])
      else callback(null, target.address, target.family)
    }
    const send = target.url.protocol === 'https:' ? httpsRequest : httpRequest
    const request = send(target.url, { method: 'POST', agent: false, lookup: pinned, signal: options.signal ? AbortSignal.any([controller.signal, options.signal]) : controller.signal,
      headers: { 'Content-Type': 'application/json', 'Content-Length': body.length, 'Webhook-Signature': signature } }, (response) => {
      let length = 0
      const chunks: Buffer[] = []
      response.on('data', (chunk: Buffer) => {
        length += chunk.length
        if (length > 64 * 1024) { response.destroy(new Error('Webhook response exceeds configured limit')); return }
        chunks.push(chunk)
      })
      response.on('error', reject)
      response.on('end', () => {
        let done = false
        try { done = (JSON.parse(Buffer.concat(chunks, length).toString('utf8')) as { done?: unknown })?.done === true } catch { /* empty/non-JSON success means asynchronous processing */ }
        resolve({ ok: response.statusCode !== undefined && response.statusCode >= 200 && response.statusCode < 300, done })
      })
    })
    request.once('error', reject)
    request.once('close', () => clearTimeout(timer))
    request.end(body)
  })
}
