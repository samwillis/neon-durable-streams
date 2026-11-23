import { createHash } from 'node:crypto'

export interface ReplayIdentity {
  readonly generationId: string
  readonly servingEpoch: string
  readonly start: string
  readonly end: string
  readonly contentType: string
  readonly closed: boolean
}

/** The epoch distinguishes rewritten bytes after restoring a prior position. */
export function replayEtag(identity: ReplayIdentity): string {
  const representation = [1, identity.generationId, identity.servingEpoch, identity.start, identity.end, identity.contentType, identity.closed]
  return `"${createHash('sha256').update(JSON.stringify(representation)).digest('hex')}"`
}

export function matchesEtag(condition: string | null | undefined, etag: string): boolean {
  return condition?.split(',').some((candidate) => {
    const value = candidate.trim()
    return value === '*' || value.replace(/^W\//, '') === etag
  }) ?? false
}
