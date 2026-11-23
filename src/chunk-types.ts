import { createHash } from 'node:crypto'

export const MAX_CHUNK_BYTES = 128 * 1024 * 1024
export interface ChunkIdentity { readonly installationId: string; readonly streamId: string; readonly chunkId: string }
export interface ChunkFrame {
  readonly start: string
  readonly end: string
  readonly payload: Buffer
  readonly messageEnds: readonly number[] | null
}
export interface Chunk extends ChunkIdentity { readonly frames: readonly ChunkFrame[] }
export class ChunkIntegrityError extends Error { override readonly name = 'ChunkIntegrityError' }
export function chunkDigest(bytes: Uint8Array): string { return createHash('sha256').update(bytes).digest('hex') }
