import { encodeBlockObject, decodeBlockObject } from './block-codec.js'
import type { Chunk } from './chunk-types.js'

export { chunkDigest, ChunkIntegrityError, MAX_CHUNK_BYTES } from './chunk-types.js'
export type { ChunkIdentity, ChunkFrame, Chunk } from './chunk-types.js'

export function encodeChunk(chunk: Chunk): Buffer { return encodeBlockObject(chunk).bytes }
export const decodeChunk = decodeBlockObject
