// Deterministic inputs shared by the deployed writer and independent reader.
// The seed and frame index determine every byte, including large-number JSON.
import { createHash } from 'node:crypto'

export const replayProfiles = {
  tiny: { frames: 2048, bytes: 32, contentType: 'application/octet-stream' },
  binary: { frames: 256, bytes: 32768, contentType: 'application/octet-stream' },
  large: { frames: 4, bytes: 1048576, contentType: 'application/octet-stream' },
  json: { frames: 256, bytes: 512, contentType: 'application/json' },
  mixed: { frames: 128, bytes: 0, contentType: 'application/octet-stream' },
}

export function replayFrame(profile, index, seed = 20261123) {
  const options = replayProfiles[profile]
  if (!options || !Number.isSafeInteger(index) || index < 0 || index >= 100000) throw new Error('Invalid replay frame')
  const stamp = createHash('sha256').update(`${seed}/${profile}/${index}`).digest()
  if (profile === 'json') return Buffer.from(`{"index":${index},"number":9007199254740993123456789,"text":"${stamp.toString('hex').repeat(6)}"}`)
  const length = options.bytes || [32, 1024, 65536, 131072][index % 4]
  const bytes = Buffer.alloc(length)
  for (let position = 0; position < length; position += stamp.length) stamp.copy(bytes, position, 0, Math.min(stamp.length, length - position))
  return bytes
}

export function expectedReplay(profile, from, through, seed = 20261123) {
  const frames = Array.from({ length: through - from }, (_, index) => replayFrame(profile, from + index, seed))
  if (replayProfiles[profile].contentType === 'application/json') return Buffer.from(`[${frames.map((frame) => frame.toString()).join(',')}]`)
  return Buffer.concat(frames)
}
