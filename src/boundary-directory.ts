import { ChunkIntegrityError } from './chunk-codec.js'
import type { ChunkFrame } from './chunk-codec.js'
import { parseOffset } from './offset.js'

/** Relative positions keep exact frame boundaries without per-frame SQL rows. */
export function boundaryDirectory(start: string, frames: readonly Pick<ChunkFrame, 'start' | 'end' | 'payload'>[]): number[] {
  return directoryFromLengths(
    start,
    frames.map((frame) => ({ start: frame.start, end: frame.end, bytes: frame.payload.length })),
  )
}

export function directoryFromLengths(start: string, frames: readonly { start: string; end: string; bytes: number }[]): number[] {
  const base = parseOffset(start)
  let previous = start
  if (!frames.length || frames.length > 1024) throw new ChunkIntegrityError('Invalid boundary directory size')
  return frames.map((frame) => {
    const end = parseOffset(frame.end),
      before = parseOffset(frame.start)
    const delta = end.position - base.position
    if (
      frame.start !== previous ||
      end.epoch !== base.epoch ||
      before.epoch !== base.epoch ||
      !Number.isSafeInteger(frame.bytes) ||
      frame.bytes < 1 ||
      end.position - before.position !== BigInt(frame.bytes) + 5n ||
      delta < 6n ||
      delta > 2_147_483_647n
    )
      throw new ChunkIntegrityError('Invalid boundary directory frame')
    previous = frame.end
    return Number(delta)
  })
}

export function equalDirectory(left: readonly number[], right: readonly number[]): boolean {
  return left.length === right.length && left.every((value, i) => value === right[i])
}
