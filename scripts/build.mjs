import { rm } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'

// TypeScript does not remove outputs belonging to deleted source files.
// Start clean so removed codecs cannot remain in a locally packed artifact.
await rm(new URL('../dist/', import.meta.url), { recursive: true, force: true })
execFileSync('tsc', ['-p', 'tsconfig.build.json'], { stdio: 'inherit' })
