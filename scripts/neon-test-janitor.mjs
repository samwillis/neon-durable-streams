import { createNeonClient } from '@neon/sdk'

// Trusted CI tooling only; never imported by the library or Function runtime.
const { NEON_API_KEY: apiKey, NEON_PROJECT_ID: projectId } = process.env
if (!apiKey || !projectId) throw new Error('Project-scoped Neon credentials required')
const neon = createNeonClient({ apiKey, throwOnError: true, waitForReadiness: true })
const cutoff = Date.now() - 26 * 3600_000
const branches = await neon.branches.list({ projectId }).all()
// These names are reserved for this repository's disposable test tooling.
const owned = /^ds-test-(?:\d+(?:-\d+)?|recovery-[a-f0-9-]{36})$/
const stale = branches.filter((branch) => owned.test(branch.name) && !branch.default && !branch.protected
  && Date.parse(branch.created_at) < cutoff).sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))
let removed = 0
const remaining = new Set(branches.map((branch) => branch.id))
for (const branch of stale) {
  // Refuse any parent with a live/unrecognized child, even if its name matches.
  if (branches.some((child) => child.parent_id === branch.id && remaining.has(child.id))) continue
  await neon.branches.delete({ projectId, branchId: branch.id })
  remaining.delete(branch.id)
  removed++
}
console.log(`Deleted ${removed} stale disposable test branches.`)
