import { readFile, unlink } from 'node:fs/promises'
import { createNeonClient } from '@neon/sdk'

// Trusted test tooling. Resolve both exact reserved names after an ambiguous
// restore response, which may create the preserved branch before returning.
const file = '.neon-test-state-inplace.json'
let state
try { state = JSON.parse(await readFile(file, 'utf8')) }
catch (error) { if (error.code === 'ENOENT') process.exit(0); throw error }
const { NEON_API_KEY: apiKey, NEON_PROJECT_ID: projectId } = process.env
const uuid = '[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
if (!apiKey || state.projectId !== projectId || !new RegExp(`^ds-test-inplace-${uuid}$`).test(state.name)
  || state.preservedName !== state.name.replace('inplace-', 'preserved-') || !state.parentId) throw new Error('Refusing unrecognized in-place fixture cleanup')
const neon = createNeonClient({ apiKey, throwOnError: true, waitForReadiness: true })
const names = new Set([state.name, state.preservedName])
for (let wave = 0; wave < 3; wave++) {
  const branches = await neon.branches.list({ projectId }).all()
  const owned = branches.filter((branch) => names.has(branch.name))
  if (!owned.length) break
  let removed = 0
  for (const branch of owned) {
    if (branch.default || branch.protected || branch.id === state.parentId
      || (branch.name === state.name && state.branchId && state.branchId !== branch.id)) throw new Error('In-place fixture identity changed; manual inspection required')
    if (branches.some((child) => child.parent_id === branch.id)) continue
    await neon.branches.delete({ projectId, branchId: branch.id })
    removed++
  }
  if (!removed) throw new Error('Refusing to delete an in-place fixture with remaining children')
}
if ((await neon.branches.list({ projectId }).all()).some((branch) => names.has(branch.name))) throw new Error('In-place fixture cleanup incomplete')
for (const path of ['.env.neon.inplace', '.env.neon.inplace.function', '.neon-test-state-inplace-branch.json', file]) {
  try { await unlink(path) } catch (error) { if (error.code !== 'ENOENT') throw error }
}
console.log('Deleted the recorded in-place restore fixture and preserved branch.')
