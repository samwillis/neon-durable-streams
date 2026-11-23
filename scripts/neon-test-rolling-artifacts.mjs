import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { promisify } from 'node:util'

// Build actual packed consumers, without cloud credentials in build processes.
// Optional archived revision for research comparisons; CI builds only current.
const baseline = process.argv[2]
if (baseline !== undefined && !/^[a-f0-9]{40}$/.test(baseline)) throw new Error('Expected a full archived commit SHA')
const stateFile = '.neon-test-state-rolling-artifacts.json'
const root = resolve(import.meta.dirname, '..')
const run = promisify(execFile)
const buildEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/SECRET|TOKEN|PASSWORD|KEY|DATABASE|^NEON_|^AWS_|^TEST_|^DURABLE_|^NODE_OPTIONS$/.test(key)))
buildEnv.GIT_TERMINAL_PROMPT = '0'
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')
const sourcePaths = ['src', 'migrations', 'package.json', 'pnpm-lock.yaml', 'tsconfig.json', 'tsconfig.build.json', 'README.md', 'NOTICE']
const candidatePaths = [...sourcePaths, 'LICENSE', 'scripts/build.mjs']
let baselinePaths = sourcePaths
const directory = await mkdtemp(join(tmpdir(), 'durable-streams-rolling-'))
try { await writeFile(stateFile, JSON.stringify({ directory }), { mode: 0o600, flag: 'wx' }) }
catch (error) { await rm(directory, { recursive: true, force: true }); throw error }
async function command(cmd, args, cwd = root) {
  return (await run(cmd, args, { cwd, env: buildEnv, timeout: 240_000, maxBuffer: 8 * 1024 * 1024 })).stdout
}
const prior = join(directory, 'baseline-source'), archive = join(directory, 'baseline.tar')
if (baseline) {
  try { await command('git', ['cat-file', '-e', `${baseline}^{commit}`]) }
  catch { await command('git', ['fetch', '--no-tags', '--depth=1', 'https://github.com/samwillis/neon-durable-streams.git', baseline]) }
  try { await command('git', ['cat-file', '-e', `${baseline}:scripts/build.mjs`]); baselinePaths = [...sourcePaths, 'scripts/build.mjs'] } catch { /* Archived revisions may use a direct tsc build. */ }
  await mkdir(prior)
  await command('git', ['archive', '--format=tar', '--output', archive, baseline, ...baselinePaths])
  await command('tar', ['-xf', archive, '-C', prior])
  await command('pnpm', ['install', '--frozen-lockfile', '--prefer-offline', '--ignore-scripts'], prior)
  await command('pnpm', ['build'], prior)
}
const currentCommit = (await command('git', ['rev-parse', 'HEAD'])).trim()
const currentDirty = Boolean((await command('git', ['status', '--porcelain', '--untracked-files=all', '--', ...candidatePaths])).trim())
const candidate = join(directory, 'current-source')
await mkdir(candidate)
for (const path of candidatePaths) { await mkdir(dirname(join(candidate, path)), { recursive: true }); await cp(join(root, path), join(candidate, path), { recursive: true }) }
await command('pnpm', ['install', '--frozen-lockfile', '--prefer-offline', '--ignore-scripts'], candidate)
await command('pnpm', ['build'], candidate)
async function sourceDigest(source) {
  const hash = createHash('sha256')
  async function include(path) {
    const absolute = join(source, path)
    let entries
    try { entries = await readdir(absolute) } catch (error) { if (error.code !== 'ENOTDIR') throw error }
    if (entries) { for (const child of entries.sort()) await include(join(path, child)); return }
    hash.update(path); hash.update('\0'); hash.update(await readFile(absolute)); hash.update('\0')
  }
  for (const path of source === candidate ? candidatePaths : baselinePaths) await include(path)
  return hash.digest('hex')
}
const artifacts = {}
for (const [label, source, commit] of [...(baseline ? [['baseline', prior, baseline]] : []), ['current', candidate, currentCommit]]) {
  const consumer = join(directory, label)
  await mkdir(consumer)
  await command('pnpm', ['pack', '--pack-destination', consumer], source)
  const tarball = (await readdir(consumer)).find((path) => path.endsWith('.tgz'))
  if (!tarball) throw new Error('Rolling fixture package was not produced')
  await writeFile(join(consumer, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies: {
    '@samwillis/durable-streams-neon': `file:./${tarball}`, pg: '8.23.1',
  } }))
  await command('pnpm', ['install', '--prefer-offline', '--ignore-scripts'], consumer)
  artifacts[label] = { commit, sourceDirty: label === 'current' && currentDirty,
    sourceSnapshotSha256: await sourceDigest(source),
    packageRoot: join(consumer, 'node_modules/@samwillis/durable-streams-neon'), consumer,
    tarballSha256: digest(await readFile(join(consumer, tarball))),
    sourceLockSha256: digest(await readFile(join(source, 'pnpm-lock.yaml'))),
    consumerLockSha256: digest(await readFile(join(consumer, 'pnpm-lock.yaml'))) }
}
await writeFile(stateFile, JSON.stringify({ directory, ...(baseline ? { baselineSourceArchiveSha256: digest(await readFile(archive)) } : {}), artifacts }), { mode: 0o600 })
console.log('Prepared independently installed packed package consumer(s).')
