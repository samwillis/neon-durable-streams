import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { cp, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, relative, resolve, sep } from 'node:path'
import { promisify } from 'node:util'
import { zipSync } from 'fflate'

// Build without management or data credentials. Ship the actual unchanged
// Vitest fixture, upstream package, and frozen dependency graph to Node 24.
export async function buildConformanceArtifact() {
  const root = resolve(import.meta.dirname, '..')
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'ds-conformance-build-')))
  const run = promisify(execFile)
  const buildEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/SECRET|TOKEN|PASSWORD|KEY|DATABASE|^NEON_|^AWS_|^TEST_|^DURABLE_|^NODE_OPTIONS$/.test(key)))
  const sourcePaths = ['src', 'migrations', 'test/conformance', 'test/fixtures/postgres-limits.ts', 'vitest.config.ts', 'package.json', 'pnpm-lock.yaml', 'tsconfig.json', 'LICENSE', 'NOTICE']
  try {
    for (const path of sourcePaths) { await mkdir(dirname(join(directory, path)), { recursive: true }); await cp(join(root, path), join(directory, path), { recursive: true }) }
    // Reject a different runtime rather than use the build host's native code.
    await writeFile(join(directory, 'pnpm-workspace.yaml'), 'supportedArchitectures:\n  os: [linux]\n  cpu: [arm64, wasm32]\n  libc: [glibc]\n')
    await run('pnpm', ['install', '--frozen-lockfile', '--ignore-scripts', '--prefer-offline'], {
      cwd: directory, env: buildEnv, timeout: 240_000, maxBuffer: 8 * 1024 * 1024,
    })
    const files = {}, links = {}, executable = [], included = new Set()
    async function include(path) {
      const absolute = join(directory, path), stat = await lstat(absolute)
      if (stat.isSymbolicLink()) {
        const target = relative(directory, await realpath(absolute))
        if (target.startsWith(`..${sep}`) || target === '..') throw new Error('Dependency symlink escaped artifact')
        links[path] = relative(dirname(path), target)
        await include(target)
      } else if (stat.isDirectory()) {
        for (const entry of (await readdir(absolute)).sort()) if (entry !== 'node_modules') await include(join(path, entry))
      } else if (stat.isFile()) { files[path] = new Uint8Array(await readFile(absolute)); if (stat.mode & 0o111) executable.push(path) }
    }
    async function dependency(name, from, optional = false) {
      // The native Rolldown addon exceeds the executable source-size budget.
      // Its locked WASI variant changes only the compiler, never test clocks.
      if (name.startsWith('@rolldown/binding-') && name !== '@rolldown/binding-wasm32-wasi') return
      if (name.endsWith('-musl')) return
      let parent = from
      for (;;) {
        const candidate = join(parent, 'node_modules', name)
        let actual
        try { actual = await realpath(candidate) } catch (error) { if (error.code !== 'ENOENT') throw error }
        if (actual) {
          if (candidate !== actual) links[relative(directory, candidate)] = relative(dirname(candidate), actual)
          if (included.has(actual)) return
          included.add(actual)
          await include(relative(directory, actual))
          const pkg = JSON.parse(await readFile(join(actual, 'package.json'), 'utf8'))
          for (const child of Object.keys(pkg.dependencies ?? {})) await dependency(child, actual)
          for (const child of new Set([...Object.keys(pkg.optionalDependencies ?? {}), ...Object.keys(pkg.peerDependencies ?? {})])) await dependency(child, actual, true)
          return
        }
        const next = dirname(parent)
        if (parent === directory || next === parent || !parent.startsWith(directory)) break
        parent = next
      }
      if (!optional) throw new Error(`Missing frozen dependency ${name}`)
    }
    for (const path of sourcePaths) await include(path)
    for (const name of ['vitest', 'pg', '@durable-streams/server-conformance-tests', '@aws-sdk/client-s3', '@neon/realtime', 'ws', 'ipaddr.js']) await dependency(name, directory)
    if (![...included].some((path) => path.endsWith('/@rolldown/binding-wasm32-wasi'))) throw new Error('Frozen WASI test compiler missing')
    const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex')
    const native = {}, nativeFiles = {}
    // /tmp is writable but noexec on Functions. ELF executables and native
    // addons must be deployed on the executable code mount, not unpacked there.
    for (const [path, bytes] of Object.entries(files)) if (bytes[0] === 0x7f && bytes[1] === 0x45 && bytes[2] === 0x4c && bytes[3] === 0x46) {
      const name = `native/${sha256(bytes)}.${path.endsWith('.node') ? 'node' : 'bin'}`
      native[path] = name; nativeFiles[name] = [bytes, { os: 3, attrs: 0o100755 << 16 }]; delete files[path]
    }
    files['.artifact-links.json'] = new TextEncoder().encode(JSON.stringify(links))
    files['.artifact-executable.json'] = new TextEncoder().encode(JSON.stringify(executable.filter((path) => !native[path])))
    files['.artifact-native.json'] = new TextEncoder().encode(JSON.stringify(native))
    const payload = zipSync(files, { level: 6 })
    const sourceHash = createHash('sha256')
    for (const path of Object.keys(files).filter((path) => !path.startsWith('node_modules/')).sort()) sourceHash.update(path).update('\0').update(files[path]).update('\0')
    const { stdout: commit } = await run('git', ['rev-parse', 'HEAD'], { cwd: root, env: buildEnv })
    const { stdout: dirty } = await run('git', ['status', '--porcelain', '--untracked-files=all', '--', ...sourcePaths, 'scripts/neon-conformance-artifact.mjs', 'scripts/neon-test-conformance.mjs', 'test/fixtures/conformance-function.mjs'], { cwd: root, env: buildEnv })
    const upstream = await readFile(join(directory, 'node_modules/@durable-streams/server-conformance-tests/dist/index.js'))
    return { payload, nativeFiles, manifest: { target: 'linux/arm64/glibc', testCompiler: 'rolldown-wasi', nativeFiles: Object.keys(nativeFiles), sourceCommit: commit.trim(), sourceDirty: Boolean(dirty.trim()), sourceSnapshotSha256: sourceHash.digest('hex'),
      payloadSha256: sha256(payload), payloadBytes: payload.length, expandedBytes: Object.values(files).reduce((sum, bytes) => sum + bytes.length, 0),
      lockSha256: sha256(files['pnpm-lock.yaml']), fixtureSha256: sha256(files['test/conformance/server.test.ts']),
      upstreamVersion: '0.3.7', upstreamSha256: sha256(upstream), subscriptions: true,
      reclamation: { enabled: true, recovery: 'provider-owned' }, expectedTests: 338 } }
  } finally { await rm(directory, { recursive: true, force: true }) }
}
