import { spawnSync } from 'node:child_process'
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const temporary = await mkdtemp(join(tmpdir(), 'durable-streams-package-'))
function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', env: process.env })
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed\n${result.stdout}\n${result.stderr}`)
  return result.stdout
}
try {
  run('pnpm', ['build'], root)
  run('pnpm', ['pack', '--pack-destination', temporary], root)
  const tarball = (await readdir(temporary)).find((f) => f.endsWith('.tgz'))
  if (!tarball) throw new Error('No package tarball was produced')
  await writeFile(join(temporary, 'package.json'), JSON.stringify({ private: true, type: 'module',
    dependencies: { '@samwillis/durable-streams-neon': `file:./${tarball}`, pg: '8.23.1' },
    devDependencies: { '@types/pg': '8.23.1', '@types/node': '24.10.1' },
  }))
  run('pnpm', ['install', '--offline', '--ignore-scripts'], temporary)
  await writeFile(join(temporary, 'consumer.ts'), `
import { createDurableStreams, type Principal } from '@samwillis/durable-streams-neon'
import { migrationPlan } from '@samwillis/durable-streams-neon/migrations'
import pg from 'pg'
const pool = new pg.Pool()
const server = createDurableStreams({ postgres: { pool }, auth: {
  authenticate: (): Principal | null => null, authorize: () => false,
} })
const result = await server.fetch(new Request('https://example.test/v1/stream/test'))
if (result.status !== 401) throw new Error('Unexpected unauthenticated behavior')
const plan = await migrationPlan('consumer_schema')
if (plan.length !== 1 || !plan[0]!.sql.includes('"consumer_schema".streams')) throw new Error('Packaged SQL migrations missing')
await server.close()
await pool.end()
`)
  run(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '--strict', '--skipLibCheck', '--target', 'ES2024',
    '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--noEmit', 'consumer.ts'], temporary)
  run(process.execPath, ['consumer.ts'], temporary)
  console.log('Packed consumer: install, public declarations, runtime exports, and SQL assets passed.')
} finally {
  await rm(temporary, { recursive: true, force: true })
}
