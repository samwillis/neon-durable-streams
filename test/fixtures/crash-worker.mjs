// Test-only process barriers; never imported into the library or its HTTP routes.
import pg from 'pg'
import { createDurableStreams } from '../../dist/index.js'
import { transaction } from '../../dist/transaction.js'

const schema = process.env.TEST_SCHEMA
if (!/^ds_test_[a-f0-9]+$/.test(schema ?? '') || !process.env.TEST_DATABASE_URL) throw new Error('Disposable test configuration required')
const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL, max: 1 })
process.on('message', () => {}) // Keep the IPC channel alive until the parent kills us.
if (process.argv[2] === 'committed') {
  const server = createDurableStreams({ postgres: { pool, schema }, auth: {
    authenticate: () => ({ tenantId: 'tenant-a', subject: 'crash-fixture' }), authorize: () => true,
  } })
  const response = await server.fetch(new Request('https://example.test/v1/stream/crash-committed', {
    method: 'PUT', headers: { 'Content-Type': 'text/plain' }, body: 'acknowledged bytes',
  }))
  if (response.status !== 201) throw new Error('Append was not acknowledged')
  process.send({ ready: true })
} else {
  await transaction(pool, {}, async (client) => {
    await client.query(`INSERT INTO "${schema}".paths (tenant_id, path) VALUES ('tenant-a', 'crash-uncommitted')`)
    process.send({ ready: true })
    await new Promise(() => {})
  })
}
