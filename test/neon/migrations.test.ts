import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { afterAll, expect, test } from 'vitest'
import { migrate, migrationPlan } from '../../src/migrations.js'

if (!process.env.TEST_DATABASE_URL_UNPOOLED) throw new Error('Migration qualification requires actual Neon')
const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL_UNPOOLED, max: 1 })
pool.on('error', () => {})
const schemas: string[] = []
const fresh = () => { const schema = `ds_test_${randomUUID().replaceAll('-', '')}`; schemas.push(schema); return schema }
afterAll(async () => {
  for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
  await pool.end()
})

test('development migration histories are rejected without changing existing data', async () => {
  const schema = fresh()
  await pool.query(`CREATE SCHEMA "${schema}";
    CREATE TABLE "${schema}".migrations (id text PRIMARY KEY, checksum text NOT NULL);
    INSERT INTO "${schema}".migrations VALUES ('0001_core','archived');
    CREATE TABLE "${schema}".retained_data (payload text);
    INSERT INTO "${schema}".retained_data VALUES ('keep this')`)
  await expect(migrate(pool, { schema })).rejects.toThrow('fresh schema')
  expect((await pool.query(`SELECT * FROM "${schema}".retained_data`)).rows).toEqual([{ payload: 'keep this' }])
  expect((await pool.query(`SELECT * FROM "${schema}".migrations`)).rows).toEqual([{ id: '0001_core', checksum: 'archived' }])
  expect((await pool.query('SELECT to_regclass($1) AS installation', [`${schema}.installation`])).rows[0].installation).toBeNull()
})

test('a missing migration in the ledger fails before any schema repair or identity change', async () => {
  const schema = fresh()
  await migrate(pool, { schema })
  const identity = (await pool.query(`SELECT * FROM "${schema}".installation`)).rows
  await pool.query(`DELETE FROM "${schema}".migrations WHERE id='0002_streams'`)
  await expect(migrate(pool, { schema })).rejects.toThrow('Unknown or modified migration')
  expect((await pool.query(`SELECT * FROM "${schema}".installation`)).rows).toEqual(identity)
  expect((await pool.query(`SELECT count(*) FROM "${schema}".migrations`)).rows[0].count).toBe('3')
})

test('failed initialization rolls back the whole baseline and a retry installs it once', async () => {
  const schema = fresh()
  await expect(migrate(pool, { schema, environmentId: '' })).rejects.toThrow('Invalid environment identity')
  expect((await pool.query('SELECT 1 FROM pg_namespace WHERE nspname=$1', [schema])).rowCount).toBe(0)
  const plan = await migrationPlan(schema)
  expect(await migrate(pool, { schema })).toEqual(plan.map(({ id }) => id))
  const identity = (await pool.query(`SELECT * FROM "${schema}".installation`)).rows
  expect(await migrate(pool, { schema })).toEqual([])
  expect((await pool.query(`SELECT * FROM "${schema}".installation`)).rows).toEqual(identity)
  expect((await pool.query(`SELECT enabled FROM "${schema}".reclamation_policy`)).rows).toEqual([{ enabled: false }])
  expect((await pool.query(`SELECT max_readers FROM "${schema}".live_capacity`)).rows).toEqual([{ max_readers: 80 }])
})

test('removed partial migration targets fail before creating a schema', async () => {
  const schema = fresh(), options = { schema, target: '0008_compact_prepare' }
  await expect(migrate(pool, options)).rejects.toThrow('Unsupported migration option: target')
  expect((await pool.query('SELECT 1 FROM pg_namespace WHERE nspname=$1', [schema])).rowCount).toBe(0)
})
