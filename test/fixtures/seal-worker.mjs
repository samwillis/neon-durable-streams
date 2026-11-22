// Test-only process barrier after a real Neon Object Storage publication commit.
import pg from 'pg'
import { createDurableStreams } from '../../dist/index.js'
import { createS3ChunkStorage } from '../../dist/s3.js'
const schema = process.env.TEST_SCHEMA
if (!/^ds_test_[a-f0-9]+$/.test(schema ?? '') || !process.env.TEST_DATABASE_URL || !process.env.AWS_ENDPOINT_URL_S3) throw new Error('Isolated Neon test configuration required')
const objects = createS3ChunkStorage({ endpoint: process.env.AWS_ENDPOINT_URL_S3, region: process.env.AWS_REGION, bucket: process.env.NEON_TEST_BUCKET })
const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL, max: 2 })
const handler = createDurableStreams({ postgres: { pool, schema }, storage: { objects }, auth: {
  authenticate: () => null, authorize: () => false,
} })
process.on('message', () => {})
await handler.maintain({ maxChunks: 16 })
process.send({ ready: true })
