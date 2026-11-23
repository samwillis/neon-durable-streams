import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { expect, test } from 'vitest'
import { migrate } from '../../src/migrations.js'
import { PostgresStreamStore } from '../../src/store.js'
import { createS3ChunkStorage } from '../../src/s3.js'
import { packBlockDirectory } from '../../src/block-codec.js'
import { ordinalOffset, ZERO_OFFSET } from '../../src/offset.js'
import type { StoredFrame } from '../../src/replay-plan.js'

// Exercise the SQL planner without downloading synthetic objects. Each capped
// ancestor contributes one small block, followed physically by a large tail
// that the child did not inherit. Whole object bytes cannot bound this page.
test('deep capped ancestors are bounded by contributing ordinals, independent of discarded object tails', async () => {
  const env = process.env
  if (!env.TEST_DATABASE_URL_UNPOOLED || !env.AWS_ENDPOINT_URL_S3 || !env.AWS_REGION || !env.NEON_TEST_BUCKET) throw new Error('Planning regression requires actual Neon')
  const pool = new pg.Pool({ connectionString: env.TEST_DATABASE_URL_UNPOOLED, max: 2 })
  pool.on('error', () => {})
  const objects = createS3ChunkStorage({ endpoint: env.AWS_ENDPOINT_URL_S3, region: env.AWS_REGION, bucket: env.NEON_TEST_BUCKET })
  const schema = `ds_test_${randomUUID().replaceAll('-', '')}`, sql = `"${schema}"`
  const store = new PostgresStreamStore(pool, schema, { maxStoredBytes: 4194304, maxPageBytes: 8388608, storage: { objects, blockBytes: 4096 } })
  try {
    await migrate(pool, { schema, storageIdentity: objects.identity })
    const epoch = (await pool.query(`SELECT serving_epoch FROM ${sql}.installation`)).rows[0].serving_epoch
    const ids = Array.from({ length: 41 }, () => randomUUID())
    const streams = ids.map((id, index) => {
      const history = id.replaceAll('-', '')
      const anchor = index ? ordinalOffset(BigInt(index), ids[index - 1]!.replaceAll('-', '')) : ZERO_OFFSET
      return { id, index, source: index ? ids[index - 1] : null, anchor, tail: ordinalOffset(BigInt(index + (index === 40 ? 1 : 2)), history) }
    })
    const chunks = streams.slice(0, 40).map((stream) => {
      const middle = ordinalOffset(BigInt(stream.index + 1), stream.id.replaceAll('-', ''))
      const header = 68 + 112 + stream.anchor.length + middle.length * 2 + stream.tail.length
      const blocks = [
        { firstFrame: 0, frameCount: 1, payloadBytes: 4096, byteStart: header, byteLength: 4099, start: stream.anchor, end: middle, sha256: '0'.repeat(64) },
        { firstFrame: 1, frameCount: 1, payloadBytes: 4194304, byteStart: header + 4099, byteLength: 4194309, start: middle, end: stream.tail, sha256: '0'.repeat(64) },
      ]
      return { id: randomUUID(), stream: stream.id, start: stream.anchor, end: stream.tail, directory: packBlockDirectory(blocks).toString('hex'), bytes: header + 4099 + 4194309 }
    })
    await pool.query(`INSERT INTO ${sql}.paths(tenant_id,path) SELECT 'planning',x.index::text FROM jsonb_to_recordset($1::jsonb) x(index integer)`, [JSON.stringify(streams)])
    await pool.query(`INSERT INTO ${sql}.streams(id,tenant_id,path,content_type,tail_offset,write_epoch,write_history,source_id,fork_offset,fork_depth,edge_active,ref_count,hot_start_offset,hot_frames,hot_bytes)
      SELECT x.id,'planning',x.index::text,'application/octet-stream',x.tail,$2,x.id,x.source,CASE WHEN x.source IS NOT NULL THEN x.anchor END,x.index,x.source IS NOT NULL,
        CASE WHEN x.index<40 THEN 1 ELSE 0 END,CASE WHEN x.index<40 THEN x.tail ELSE x.anchor END,CASE WHEN x.index=40 THEN 1 ELSE 0 END,CASE WHEN x.index=40 THEN 4 ELSE 0 END
      FROM jsonb_to_recordset($1::jsonb) x(id uuid,index integer,source uuid,anchor text,tail text) ORDER BY x.index`, [JSON.stringify(streams), epoch])
    await pool.query(`UPDATE ${sql}.paths p SET active_stream_id=s.id FROM ${sql}.streams s WHERE p.tenant_id=s.tenant_id AND p.path=s.path`)
    await pool.query(`INSERT INTO ${sql}.chunks(id,stream_id,start_offset,end_offset,state,frame_count,payload_bytes,block_directory,object_key,sha256,encoded_bytes,sealed_at)
      SELECT x.id,x.stream,x.start,x.end,'sealed',2,4198400,decode(x.directory,'hex'),'synthetic/'||x.id,repeat('0',64),x.bytes,clock_timestamp()
      FROM jsonb_to_recordset($1::jsonb) x(id uuid,stream uuid,start text,"end" text,directory text,bytes integer)`, [JSON.stringify(chunks)])
    const leaf = streams.at(-1)!
    await pool.query(`INSERT INTO ${sql}.appends(stream_id,start_offset,end_offset,payload,payload_bytes) VALUES($1,$2,$3,$4,4)`, [leaf.id, leaf.anchor, leaf.tail, Buffer.from('leaf')])
    await store.assertReady()
    const internal = store as unknown as { framePlan(client: pg.PoolClient, id: string, start: string): Promise<readonly StoredFrame[]> }
    const plan = await store.transaction({ repeatableRead: true }, (client) => internal.framePlan(client, leaf.id, ZERO_OFFSET))
    expect(plan).toHaveLength(41)
    expect(plan.at(-1)!.end_offset).toBe(leaf.tail)
    expect(plan.reduce((sum, frame) => sum + frame.payload_bytes, 0)).toBe(40 * 4096 + 4)
  } finally {
    store.close(); objects.close()
    await pool.query(`DROP SCHEMA IF EXISTS ${sql} CASCADE`)
    await pool.end()
  }
}, 120000)
