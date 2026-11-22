import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, unlink, writeFile } from 'node:fs/promises'
import { createNeonClient } from '@neon/sdk'
import pg from 'pg'
import { ChunkNotFoundError, createDurableStreams } from '../dist/index.js'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../dist/operations.js'
import { createS3ChunkStorage } from '../dist/s3.js'
import { parseSubscriptionKeyring } from '../dist/subscriptions.js'

// A companion to the real-time collection soak. It observes its checkpoint;
// never changes parent policy, deadlines, stream histories, or serving epoch.
const env = process.env, state = JSON.parse(await readFile(env.NEON_TEST_STATE_FILE ?? '.neon-test-state.json','utf8'))
if (!env.NEON_API_KEY || state.projectId!==env.NEON_PROJECT_ID || state.branchId!==env.NEON_TEST_BRANCH_ID || state.branchId!==env.TEST_FUNCTION_BRANCH_ID
  || !state.name.startsWith('ds-test-') || !/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA??'')) throw new Error('Recorded isolated parent required')
const projectId=state.projectId, parentId=state.branchId, schema=env.TEST_FUNCTION_SCHEMA, id=randomUUID(), prefix=`gc-recovery-${id}`
const cleanupFile='.neon-test-state-reclamation-recovery.json', checkpointFile='.neon-test-state-reclamation-checkpoint.json'
const neon=createNeonClient({ apiKey:env.NEON_API_KEY,throwOnError:true,waitForReadiness:true,wait:{ timeoutMs:180_000 } })
const parentPool=new pg.Pool({ connectionString:env.TEST_DATABASE_URL_UNPOOLED,max:2,connectionTimeoutMillis:20_000,query_timeout:30_000 })
parentPool.on('error',()=>{})
const parentObjects=createS3ChunkStorage({ endpoint:env.AWS_ENDPOINT_URL_S3,region:env.AWS_REGION,bucket:env.NEON_TEST_BUCKET })
const payload=Buffer.alloc(65536,0x52); payload.write(prefix)
const digest=(bytes)=>createHash('sha256').update(bytes).digest('hex')
const tuple={ 'Producer-Id':prefix,'Producer-Epoch':'0','Producer-Seq':'0' }
const request=(path,method='GET',body,headers={})=>fetch(new URL(`v1/stream/${prefix}/${path}`,env.TEST_FUNCTION_URL),{ method,
  headers:{ Authorization:`Bearer ${env.TEST_FUNCTION_API_KEY}`,'Content-Type':'application/octet-stream',...headers },
  ...(body===undefined?{}:{body}),signal:AbortSignal.timeout(30_000) })
const expectStatus=async(response,status)=>{ if(response.status!==status){ await response.arrayBuffer(); throw new Error(`Witness HTTP ${response.status}; expected ${status}`) } return response }
let childId,childPool,childObjects,recovered,cleanup,checkpoint,restoreStartedAt,checkpointAgeMs,verified=0,report,ownsCleanup=false
try {
  const branch=await neon.branches.get({projectId,branchId:parentId})
  if(branch.default||branch.protected||branch.name!==state.name) throw new Error('Parent identity changed')
  cleanup={projectId,parentId,parentName:state.name,parentExpiry:branch.expires_at??null,childName:`ds-test-gcrecovery-${id}`,schema,operationId:randomUUID()}
  await writeFile(cleanupFile,JSON.stringify(cleanup),{mode:0o600,flag:'wx'})
  ownsCleanup=true
  await (await expectStatus(await request('binary','PUT'),201)).arrayBuffer()
  await (await expectStatus(await request('binary','POST',payload,tuple),200)).arrayBuffer()
  await (await expectStatus(await request('fork','PUT',undefined,{'Stream-Forked-From':`/v1/stream/${prefix}/binary`}),201)).arrayBuffer()
  await (await expectStatus(await request('json','PUT',`[{"marker":"${prefix}","n":9007199254740993}]`,{'Content-Type':'application/json'}),201)).arrayBuffer()
  const maintenance=await fetch(new URL('_maintenance',env.TEST_FUNCTION_URL),{method:'POST',headers:{'x-ds-maintenance-key':env.TEST_FUNCTION_MAINTENANCE_KEY},signal:AbortSignal.timeout(30_000)})
  if(!maintenance.ok)throw new Error(`Witness maintenance HTTP ${maintenance.status}`)
  await maintenance.arrayBuffer()
  const expectedJson=await(await expectStatus(await request('json'),200)).text()
  if(!(await parentPool.query(`SELECT 1 FROM "${schema}".chunks c JOIN "${schema}".streams s ON s.id=c.stream_id WHERE s.path=$1 AND c.state='sealed'`,[`${prefix}/binary`])).rowCount)throw new Error('Witness must be sealed before its recovery point')
  console.log('Sealed binary/JSON/fork witnesses and producer state ready; waiting for real elapsed collection.')
  const deadline=Date.now()+45*60_000
  while(Date.now()<deadline){
    if(!checkpoint){try{checkpoint=JSON.parse(await readFile(checkpointFile,'utf8'))}catch(error){if(error.code!=='ENOENT')throw error}}
    if(checkpoint){
      if(checkpoint.projectId!==projectId||checkpoint.branchId!==parentId||checkpoint.schema!==schema)throw new Error('Checkpoint identity differs')
      try{await parentObjects.get(checkpoint.retiredChunk.objectKey,checkpoint.retiredChunk.encodedBytes)}catch(error){if(error instanceof ChunkNotFoundError)break;throw error}
    }
    await new Promise(resolve=>setTimeout(resolve,10_000))
  }
  if(!checkpoint)throw new Error('Collection checkpoint not observed')
  checkpointAgeMs=Date.now()-Date.parse(checkpoint.recordedAt)
  if(checkpointAgeMs<0||checkpointAgeMs>300_000)throw new Error('Recovery point is outside the declared five-minute window')
  // Re-check physical absence independently; merely seeing a ledger claim is
  // insufficient evidence that external deletion actually completed.
  let absent=false
  try{await parentObjects.get(checkpoint.retiredChunk.objectKey,checkpoint.retiredChunk.encodedBytes)}catch(error){if(!(error instanceof ChunkNotFoundError))throw error;absent=true}
  if(!absent)throw new Error('Retired object remains physically readable')
  restoreStartedAt=new Date().toISOString()
  await neon.branches.update({projectId,branchId:parentId,expires_at:null})
  const child=await neon.branches.create({projectId,parent_id:parentId,parent_lsn:checkpoint.lsn,name:cleanup.childName,
    expires_at:new Date(Date.now()+3600_000).toISOString(),compute:{minCu:0.25,maxCu:1,suspendTimeoutSeconds:300}})
  childId=child.id;cleanup.childId=childId
  await writeFile(cleanupFile,JSON.stringify(cleanup),{mode:0o600})
  // No serving or scheduled worker participates in the child's adoption.
  for(const trigger of await neon.triggers.list({projectId,branchId:childId}))await neon.triggers.delete({projectId,branchId:childId,triggerId:trigger.trigger_id})
  for(const fn of await neon.functions.list({projectId,branchId:childId}))await neon.functions.delete({projectId,branchId:childId,slug:fn.slug})
  const [connection,storage,credential]=await Promise.all([
    neon.postgres.connectionString({projectId,branchId:childId,pooled:false}),neon.storage.get({projectId,branchId:childId}),
    neon.credentials.create({projectId,branchId:childId,name:'gc-recovery',principal_type:'user',scopes:['storage:read','storage:write'],expires_at:new Date(Date.now()+3600_000).toISOString()}),
  ])
  const url=new URL(connection);url.searchParams.set('sslmode','verify-full')
  childPool=new pg.Pool({connectionString:url.href,max:3,connectionTimeoutMillis:20_000,query_timeout:30_000});childPool.on('error',()=>{})
  childObjects=createS3ChunkStorage({endpoint:storage.s3_endpoint,region:storage.region,bucket:env.NEON_TEST_BUCKET,
    credentials:{accessKeyId:credential.token_id,secretAccessKey:credential.s3_secret_access_key}})
  const retired=(await childPool.query(`SELECT retired_at,object_key FROM "${schema}".chunks WHERE id=$1`,[checkpoint.retiredChunk.chunkId])).rows[0]
  if(!retired?.retired_at||retired.object_key!==checkpoint.retiredChunk.objectKey)throw new Error('Restored point lost its recorded retired manifest')
  absent=false
  try{await childObjects.get(retired.object_key,checkpoint.retiredChunk.encodedBytes)}catch(error){if(!(error instanceof ChunkNotFoundError))throw error;absent=true}
  if(!absent)throw new Error('Child still has the retired object; missing-object recovery gate was not exercised')
  const before=await inspectInstallation(childPool,{schema})
  const {token}=await beginRecovery(childPool,{schema,operationId:cleanup.operationId,expectedInstallationId:before.id,expectedServingEpoch:before.serving_epoch,
    environmentId:childId,storageIdentity:childObjects.identity})
  for(let n=0;n<10_000;n++){
    const batch=await verifyRecovery(childPool,{schema,token,keyring:parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING),objects:childObjects,maxChunks:128})
    verified=batch.verified;if(batch.complete)break;if(n===9999)throw new Error('Recovery verification bound exceeded')
  }
  const drainDeadline=Date.now()+340_000
  while((await childPool.query(`SELECT 1 FROM "${schema}".live_leases WHERE expires_at>clock_timestamp() UNION ALL SELECT 1 FROM "${schema}".replay_pins WHERE expires_at>clock_timestamp() LIMIT 1`)).rowCount){
    if(Date.now()>drainDeadline)throw new Error('Restored reader leases did not drain')
    await new Promise(resolve=>setTimeout(resolve,1000))
  }
  await finishRecovery(childPool,{schema,token})
  recovered=createDurableStreams({postgres:{pool:childPool,schema,environmentId:childId},storage:{objects:childObjects},
    auth:{authenticate:()=>({tenantId:'function-a',subject:'gc-recovery-fixture'}),authorize:()=>true}})
  const local=(path,method='GET',body,headers={})=>recovered.fetch(new Request(`https://fixture.test/v1/stream/${prefix}/${path}`,{method,
    headers:{'Content-Type':'application/octet-stream',...headers},...(body===undefined?{}:{body})}))
  for(const path of ['binary','fork']){
    const response=await local(path)
    if(response.status!==200||!Buffer.from(await response.arrayBuffer()).equals(payload))throw new Error('Restored sealed witness differs from exact bytes')
  }
  if(await(await local('json')).text()!==expectedJson)throw new Error('Restored JSON witness differs')
  if((await local('binary','POST','duplicate',tuple)).status!==204)throw new Error('Restored producer deduplication differs')
  report={passed:true,checkpointAgeAtRestoreStartMs:checkpointAgeMs,restoreStartedAt,completedAt:new Date().toISOString(),verifiedReachableChunks:verified,
    retiredManifestPresent:true,retiredObjectMissingOnParent:true,retiredObjectMissingOnChild:true,witnessBinarySha256:digest(payload),witnessJsonSha256:digest(expectedJson),
    producerRetryDeduplicated:true,sourceCommit:env.TEST_FUNCTION_SOURCE_COMMIT,bundleSha256:env.TEST_FUNCTION_BUNDLE_SHA,checkpoint}
  console.log('Collected-object recovery passed: supported-point child retains retired manifest, lacks its object, verifies every reachable object, and replays exact witnesses.')
}catch(error){report={passed:false,error:{name:error.name,message:error.message},restoreStartedAt,checkpointAgeAtRestoreStartMs:checkpointAgeMs,verifiedReachableChunks:verified};throw error}
finally{
  await recovered?.close();await childPool?.end();childObjects?.close();parentObjects.close();await parentPool.end()
  let cleanupComplete=false
  try{
    if(ownsCleanup){
      const owned=(await neon.branches.list({projectId}).all()).find(branch=>branch.name===cleanup.childName)
      if(owned){if(owned.default||owned.protected||owned.parent_id!==parentId||(childId&&owned.id!==childId))throw new Error('Recovery child identity changed');await neon.branches.delete({projectId,branchId:owned.id})}
      if(cleanup.parentExpiry)await neon.branches.update({projectId,branchId:parentId,expires_at:cleanup.parentExpiry})
      await unlink(cleanupFile)
    }
    cleanupComplete=true
  }finally{
    await mkdir('qualification-results',{recursive:true})
    await writeFile('qualification-results/reclamation-recovery.json',JSON.stringify({...report,cleanupComplete},null,2)+'\n')
  }
}
