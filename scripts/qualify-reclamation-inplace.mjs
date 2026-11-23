import { createHash, randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { mkdir, readFile, readdir, unlink, writeFile } from 'node:fs/promises'
import { parseEnv, promisify } from 'node:util'
import { createNeonClient, raw } from '@neon/sdk'
import pg from 'pg'
import { ChunkNotFoundError } from '../dist/index.js'
import { beginRecovery, finishRecovery, inspectInstallation, verifyRecovery } from '../dist/operations.js'
import { createS3ChunkStorage } from '../dist/s3.js'
import { createNeonRealtimeNotifier } from '../dist/neon.js'
import { parseSubscriptionKeyring } from '../dist/subscriptions.js'

// Run after starting BOTH long-running qualification scripts and their child
// recovery observer. It waits for their passing reports before stopping ONLY
// that recorded disposable deployment. Cleanup uses the existing restore ledger.
const env=process.env, stateFile=env.NEON_TEST_STATE_FILE??'.neon-test-state.json', functionFile=env.NEON_TEST_FUNCTION_ENV_FILE??'.env.neon.function'
const state=JSON.parse(await readFile(stateFile,'utf8'))
if(!env.NEON_API_KEY||state.projectId!==env.NEON_PROJECT_ID||state.branchId!==env.NEON_TEST_BRANCH_ID||state.branchId!==env.TEST_FUNCTION_BRANCH_ID
  ||!state.name.startsWith('ds-test-')||!/^ds_test_[a-f0-9]{32}$/.test(env.TEST_FUNCTION_SCHEMA??''))throw new Error('Recorded isolated Function required')
const projectId=state.projectId,branchId=state.branchId,schema=env.TEST_FUNCTION_SCHEMA,run=promisify(execFile),id=randomUUID(),file='.neon-test-state-inplace.json'
const neon=createNeonClient({apiKey:env.NEON_API_KEY,throwOnError:true,waitForReadiness:true,wait:{timeoutMs:180_000}})
const hash=bytes=>createHash('sha256').update(bytes).digest('hex')
const sourceSha256=hash(await readFile(new URL(import.meta.url)))
const connect=connectionString=>{const result=new pg.Pool({connectionString,max:2,connectionTimeoutMillis:20_000,query_timeout:30_000});result.on('error',()=>{});return result}
const storage=()=>createS3ChunkStorage({endpoint:env.AWS_ENDPOINT_URL_S3,region:env.AWS_REGION,bucket:env.NEON_TEST_BUCKET})
const request=(path,method='GET',body,headers={})=>fetch(new URL(path,env.TEST_FUNCTION_URL),{method,headers:{Authorization:`Bearer ${env.TEST_FUNCTION_API_KEY}`,...headers},...(body===undefined?{}:{body}),signal:AbortSignal.timeout(45_000)})
let pool=connect(env.TEST_DATABASE_URL_UNPOOLED),objects=storage(),realtimeObserver,ownsLedger=false,renamed=false,stopped=false,report={passed:false},checkpoint,checkpointAgeMs
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms))
try{
  console.log('Waiting for passing live-byte, real elapsed collection, and child recovery reports before any rollback.')
  const deadline=Date.now()+65*60_000
  let load,gc,child
  while(Date.now()<deadline){
    for(const name of await readdir('qualification-results')){
      if(!/^(qualify-|reclaim-).+\.json$/.test(name))continue
      let value;try{value=JSON.parse(await readFile(`qualification-results/${name}`,'utf8'))}catch{continue}
      if(value.bundleSha256!==env.TEST_FUNCTION_BUNDLE_SHA||value.deploymentId!==env.TEST_FUNCTION_DEPLOYMENT_ID||value.profile?.seconds!==2700)continue
      if(name.startsWith('qualify-'))load={...value,file:name};else gc={...value,file:name}
    }
    try{child=JSON.parse(await readFile('qualification-results/reclamation-recovery.json','utf8'))}catch(error){if(error.code!=='ENOENT'&&!(error instanceof SyntaxError))throw error}
    if(load&&gc&&child)break
    await pause(10_000)
  }
  // An explicitly requested independent recovery drill may use a byte-exact
  // run whose ONLY failures concern the external diagnostic head watcher.
  // Preserve that failed benchmark and record the distinction in this report.
  const loadDataSafetyPassed=!!load&&load.acknowledgedAppends>0&&load.replayedRecords===load.acknowledgedAppends
    &&load.liveVerification?.verifiedReaders===16&&load.liveVerification?.orderedSha256MatchesReplay===true&&load.remainingLocalPayloads===0
    &&Array.isArray(load.errors)&&load.errors.every(error=>error.phase==='head-probe')
  const allowHeadProbeFailure=env.DS_GC_RESTORE_ALLOW_HEAD_PROBE_FAILURE==='1'&&loadDataSafetyPassed
  if((!load?.passed&&!allowHeadProbeFailure)||!gc?.passed||!child?.passed||!child.cleanupComplete||child.checkpoint?.branchId!==branchId||child.checkpoint?.schema!==schema||load.profile.seconds!==2700||gc.profile.seconds!==2700
    ||child.bundleSha256!==env.TEST_FUNCTION_BUNDLE_SHA)throw new Error('All three full passing qualification reports are required before rollback')
  const witness=(await pool.query(`SELECT path FROM "${schema}".streams WHERE tenant_id='function-a' AND path LIKE 'gc-recovery-%/binary' AND deleted_at IS NULL`)).rows
  if(witness.length!==1)throw new Error('Expected exactly one recorded sealed witness')
  const prefix=witness[0].path.slice(0,-'/binary'.length)
  const binaryResponse=await request(`v1/stream/${prefix}/binary`),jsonResponse=await request(`v1/stream/${prefix}/json`)
  if(!binaryResponse.ok||!jsonResponse.ok)throw new Error('Parent witness unavailable')
  const binary=Buffer.from(await binaryResponse.arrayBuffer()),json=await jsonResponse.text()
  if(hash(binary)!==child.witnessBinarySha256||hash(json)!==child.witnessJsonSha256)throw new Error('Parent witness differs from child recovery proof')
  // Lock this retired manifest only while recording its WAL point. No object
  // I/O occurs in SQL, and no retention deadline is changed.
  const client=await pool.connect()
  try{
    await client.query('BEGIN')
    const target=(await client.query(`SELECT c.id,c.object_key,c.encoded_bytes,c.retired_at,o.collect_after FROM "${schema}".chunks c JOIN "${schema}".chunk_objects o ON o.chunk_id=c.id AND o.object_key=c.object_key
      WHERE c.retired_at IS NOT NULL AND o.collected_at IS NULL AND o.collect_after BETWEEN clock_timestamp()+interval '5 seconds' AND clock_timestamp()+interval '60 seconds'
      ORDER BY o.collect_after LIMIT 1 FOR SHARE OF c`)).rows[0]
    if(!target)throw new Error('No naturally expiring retired manifest available for supported-point restore')
    const lsn=(await client.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn
    checkpoint={...target,lsn,recordedAt:new Date().toISOString()}
    await client.query('COMMIT')
  }catch(error){await client.query('ROLLBACK');throw error}finally{client.release()}
  for(let n=0;n<100;n++){if((await pool.query('SELECT pg_current_wal_flush_lsn()>=$1::pg_lsn AS flushed',[checkpoint.lsn])).rows[0].flushed)break;if(n===99)throw new Error('Checkpoint did not flush');await pause(100)}
  const collectionDeadline=Date.now()+180_000
  let absent=false
  while(Date.now()<collectionDeadline){
    try{await objects.get(checkpoint.object_key,checkpoint.encoded_bytes)}catch(error){if(!(error instanceof ChunkNotFoundError))throw error;absent=true;break}
    const response=await request('_maintenance','POST',undefined,{'x-ds-maintenance-key':env.TEST_FUNCTION_MAINTENANCE_KEY})
    if(!response.ok)throw new Error('Post-soak maintenance failed');await response.arrayBuffer();await pause(1000)
  }
  if(!absent)throw new Error('Target retired object did not physically collect')
  const branch=await neon.branches.get({projectId,branchId})
  if(branch.default||branch.protected||branch.name!==state.name||!branch.parent_id||(await neon.branches.list({projectId}).all()).some(item=>item.parent_id===branchId))throw new Error('Unsafe restore target identity or remaining child')
  const name=`ds-test-inplace-${id}`,preservedName=`ds-test-preserved-${id}`
  for(const reserved of ['.env.neon.inplace','.env.neon.inplace.function','.neon-test-state-inplace-branch.json']){
    try{await readFile(reserved);throw new Error('Reserved in-place fixture file already exists')}catch(error){if(error.code!=='ENOENT')throw error}
  }
  await writeFile(file,JSON.stringify({projectId,branchId,parentId:branch.parent_id,name,preservedName,checkpoint}),{mode:0o600,flag:'wx'});ownsLedger=true
  await neon.branches.update({projectId,branchId,name});renamed=true
  await writeFile(stateFile,JSON.stringify({...state,name}),{mode:0o600})
  for(const trigger of await neon.triggers.list({projectId,branchId}))await neon.triggers.delete({projectId,branchId,triggerId:trigger.trigger_id})
  const before=await inspectInstallation(pool,{schema})
  await beginRecovery(pool,{schema,operationId:randomUUID(),expectedInstallationId:before.id,expectedServingEpoch:before.serving_epoch,environmentId:branchId,storageIdentity:objects.identity})
  for(const fn of await neon.functions.list({projectId,branchId}).all())await neon.functions.delete({projectId,branchId,slug:fn.slug})
  if((await neon.functions.list({projectId,branchId}).all()).length||(await neon.triggers.list({projectId,branchId})).length)throw new Error('Serving resources did not stop')
  stopped=true;await pause(6000)
  await pool.end();pool=undefined;objects.close();objects=undefined
  checkpointAgeMs=Date.now()-Date.parse(checkpoint.recordedAt)
  if(checkpointAgeMs<0||checkpointAgeMs>300_000)throw new Error('Restore request would exceed the five-minute supported window')
  await neon.branches.update({projectId,branchId,expires_at:null})
  const response=await raw.restoreProjectBranch({client:neon.client,path:{project_id:projectId,branch_id:branchId},body:{source_branch_id:branchId,source_lsn:checkpoint.lsn,preserve_under_name:preservedName},throwOnError:false})
  if(!response.data)throw new Error(`Self-restore HTTP ${response.response?.status}`)
  await neon.operations.waitFor({operations:response.data.operations})
  await neon.branches.update({projectId,branchId,expires_at:new Date(Date.now()+3600_000).toISOString()})
  const connection=new URL(await neon.postgres.connectionString({projectId,branchId,pooled:false}));connection.searchParams.set('sslmode','verify-full')
  pool=connect(connection.href);objects=storage()
  const retired=(await pool.query(`SELECT retired_at,object_key FROM "${schema}".chunks WHERE id=$1`,[checkpoint.id])).rows[0]
  if(!retired?.retired_at||retired.object_key!==checkpoint.object_key)throw new Error('In-place restore did not retain the retired manifest')
  absent=false;try{await objects.get(checkpoint.object_key,checkpoint.encoded_bytes)}catch(error){if(!(error instanceof ChunkNotFoundError))throw error;absent=true}
  if(!absent)throw new Error('Missing collected object gate was not exercised after in-place restore')
  const restored=await inspectInstallation(pool,{schema}),operation=await beginRecovery(pool,{schema,operationId:randomUUID(),expectedInstallationId:restored.id,expectedServingEpoch:restored.serving_epoch,environmentId:branchId,storageIdentity:objects.identity})
  let verified=0
  for(let n=0;n<10_000;n++){const batch=await verifyRecovery(pool,{schema,token:operation.token,objects,keyring:parseSubscriptionKeyring(env.DURABLE_STREAMS_KEYRING),maxChunks:128});verified=batch.verified;if(batch.complete)break;if(n===9999)throw new Error('Recovery verification bound exceeded')}
  const drainDeadline=Date.now()+340_000
  while((await pool.query(`SELECT 1 FROM "${schema}".live_leases WHERE expires_at>clock_timestamp() UNION ALL SELECT 1 FROM "${schema}".replay_pins WHERE expires_at>clock_timestamp() LIMIT 1`)).rowCount){if(Date.now()>drainDeadline)throw new Error('Restored reader leases did not drain');await pause(1000)}
  await finishRecovery(pool,{schema,token:operation.token})
  const deployed=await run(process.execPath,['scripts/neon-test-function.mjs'],{env:{...env,NEON_TEST_NO_CRON:'1',NEON_TEST_MAINTENANCE_HINTS:'off',NEON_TEST_LIVE_FALLBACK_MS:'60000',NEON_TEST_LIVE_VALIDATION_MS:'60000'},timeout:360_000,maxBuffer:8*1024*1024})
  if(!deployed.stdout.includes('Deployed and verified'))throw new Error('Fresh deployment did not verify readiness')
  const fresh=parseEnv(await readFile(functionFile,'utf8'))
  if(fresh.TEST_FUNCTION_DEPLOYMENT_ID===env.TEST_FUNCTION_DEPLOYMENT_ID||fresh.TEST_FUNCTION_HAS_CRON!=='no'||fresh.TEST_FUNCTION_BUNDLE_SHA!==env.TEST_FUNCTION_BUNDLE_SHA)throw new Error('Expected fresh deployment of the same bundle without cron')
  for(const path of ['binary','fork']){const replay=await request(`v1/stream/${prefix}/${path}`);if(!replay.ok||!Buffer.from(await replay.arrayBuffer()).equals(binary))throw new Error('Public restored sealed bytes differ')}
  const replayJson=await request(`v1/stream/${prefix}/json`);if(!replayJson.ok||await replayJson.text()!==json)throw new Error('Public restored JSON differs')
  const retry=await request(`v1/stream/${prefix}/binary`,'POST','duplicate',{'Content-Type':'application/octet-stream','Producer-Id':prefix,'Producer-Epoch':'0','Producer-Seq':'0'});await retry.arrayBuffer();if(retry.status!==204)throw new Error('Restored producer retry did not deduplicate')
  const livePath=`v1/stream/${prefix}/post-restore`,created=await request(livePath,'PUT',undefined,{'Content-Type':'text/plain'});await created.arrayBuffer();if(created.status!==201)throw new Error('Restored live fixture create failed')
  const liveStream=(await pool.query(`SELECT id,tail_offset FROM "${schema}".streams WHERE tenant_id='function-a' AND path=$1 AND deleted_at IS NULL`,[`${prefix}/post-restore`])).rows[0]
  if(!liveStream)throw new Error('Restored live stream identity unavailable')
  realtimeObserver=createNeonRealtimeNotifier({url:env.NEON_REALTIME_URL,secret:env.NEON_REALTIME_SECRET,database:connection.pathname.slice(1),readyTimeoutMs:30_000})
  let observedAppend
  const appendWatch=realtimeObserver.watchAppends({schema,tenantId:'function-a',streamId:liveStream.id,after:liveStream.tail_offset,installation:await inspectInstallation(pool,{schema})},update=>{
    if(update.type!=='invalidate')observedAppend??=update.rows.find(row=>row.streamId===liveStream.id&&row.start===liveStream.tail_offset&&row.payload&&Buffer.from(row.payload).equals(Buffer.from('after-gc-restore')))
  })
  await appendWatch.ready
  const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),45_000)
  let reader
  try{
    const live=await fetch(new URL(`${livePath}?offset=now&live=sse`,fresh.TEST_FUNCTION_URL),{headers:{Authorization:`Bearer ${fresh.TEST_FUNCTION_API_KEY}`},signal:abort.signal})
    if(!live.ok||!live.body)throw new Error('Restored SSE did not open')
    reader=live.body.getReader()
    const appended=await request(livePath,'POST','after-gc-restore',{'Content-Type':'text/plain','Stream-Closed':'true'});await appended.arrayBuffer();if(!appended.ok)throw new Error('Restored live append failed')
    const observationDeadline=Date.now()+30_000
    while(observedAppend?.end!==appended.headers.get('Stream-Next-Offset')){if(Date.now()>observationDeadline)throw new Error('Restored raw Realtime payload was not independently observed');await pause(50)}
    let text='',ended=false
    for(let n=0;n<100;n++){const part=await reader.read();if(part.done){ended=true;break}text+=new TextDecoder().decode(part.value);if(text.length>65536)throw new Error('Restored SSE buffer exceeded bound')}
    if(!ended||!text.includes('after-gc-restore'))throw new Error('Restored SSE did not deliver final bytes before EOF')
  }finally{clearTimeout(timer);abort.abort();await reader?.cancel().catch(()=>{});reader?.releaseLock();appendWatch.close();realtimeObserver.close();realtimeObserver=undefined}
  report={passed:true,generatedAt:new Date().toISOString(),harnessSha256:sourceSha256,checkpoint,checkpointAgeAtRestoreRequestMs:checkpointAgeMs,verifiedReachableChunks:verified,
    retiredManifestPresent:true,collectedObjectMissing:true,publicExactBinaryJsonForkReplay:true,producerRetryDeduplicated:true,newRealtimeAppendAndEof:true,
    newRealtimePayloadObserved:true,newRealtimeObservedOffset:observedAppend.end,
    originalBundleSha256:env.TEST_FUNCTION_BUNDLE_SHA,restoredBundleSha256:fresh.TEST_FUNCTION_BUNDLE_SHA,originalDeploymentId:env.TEST_FUNCTION_DEPLOYMENT_ID,restoredDeploymentId:fresh.TEST_FUNCTION_DEPLOYMENT_ID,
    loadReport:load.file,loadBenchmarkPassed:load.passed,loadDataSafetyPassed,loadDiagnosticErrors:load.errors,
    independentRecoveryAfterHeadProbeFailure:!load.passed&&allowHeadProbeFailure,collectionReport:gc.file,childRecoveryReport:'reclamation-recovery.json'}
  console.log('Stopped-Function in-place collection restore passed: reachable objects, exact public replay, producer retry, fresh Realtime final bytes and EOF.')
}catch(error){report={...report,passed:false,error:{name:error.name,message:error.message},checkpoint,checkpointAgeAtRestoreRequestMs:checkpointAgeMs,stopped};throw error}
finally{
  realtimeObserver?.close();await pool?.end();objects?.close()
  let cleanupComplete=false
  try{
    if(ownsLedger&&renamed){
      await run(process.execPath,['scripts/neon-test-inplace-cleanup.mjs'],{env,timeout:180_000,maxBuffer:1024*1024})
      const current=JSON.parse(await readFile(stateFile,'utf8'))
      if(current.branchId!==branchId||current.projectId!==projectId)throw new Error('Cleanup ledger changed')
      await unlink(stateFile)
      for(const [path,key] of [[functionFile,'TEST_FUNCTION_BRANCH_ID'],[env.NEON_TEST_ENV_FILE??'.env.neon.test','NEON_TEST_BRANCH_ID']]){
        try{const saved=parseEnv(await readFile(path,'utf8'));if(saved[key]!==branchId)throw new Error('Refusing to remove another branch environment');await unlink(path)}catch(error){if(error.code!=='ENOENT')throw error}
      }
      cleanupComplete=true
    }else if(ownsLedger){await unlink(file)}
  }finally{
    await mkdir('qualification-results',{recursive:true})
    await writeFile('qualification-results/reclamation-inplace.json',JSON.stringify({...report,cleanupComplete},null,2)+'\n')
  }
}
