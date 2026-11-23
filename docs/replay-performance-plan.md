# Historical replay on Neon

**Proposed implementation order.** Start with the deployed read path and measure each change against it. The design uses three independent units. An immutable object is the unit of publication and collection. A block is the unit of integrity verification and partial download. A response page is the unit of client progress. Each has a different reason to be large or small.

Development installations can be cleared and recreated. At adoption, install the selected schema, offset grammar and object format as one fresh baseline. Drop superseded codecs and fixtures from the shipping package. Use separate fresh installations for comparisons. Checkpoints issued within the selected design must survive sealing, restarts, forks and recovery of their history.

## Execution record

Work started on 2026-11-23. All six milestones remain in scope. The baseline and first read-path candidate have separately deployed Functions and fresh schemas on the recorded replay test branch.

| Milestone | Evidence and remaining work |
| --- | --- |
| 1 | Baseline `1e2d986` and candidate `66cc94f` each passed 517 samples with three repetitions per profile. Coverage includes five payload profiles, quiet streams, sixteen fork levels, cancellation, catch-up into live delivery and exact replay of concurrent writes. [Reports and limits](qualification/2026-11-23-replay-readpath.json). |
| 2 | Candidate `66cc94f` removes stream-head locking for non-TTL snapshots, combines pin statements/transactions and reduces buffer copies. Typical warmed sealed replay dropped from 17 to 12 SQL calls per page. Six actual-Neon snapshot/pin cases passed. The broader run passed 36 cases. A new range case encountered a cached old adapter in that long-running process and passed its fresh-process recheck. Client tail latency varied across routed instances. |
| 3 | The adapter supports bounded cancellable HTTP Range GETs with strict response validation. Actual Neon returned 3,074 selected bytes from a 4,224,034-byte sealed object in three exact ranges. The compact independently verifiable block codec has six passing property/corruption cases. Serving replay still requires publication, directory, cache and read integration. |
| 4 | Response pipelining, byte-oriented pages, shared cancellation and conditional GET optimization are pending. |
| 5 | Offset prototypes, restore-collision checks and the measured choice are pending. |
| 6 | Fresh-baseline adoption and complete final qualification are pending. |

Initial reports are in ignored `qualification-results/replay-*.json`. An interrupted baseline attempt records a driver error when reading a body from a 304 response. The corrected initial run passed. Some exploratory baseline requests overlapped isolated-schema regression activity on the same branch. Run the matched performance comparisons without that overlap before attributing latency differences. The final report will retain source, bundle and harness identities and distinguish header-stage metrics from response-body lifetime measurements.

## 1. Establish the baseline on a deployed Function

Extend the existing load and compact-storage harnesses with a historical replay profile. Start with a clean package on a disposable Neon branch in the Function's region. Record the commit, bundle, compute size, pool and concurrency settings, storage layout and random seed. Reuse the generated stream contents and request schedule across candidates. Check bytes and checkpoints against an independent expected history for every reader.

The earlier 8,192-frame experiment took 1,087 SQL calls for 64 pages with either chunk size. Larger chunks reduced object GETs from 64 to eight. Those timings include a remote coordinator. The current default of 128 frames per page also means that a stream of 32-byte appends returns at most 4 KiB of payload per page. These provide two concrete targets for the deployed benchmark. [Measurement and methodology](experiments/README.md)

| Workload | Record |
| --- | --- |
| Random cold seeks, repeat seeks and sequential catch-up across objects | Time to first byte and complete page, pages per MiB, storage GET count and downloaded bytes per delivered byte. Distinguish a new Function instance, a verified-cache hit and concurrent readers sharing a fetch. |
| Readers and a writer on one stream, then distributed across streams and Function instances | Offered and achieved throughput, writer and reader p95/p99, SQL calls and transactions per page, lock wait, pin inserts/deletes and pool occupancy. |
| Tiny appends, mixed sizes, maximum-size binary frames, JSON batches, many quiet streams | Postgres heap/index/TOAST bytes per million sealed appends after vacuum, WAL, object count and size, seal lag, index bytes read and CPU spent decoding. |
| Slow and canceled readers, deep forks and catch-up into a live tail | Peak Function RSS and external buffer memory, bytes in flight, canceled work, pin lifetime, Realtime lag and exact final checkpoint. |

Run cold and warm samples separately, repeat matched runs, and report their spread. Keep the benchmark driver outside the measured Function. Instrument inside the Function so client network latency can be distinguished from SQL, object I/O, verification and formatting.

## 2. Reduce SQL contention and copies with the current format

- Prototype snapshot-based planning for ordinary reads without sliding TTL. Capture path generation, expiry, closure, tail, capped fork ancestry and the hot/sealed page consistently. Acquire the existing collection protection before releasing the snapshot. Keep atomic TTL touch/expiry scheduling on its own path. Pause a non-TTL reader after taking its snapshot and prove an appender can commit while planning is still open. Race that reader with sealing, delete/recreate, retirement and restore, and verify its bytes or retry outcome.
- Combine planning queries and pin operations where their locks and snapshot permit it. Count post-download pin validation, release and serving-epoch checks in the benchmark. Target one planning request plus the measured cost of final validation and cleanup. A separate decision about pins is justified if writes still dominate at fan-out. [Current read path](../src/store.ts) · [Replay protection](../src/replay.ts)
- Give the storage adapter and page formatter explicit buffer ownership. Read a known bounded length into one owned buffer, decode views over verified bytes, and assemble the final page in one allocation when a contiguous body is needed. For JSON, write brackets and separators directly. Keep cached bytes private and prevent small responses from retaining an entire evicted object. Check actual Node Fetch and deployed Function memory behavior. [Adapter](../src/s3.ts) · [Page formatting](../src/payload.ts)
- Keep the authorized Realtime payload fast path. Its query uses the stream UUID and lower-offset predicate. Ordering, deduplication, gap recovery and terminal EOF stay in the Function. Benchmark the transition from historical replay to live forwarding.

**Exit evidence.** Compare the same layout before and after. Record SQL calls, lock contention, page copies and peak memory. Verify page bytes, ETags, TTL renewal and all reader/collector races before using this version as the control for format experiments.

## 3. Implement verifiable blocks and HTTP Range GETs

Give each sealed object a compact versioned encoding. Group complete append frames into blocks by target encoded bytes. Store frame lengths and JSON element boundaries inside each block. An oversized valid frame can occupy its own block up to the configured hard limit. Cap index entries, decoded bytes and encoding overhead as well as payload bytes. Seal on size, age or bounded work so quiet streams can leave Postgres. Continue verifying the full upload before the publication transaction removes staging rows.

Start with a packed directory on the Postgres chunk manifest. Each entry identifies a logical frame interval, physical byte start and length, payload size and digest. Bind the directory to the immutable object identity, format and exact encoded size at publication. Validate ordering, coverage and bounds before I/O. This allows a cold Function to locate and authenticate a partial read with its first storage request. Measure directory size and SQL transfer. Compare an authenticated directory inside the object if manifest egress becomes expensive.

Add a bounded range operation to the object adapter. Convert the internal half-open interval to inclusive S3 `Range: bytes=start-end`. Verify HTTP 206, exact `Content-Range` including total size, actual body length and each block digest. Reject ignored ranges, truncation, unexpected bytes and corrupt blocks before using the result. Use an explicit full GET for small objects, full-object verification and dense sequential reads when cheaper. Coalesce adjacent selected blocks into one GET. Test the request and returned bytes against real Neon Object Storage as well as fault-injected adapter responses. [Neon S3 compatibility](https://neon.com/docs/storage/s3-compatibility)

**Exit evidence.** A cold interior seek in a multi-block object transfers only the selected blocks. A warm repeated seek needs zero object GETs when its blocks are cached. A sequential scan uses coalesced ranges or full GETs. Measure 64 and 256 KiB block targets with 4 and 16 MiB object targets, plus the current format as a control. Choose defaults from measured request counts, amplification, latency and metadata cost.

## 4. Pipeline reads, verification and responses

Plan bounded pages by bytes with a separate decoding-work limit. Allow compact sealed blocks to contribute many tiny frames without generating thousands of small HTTP responses. Set the exact logical end before issuing a response checkpoint. Use block totals for whole blocks and decode boundary blocks when an interior start, byte budget or fork cap cuts through a block.

- **Storage pipeline.** Start with one range in flight and at most one range of read-ahead. Verify complete blocks as input arrives. Preserve logical order across hot/sealed and parent/child boundaries. Budget bytes and requests both per reader and per Function, including pending fetches and cached blocks. Coalesce concurrent fetches of the same immutable block. Canceling one reader releases its interest. Abort shared I/O when its last reader leaves.
- **Ordinary GET and long-poll.** Overlap I/O with verification and formatting. Prepare the verified page before sending headers to preserve the current guarantee that a corrupt later block fails the entire page. Serve verified spans through a pull-based body where this saves a copy. Compare that with a single owned output buffer. Benchmark ending a cold GET page at the first verified block, so the client can receive it while a longer scan would still be fetching. Measure the extra client round trips. Sending part of a multi-block page before verification finishes would change how late corruption is reported and when clients may use `Stream-Next-Offset`; settle those semantics before enabling it. [Current corruption regression](../test/neon/storage.test.ts)
- **SSE.** Fetch ahead only within the byte budget and downstream demand. Emit data in order, then the control checkpoint for the complete data event. Preserve text newline handling, base64 carry across fragments, UTF-8 boundaries, exact JSON and final EOF. On interruption before control, the last completed checkpoint remains resumable. Release buffers, pins, queued reads and admission on cancel, auth expiry, rotation and shutdown, including when a fetch finishes late.
- **Conditional GET.** Prototype an ETag derived from the authorized immutable representation, logical page bounds, generation/history, encoding version and closure. Identical bytes and metadata must produce a stable validator across hot-to-sealed publication. Different content after restore or a closure transition must invalidate it. Where the exact representation is known from metadata, a matching conditional GET should return 304 without an object download. Preserve the special handling of `offset=now`.

**Exit evidence.** Compare read-ahead off/on under slow clients and overlapping readers. Record actual first-byte and full-page improvements, high-water memory and work after cancellation. Inject a failure in the second block and a disconnect between SSE data and control. Check that clients can resume exactly and other readers sharing the download continue.

## 5. Decide the public offset using the improved reader

Use the improved reader and today's logical byte offset as the experimental control. Prototype a fixed-width append number followed by a history identifier on a fresh installation. For a stable logical segment and position assigned while hot, first trace the SQL plan and the checks it would still execute. Build that third benchmark candidate if it can remove a lookup from the normal path. Compare on identical workloads, including segment rollover, a frame crossing the size target and age-based early sealing.

For the append-number candidate, increment the locked stream tail once per committed nonempty stored frame. A retry, rollback or close-only request consumes no number. Persist the history identifier once per span of history. A fork inherits its parent's tokens through the anchor, and its own partial binary/JSON prefix is a new child frame. A restored installation retains surviving tokens and mints a fresh history identifier for future writes. Validate the identifier as well as the number during replay and atomic acknowledgements. Boundaries inside a dense valid interval need no per-append SQL directory. Exercise Realtime predicates, producer retries, wake-stream appends, cross-fork reads and pending acks after ancestor collection with each candidate.

For the segment candidate, measure precisely which lookup it avoids. At a chunk end, resume with the next valid frame even if it is hot or belongs to a child. Resolve the currently authorized generation and its ancestry before using any object hint. Include path recreation, sliding TTL, serving-epoch checks and pins in the SQL count. A direct route earns adoption only if it reduces total work at the same lifecycle guarantees.

**Decision.** Choose the simplest candidate that gives a repeatable reduction in SQL work or retained metadata on the target workloads without materially worsening writer latency, cold seeks or recovery. Require lexical ordering and exact checkpoint reuse across restarts and sealing. Restore to append 100, rewrite past 105 with different bytes, reject the abandoned token for 105, and accept both the surviving prefix and the new 105. Use this test for any candidate considered for adoption.

## 6. Adopt and qualify one design

Update the four logical baseline migrations, public adapter contract, storage codec, package example and runbook together. Recreate the disposable development installation with its own empty object namespace. Verify same-version restarts, multiple packed consumer Functions and client reconnects against that installation.

Run unit/property tests, real-Neon regression, transaction/process-loss and deployed Function suites, hints-only scheduling, both recovery drills and the complete pinned **338-case conformance suite** with subscriptions and collection enabled. Repeat the matched performance runs on the chosen build. Record the code and bundle hashes, settings, distributions, resource peaks and cleanup with the existing qualification evidence. Use the results to document capacity and defaults for the measured workloads. [Test entrypoints](development.md) · [Qualification index](qualification/README.md)
