# Historical replay qualification

The six replay milestones are implemented on runtime revision [`ad37ffb`](https://github.com/samwillis/neon-durable-streams/commit/ad37ffb5db5029359801067ea0f5d47b79818dcc). The final checks use actual Neon Postgres 18, Realtime and Object Storage in **US-east-1**. The performance driver runs remotely. GitHub Actions is intentionally skipped on push; these checks ran directly.

## Selected design

Objects, integrity blocks and response pages have independent sizes. Objects seal at **4 MiB, 8192 frames, or one hour**; the triggering frame can exceed the byte target. Blocks target **64 KiB**. Pages target **2 MiB**, with separate limits of 8192 frames and 64 immutable objects. Larger frame caps remain configurable up to 131072. Cross-object read-ahead defaults to zero; one look-ahead is available. Streaming range consumption overlaps verification without requiring cross-object prefetch.

The fresh four-file baseline installs storage version 3 and one block codec. Nonzero public offsets contain an append ordinal and a history identifier. The protocol zero token remains unchanged. Sealing deletes append rows, and Postgres retains chunk manifests and block directories without per-append sealed arrays. A restored timeline starts a new write history, rejecting abandoned tokens even after their ordinal is reused. Development installations must be recreated; no old-format reader or migration chain ships.

The live payload path continues to use simple unordered Realtime SQL. Authorization and durable catch-up establish the stream generation and cursor; contiguous committed rows can then flow directly through the Function. Gaps and lifecycle changes use durable validation/replay.

## Completed checks

| Gate | Result | Evidence |
| --- | --- | --- |
| Build, typecheck, example and unit/property checks | 96 tests passed | `pnpm check` on final runtime |
| Independently installed package | Passed | `pnpm test:package`: declarations, exports, SQL assets, runtime and licensing |
| Neon data regression | 115/115, 17 files | [Manifest and counts](2026-11-23-replay-final-neon.json) |
| Transaction/process loss | 21/21 | [Manifest and counts](2026-11-23-replay-final-transactions.json) |
| Full pinned upstream conformance | **338/338**, no filters or skips; subscriptions and collection enabled | [Compiler, suite, source, dependency and deployment identities](2026-11-23-replay-final-conformance.json) |
| Independently packed multi-instance consumers | 8/8; fixture cleanup passed | [Packages, bundles, migrations and checks](2026-11-23-replay-final-packed.json) |
| Public Function, HTTPS webhooks and minute cron | 9/9 on unchanged recheck | [Cases](2026-11-23-replay-final-ingress.json) |
| Hints with no cron | 2/2 | [Cases](2026-11-23-replay-final-hints.json) |
| In-place restore and point-in-time child adoption | 2/2; fixture cleanup passed | [Cases](2026-11-23-replay-final-recovery.json) · [In-place stages and Realtime witness](2026-11-23-replay-final-inplace.json) |
| Final deployed replay profile | 460 exact samples, zero errors | [Full measurements](2026-11-23-replay-final-deployed.json) |
| Full default hot batch | Exact freeze, seal, staging deletion and one-page replay | [Measurements](2026-11-23-replay-final-dense.json) |

The first ingress attempt passed eight cases and failed its health identity assertion: the response identified the preceding deployed bundle. Eight later probes matched the expected deployment, and the complete unchanged suite then passed. [First-attempt evidence](2026-11-23-replay-final-ingress-first.json) remains visible. Three matching readiness responses did not guarantee that every later request reached the new deployment. This is a deployment observation, with no assertion or deadline relaxed for the recheck.

An initial benchmark deployment also overlapped a local clean build and failed to resolve temporarily absent `dist` exports. The serialized deployment succeeded. This was a local qualification scheduling error; it produced no passing benchmark evidence. The earlier failed ordinal-zero conformance attempt remains [recorded separately](2026-11-23-replay-conformance-first.json); restoring the canonical zero token fixed it before final qualification.

## Postgres footprint and default policy

The [five-policy metadata experiment](2026-11-23-replay-metadata-final.json) generated constrained manifests for **1,048,576 sealed 32-byte appends**. It also inserted, deleted and vacuumed one full staging batch per policy. Sizes below are allocated bytes across the measured `appends`, `chunks` and `chunk_objects` relations, including indexes/TOAST. No per-append sealed rows remain.

| Offset / frame policy | Sealed objects | Retained-only bytes | Including reusable staging-index allocation |
| --- | ---: | ---: | ---: |
| Byte control / 1024 | 1024 | 7,544,832 | 7,651,328 |
| Ordinal / 1024 | 1024 | 1,843,200 | 1,966,080 |
| **Ordinal / 8192, selected** | **128** | **499,712** | **1,310,720** |
| Ordinal / 32768 | 32 | 262,144 | 3,457,024 |
| Ordinal / 131072 | 8 | 286,720 | 12,976,128 |

At the same 1024-frame policy, removing sealed boundary arrays reduces retained-only allocation about **4.1×**. The selected default reduces it about **15.1×**, or **5.8×** including the measured reusable staging-index space. The earlier retained-only “26× selected default” result concerned the former 131072-frame candidate and is superseded. It is not a total Postgres saving.

Actual deployed full-batch [freeze/seal comparisons](2026-11-23-replay-frame-cap-comparison.json) support the smaller default. At 8192/32768/131072 frames, the final triggering append took 341/825/2496 ms, sealing took 863/1740/4501 ms, and the reusable append index occupied 802816/3186688/12681216 bytes after ordinary vacuum. Those runs record their exact preceding revisions. Bulk setup is not append-throughput evidence. The final runtime repeated the selected 8192-frame case: **265 ms** triggering append, **849 ms** seal and **642 ms** one-page replay; reached process lifetime RSS high-water was **121268 KiB**.

Staging indexes are shared across streams in an installation. Active tails, frozen backlog, concurrency, autovacuum and long-lived snapshots determine their actual high-water mark. The synthetic relation sizes exclude installation, producer/subscription state and provider history; the staging probe does not characterize long-running bloat. Repeated sealing keeps live rows bounded but does not shrink an already allocated B-tree to its empty size.

## Replay performance and resource bounds

The [baseline/read-path comparison](2026-11-23-replay-readpath.json) passed 517 samples per build and reduced typical warm sealed replay from 17 to 12 SQL calls. Byte-oriented paging later reduced sequential pages from 117 to 60 on the deterministic workload. The [block matrix](2026-11-23-replay-block-matrix.json) found about 1.03× cold download amplification at 64 KiB versus 1.15× at 256 KiB. The [read-ahead comparison](2026-11-23-replay-pipeline-comparison.json) gave no repeatable latency benefit, so read-ahead remains off by default. The [matched offset comparison](2026-11-23-replay-offset-decision.json) preserves the same 1024-frame policy on both variants to isolate the offset decision.

The final 460-sample profile uses three repetitions and seed 20261123. It covers tiny, binary, maximum-size, JSON and mixed frames, quiet tails, deep forks, cancellation, concurrent readers/writers and replay into live delivery. Repeat seeks used a median **11 SQL calls** and p95 **12**. All 39 conditional samples downloaded zero object bytes; their median was six SQL calls. Final latency distributions and routed process identities are retained rather than summarized as a uniform speedup. Cold-seek p95 was 615 ms; repeat-seek p95 was 525 ms. Fan-out and concurrent-reader tails exceeded a second in this remote-coordinator sample.

The final [first-block page experiment](2026-11-23-replay-final-first-block.json) replayed the same 8 MiB binary history exactly. Normal 2 MiB pages needed **4 pages, 83 SQL calls and 1.89 s**. Ending every page at 64 KiB needed **128 pages, 2687 SQL calls and 23.68 s**. Keep ordinary pages larger and verify the complete selected page before headers. This preserves whole-page corruption failure semantics.

The default owned block cache is 32 MiB, pending encoded immutable reads have a separate 128 MiB budget, and replay/retained-body admission has a separate 256 MiB budget. All are per server instance, not fleet limits. Ordinary bodies stream 64 KiB views over an owned verified page. SSE emits bounded data fragments before the checkpoint control event. Admission remains held until drain, cancel, expiry, shutdown or deadline. Shared downloads retain independent reader interests, abort when the final reader leaves, and cannot refill a closed cache.

Header snapshots and a process lifetime high-water measurement do not certify steady-state or fleet memory. Remote timings include networking, routing and service contention. These closed-loop profiles establish exact output and bounded work on the measured workloads; they do not establish an arrival-rate ceiling, availability SLA or a universal optimum. Other concurrent qualification branches share the project/account, although the replay branch had no overlapping workload during the final performance profile.

## Requirement audit

| Milestone | Implementation and exit evidence |
| --- | --- |
| 1. Baseline | Separately deployed baseline/candidate with deterministic contents, independent byte/checkpoint validation, repeated cold/warm and concurrency profiles; source, bundle, harness, seed, SQL, I/O, pool and memory identities retained in reports. |
| 2. Read path | Coherent non-TTL MVCC planning without stream-head locking; atomic TTL touch; combined pins; owned formatting buffer. [Snapshot/seal/delete/deadline races](../../test/neon/replay.test.ts) and [reclamation races](../../test/neon/reclamation.test.ts) pass. |
| 3. Blocks/ranges | Packed trusted directory, independently hashed blocks, strict 206/Content-Range/length checks, coalescing and private bounded cache. [Adapter faults](../../test/unit/s3.test.ts), [real storage ranges](../../test/neon/storage.test.ts) and [block replay](../../test/neon/block-replay.test.ts) pass. Four size variants measured. |
| 4. Pipelines/responses | Bounded optional look-ahead, shared cancellation, retained-body admission, pull-based ordinary/SSE fragments, metadata ETags. Corrupt second block fails before headers; interruption before SSE control resumes from the last completed checkpoint. First-block and read-ahead decisions measured. |
| 5. Offsets | Dense ordinal/history spans remove SQL boundary arrays; retries/rollback/close-only consume no ordinal. Fork prefixes survive sealing. Restore/rewrite rejects abandoned history at the same ordinal in [Neon](../../test/neon/ordinal.test.ts) and both provider drills. Segment hints remove no authorized planning RPC after generation, expiry, ancestry, epoch, publication and pin checks, so no third routing variant was built. |
| 6. Adoption | One fresh baseline and codec, updated adapter/example/runbook, packed current-candidate consumers, all final gates above, selected-default performance, and recorded deletion of owned test resources. |

The [final evidence manifest](2026-11-23-replay-final.json) records cleanup and the exact tested artifacts. Recovery drills retain witness objects and do not qualify provider Object Storage recovery after physical deletion. Enabling physical collection still depends on the provider recovery contract described in the [runbook](../operator-runbook.md). Workload-specific capacity sizing and release operations remain the consuming application's responsibility.
