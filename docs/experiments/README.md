# Postgres storage experiments

These are small controlled layout/replay experiments on real Neon, separate from the protocol and sustained-load qualification gates. They do not establish an arrival-rate ceiling, an SLA, or Neon billing egress.

| Report | Scope |
| --- | --- |
| [Original synthetic metadata](2026-11-23-postgres-metadata.json) | 65,536 already-sealed 32-byte frames in schema 0007 versus copied manifests with integer arrays. No object uploads, MVCC churn, or runtime reader. Historical harness hash is retained; its source is available at commit `acde3de`. |
| [Intermediate assigned layout](2026-11-23-compact-assigned-measurement.json) | Actual sealing/replay with append chunk assignments still present. Dirty engineering snapshot, identified by hashes, before the final range refinement. Its allocated staging indexes remained large despite zero live append rows. |
| [Final compact range layout](2026-11-23-compact-range-measurement.json) | Independently packed **clean** baseline `1ba871b` versus **clean** implementation `f99f00a`, actual Object Storage publication/replay, and ordinary vacuum. |

## Final measured profile

Each profile stores 8,192 variable-size binary frames in two 4,096-frame batches. The first 64 are ordinary HTTP API appends through the library; the rest are seeded transactionally to exercise bounded maintenance and steady staging reuse without making this a write-throughput benchmark. The library runs in the coordinator process against real Neon Postgres/Object Storage in Frankfurt. Profiles run sequentially on one recorded disposable branch. Their schemas and exact object keys are removed afterward.

The final implementation uses immutable manifest ranges and a monotonic hot floor to remove per-frame freeze updates, append chunk IDs, and two staging indexes. Every replay's ordered digest must match before a sample is recorded.

| Metric | Baseline, 128 frames/chunk | Compact, 128 | Compact, 1,024 |
| --- | ---: | ---: | ---: |
| Retained append rows after sealing | 8,192 | 0 | 0 |
| Live tuple bytes, measured tables | 1,099,261 | 84,992 | 39,296 |
| Allocated measured tables/indexes after ordinary vacuum | 6.49 MiB | 0.98 MiB | 0.94 MiB |
| Stream-head tuple updates for 64 ordinary appends | 128 | 64 | 64 |
| SQL calls for those appends | 640 | 576 | 576 |
| Object GETs across 64 replay pages | 64 | 64 | 8 |
| Observed Postgres connection read bytes during replay | 3,832,042 | 315,702 | 650,772 |

Measured tables are `appends`, `chunks`, and the unchanged `chunk_objects` ledger. The original synthetic comparison predicted roughly 95–97% less metadata; final live tuple bytes fall about 96% at the new default. **Allocated** files fall about 86% in this actual-churn profile because empty staging index pages remain allocated. Ordinary vacuum makes space reusable and may truncate empty heap pages; it does not promise to shrink every B-tree. Budget staging high-water size and vacuum progress separately from retained historical metadata.

One directory contains roughly four raw bytes per stored frame. Larger chunks reduce object requests and manifest rows but can increase the directory fetched per response page; this explains the higher Postgres connection traffic at 1,024 versus 128 frames. Both are substantially below the baseline's repeated per-frame manifest data. The verified cache avoids downloading the same 1,024-frame object for each of its eight response pages. Function routing/cold starts/eviction can require additional downloads; this single-server experiment does not model that routing.

The raw report includes observed branch WAL and HOT tuple updates. WAL includes possible provider/background activity and full-page images; it is not attributed exclusively to the measured statements. The intermediate sample demonstrates that it varies. Connection counters include protocol overhead and are not a measurement of chargeable Neon egress. Replay times here are dominated by coordinator/database round trips and do not establish a capacity or latency guarantee.

## Reproduce

Use a recorded isolated Neon branch with no other application writes. Keep credentials in ignored env files. Build the library, then prepare separately installed baseline/candidate packages:

```sh
mise exec node@24 -- pnpm build
mise exec node@24 -- node scripts/neon-test-rolling-artifacts.mjs
NEON_TEST_STATE_FILE=.neon-test-state-example.json mise exec node@24 -- node --env-file=.env.neon.example docs/experiments/measure-compact-storage.mjs /tmp/compact-storage.json
mise exec node@24 -- node scripts/neon-test-rolling-cleanup.mjs
```

The recorded state and branch environment must match. The harness creates fresh random schemas and exact immutable object keys, never resets statistics, installs extensions, or runs `VACUUM FULL`. It records successful cleanup, packed source/tarball/lock hashes, Node, region, and its own hash. The synthetic harness now explicitly targets schema 0007 so it remains runnable after the canonical migration list grows; use its historical revision to reproduce the original recorded script hash exactly.

The final CI runtime `cdf0fa8` follows the measured `f99f00a` with a conservative eligibility fix for deleted ancestors whose child-generation metadata is still needed. Append, sealing, replay, cache, and migration code are unchanged between those revisions. These measurements and the ten-minute default-collection-off load remain scoped to their recorded `f99f00a` hashes; the full clean qualification of `cdf0fa8` is recorded in the [qualification index](../qualification/README.md).
