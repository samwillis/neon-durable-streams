# Qualification evidence

The first complete protocol run passed **338/338**, with no failures or skips, against real Neon Postgres, Realtime, and Object Storage. Subscriptions were enabled. The suite is pinned to upstream commit `461b40267aabd644558f9b19dbb9507dd5f691cf` and npm conformance package 0.3.7. A second full run after introducing installation fencing also passed all 338 cases (748 seconds). CI must independently verify each exact committed revision; local hosted-service results do not substitute for that job.

Current focused evidence includes 31 unit/property tests, 57 integration/Neon cases, nine actually deployed Function cases (including three public HTTPS webhook tests), and a real WAL-LSN child-branch recovery drill. See [development](../development.md) and [operations](../operations-v1.md) for their scope.

`2026-11-22-load-smoke.json` records a **20-second probe**, using a Node.js client in London against a deployed Function in `aws-eu-central-1`, Postgres compute configured for 0.25–1 CU, pool maximum five, four writers, sixteen live readers, eight binary streams, and 1 KiB records. It acknowledged and then replayed 1,009 unique records after sealing, with no observed errors. Append p50/p95 were 69.87/81.10 ms; observed live p50/p95 were 86.30/99.25 ms. The report includes the exact deployed bundle hash. Other qualification Functions shared the account.

That probe establishes harness functionality and a short sample. It does **not** establish a sustained capacity, availability, cold-start, or recovery guarantee. Its final forced maintenance drained the workload; it does not prove the default cron budget can keep up indefinitely. Physical GC, restored backups older than the drill, mixed-version rollout, in-place restore, and broader failure/overload profiles remain open gates.

Run a reproducible profile on a recorded disposable Function environment:

```sh
DS_LOAD_SECONDS=600 DS_LOAD_WRITERS=4 DS_LOAD_LIVE_READERS=16 \
  mise exec node@24 -- node --env-file=.env.neon.test --env-file=.env.neon.function scripts/qualify-load.mjs
```

The harness refuses an unrecorded branch/Function combination. It measures append acknowledgments and observed live delivery, follows durable SSE checkpoints through rotations, samples frozen/local/outbox backlog every 30 seconds, forces bounded sealing after the workload, and compares exact binary records and producer order during full replay. A run fails on lost/duplicate/reordered records, failed requests, buffer/record bounds, missing live data, or an undrained sealing backlog. Raw JSON reports are saved under ignored `qualification-results/`; promote reviewed reports here with their context. The `Deployed Neon load qualification` manual GitHub workflow provisions and removes its own environment and retains the reports as artifacts.

Live latency is measured from the client starting the append through that client observing its payload, so it includes network transit and duplicates across readers. Database connection counts include other activity on the branch and do not measure per-isolate memory or pool queues. Add those measurements before treating this as complete production qualification.
