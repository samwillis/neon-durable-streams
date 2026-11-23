# Qualified upgrade boundaries

The package is unreleased and remains `private: true`. Git revisions and artifact hashes identify the tested binaries; both prerelease snapshots call themselves `0.0.1`. A matching package version string alone does not identify a qualified pair.

| Boundary | Procedure and evidence |
| --- | --- |
| Fresh installation | Explicit checksummed migrations 0001–0007, storage format v1, environment/storage binding, and initialization of the external subscription keyring. |
| Schema 0004 → 0005 | Stop ingress, workers, and old instances. The prior snapshot fixture preserves sealed bytes, fork ranges, offsets, and producer sessions. Pre-0005 binaries have no serving-epoch guard and cannot overlap this upgrade. |
| Binary overlap on schema 0005 | `b22c1675f7eb5c32b52f39be39279c819ea5f21f` → `01643614c6ba142ef945e5a3c3607731293cf875` passes eight checks through separately deployed Neon Functions built from independently installed package tarballs. Every migration checksum matches. |
| Binary rollback on the same schema | The pinned baseline reads current-written sealed data and resumes producer sessions. After an epoch change, fresh baseline/current instances can serve the activated installation; the earlier processes remain fenced. |
| Schema 0006 → 0007 | Stop all runtimes/workers. Migration 0007 fences old epochs, removes application PITR settings, and disables collection pending explicit opt-in under provider-owned historical recovery. Exact retained bytes and old-offset forks have an actual-Neon regression; current-source CI is required. |
| Other schema/format or binary pairs | Unqualified. Use a stopped-ingress procedure until a specific compatibility pair is tested. Unknown migration history is rejected. Never infer compatibility from a similar API or an unchanged chunk-format number. |

The mixed-binary result qualifies this pair and its recorded dependencies. It does not qualify rolling DDL, a new storage format, destructive GC, or arbitrary older binaries. A future migration must explicitly define which readers/writers may overlap and fence unsupported warm runtimes before changing their storage assumptions.

## Rolling a qualified binary pair

1. Check the exact source/package hashes and migration checksums against the supported matrix. Preserve the existing backup policy, immutable objects, and external wrapping keys. Both versions must use the same installation, environment/storage identities, authentication policy, and persistent keyring.
2. Start the new binary on the existing schema without changing the schema or format during overlap. Give each deployment a unique rollout identifier. Wait for stable authenticated readiness and representative protocol reads before routing application traffic to it.
3. Shift traffic in bounded stages while checking append/error latency, reader admission, sealing/outbox backlog, and resource use. Clients retain their producer epoch/sequence and durable offsets across backends. Page boundaries can differ; clients follow the returned checkpoints and `Stream-Up-To-Date` signal.
4. Both qualified seal/dispatch workers may run during overlap. Their durable claims and publication fences coordinate ownership. A deployment must still budget aggregate pool, Function, and live-reader capacity across versions.
5. Stop routing new requests to the older version, drain its live reads and work, and stop its instances. Keep the prior artifact until the rollback window closes. Ordinary same-schema rolling deployment does not need an installation epoch change; an administrative recovery changes the epoch and requires fresh runtime instances.
6. Roll back only within the qualified schema/format boundary. A runtime that pinned an earlier serving epoch remains fenced after recovery. Deploy a fresh instance of the supported older artifact instead of reviving its old process.

For DDL or incompatible storage changes, use the [quiesced operations procedure](operations-v1.md) and [in-place restore runbook](inplace-restore-v1.md). Stopping old ingress/instances is mandatory across a rollback that can restore an old epoch.

## What the mixed-version test executes

`scripts/neon-test-rolling-artifacts.mjs` fetches only the pinned baseline commit when necessary, archives its source/assets, installs its frozen dependencies, builds it, and packs it. It also snapshots the current source and builds it in another isolated directory before packing, so stale/shared `dist` output cannot identify the tested candidate. Each tarball is installed into a separate clean consumer. Build subprocesses receive no cloud credentials. Reports retain source, archive, tarball, dependency-lock, and deployed-bundle hashes.

`test/neon/rolling.test.ts` bundles the same test consumer against each installed package, deploys the two Functions against one dedicated schema, and runs:

- Cross-version producer retries, concurrent duplicate appends, exact ordering, and epoch fencing.
- Concurrent old/new seal workers, exact cold replay, and producer retries after local payload removal.
- Whole and partial JSON forks in both directions, preserving large-number lexemes and sealed indexes.
- Registration, dispatch, claims, scoped acks, and stale-token rejection across versions.
- Actual Realtime/SSE delivery of the other version's final append before EOF.
- Installation-wide admission across runtimes and prompt cancellation recovery.
- Tenant isolation and generation isolation across deletion/recreation.
- Recovery fencing of both running binaries, followed by fresh current/baseline deployments and continued producer state.

The test creates four runtime deployments in total, limits its installation to two live readers, and uses short SSE rotations and small chunks to exercise transitions. It records the actual Function Node version, CPU architecture, and memory, separately from the coordinator's Node version. These are correctness checks, not a production capacity benchmark.

The qualification report is successful only if all eight checks pass and cleanup completes. Focused runs cannot satisfy that gate. Cleanup deletes only the recorded Function slugs and dedicated schema, then removes the isolated package consumers; the surrounding test branch and its other Functions remain untouched. Required Neon CI preserves the sanitized report and retries recorded cleanup after runner/test interruption.
