# Installation and compatibility boundaries

The package is unreleased and remains `private: true`. The current [four-file baseline](migrations.md) installs the compact schema with immutable object format v1. Source revisions and artifact hashes identify tested binaries; the prerelease package version alone does not identify a qualified pair.

| Boundary | Procedure |
| --- | --- |
| Fresh installation | Explicit checksummed baseline, environment/storage binding, and external subscription keyring initialization. |
| Earlier development migration histories | Use a fresh schema. Current migrations reject old receipts without rewriting or dropping existing data. Archived SQL and conversion tooling remain in Git history. |
| Same-schema replacement | Use only a specifically tested artifact pair. Drain old traffic/work and preserve identities, auth policy, external keys, limits, and the common object store. |
| Branch/point-in-time recovery | Use the [quiesced recovery procedure](operations-v1.md) and [in-place restore runbook](inplace-restore-v1.md), restoring every referenced immutable object's original bytes. |

Future released migrations must retain applied checksums and document compatibility. The present consolidation is an explicit reset of the unpublished development baseline, not an upgrade or adoption mechanism.

## Packed multi-instance qualification

`scripts/neon-test-rolling-artifacts.mjs` builds, packs, and installs the candidate into an independent consumer directory without cloud credentials in build processes. Reports retain source, tarball, lockfile, and Function bundle hashes. An optional full commit argument builds an additional archived consumer for research comparisons; normal CI uses only the candidate.

`test/neon/rolling.test.ts` deploys two candidate Functions against a fresh baseline and checks concurrent producer deduplication/fencing, sealing and cold replay, full/partial inherited JSON forks, subscription coordination, Realtime final-bytes-before-EOF, shared admission/cancellation, tenant/generation isolation, and recovery fencing with fresh deployments.

The fixture records actual Function Node/architecture/memory separately from coordinator Node. It uses a dedicated schema, short SSE rotations, and a two-reader installation limit. Success requires all eight checks and cleanup. These are correctness checks; they establish neither cross-version compatibility nor a capacity target. CI preserves the sanitized report and retries recorded cleanup after interruption.

## Historical qualification

Archived schema-0005 rolling checks and stopped schema-0006/0007/0009 upgrades remain in the [qualification index](qualification/README.md), scoped to their original source and artifact hashes. Their migration and conversion paths are available at those Git revisions. They do not qualify current binaries against archived layouts. The current baseline preserves the final compact storage, provider-owned historical object recovery, and current-reference safety contracts.
