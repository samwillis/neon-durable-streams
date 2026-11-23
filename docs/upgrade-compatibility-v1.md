# Upgrade boundaries

The package is unreleased and remains `private: true`. Source revisions and artifact hashes identify tested binaries; the prerelease package version alone does not identify a qualified pair. The current runtime serves only canonical schema 0009 with object format v1.

| Boundary | Procedure |
| --- | --- |
| Fresh installation | Explicit checksummed migrations 0001–0009, environment/storage binding, and external subscription keyring initialization. |
| Old layout → 0009 | Stop ingress, workers, instances, timers and possible cold starts. Prepare migration 0008, run bounded `convertSealedStorage()` batches, then finalize 0009. See [compact storage](compact-storage-v2.md). |
| Old/current binary overlap across this upgrade | Unsupported. Preparation/finalization change serving epochs, and the new runtime rejects old/intermediate migration histories. Pre-0005 binaries lack the epoch guard and must be fully stopped. |
| Binary downgrade after conversion | Unsupported. Old frame rows have been removed. Restore matching pre-upgrade database/objects if rollback is needed. |
| Same-schema replacement | Use only a specifically tested artifact pair. Drain old traffic/work and preserve identities, auth policy, external keys, limits, and the common object store. A schema number alone does not qualify arbitrary binaries. |
| Branch/point-in-time recovery | Use the [quiesced recovery procedure](operations-v1.md) and [in-place restore runbook](inplace-restore-v1.md), restoring every referenced immutable object's original bytes. |

Existing migrations 0001–0007 and immutable v1 objects are preserved. Conversion verifies active objects against their old exact indexes before atomically writing directories and deleting rows. Retired metadata with already collected objects can convert from complete retained indexes. Missing/corrupt active objects or incomplete indexes keep the upgrade unfinished. No object network I/O holds SQL locks.

## Packed upgrade qualification

`scripts/neon-test-rolling-artifacts.mjs` builds the pinned schema-0007 baseline `1ba871b2495ceb0a988676c314886384446506a2` and a clean snapshot of the candidate in independent directories. Each is packed and installed into a separate consumer; build processes receive no cloud credentials. Reports retain source, archive, tarball, lockfile, and Function bundle hashes.

`test/neon/rolling.test.ts` now tests a **stopped layout upgrade**, followed by same-candidate multi-instance behavior. It does not claim old/current rolling compatibility across schemas:

1. Deploy the actual baseline artifact on schema 0007, write/ seal v1 payloads, retain a fork at an interior frame boundary, preserve exact-number JSON, producer state, and an active scoped wake.
2. Delete the baseline deployment and stop its possible cold starts. Prepare 0008, convert bounded chunks, finalize 0009, and complete explicit recovery before reopening.
3. Deploy two candidate instances. Verify exact old bytes/forks/JSON and producer deduplication, rejected old-epoch callbacks, and a fresh increasing signed wake generation.
4. Run eight additional multi-instance checks: concurrent producers/fencing, concurrent seal workers/cold replay, full/partial inherited JSON forks, subscription coordination, Realtime final-bytes-before-EOF, shared admission/cancellation, tenant/generation isolation, and recovery fencing with fresh candidate deployments.

The fixture records actual Function Node/architecture/memory separately from coordinator Node. It uses a dedicated schema, short SSE rotations, and a two-reader installation limit. Success requires the stopped upgrade, all eight checks, and cleanup. These are correctness checks, not capacity evidence. Required CI preserves the sanitized report and retries recorded cleanup after interruption.

## Historical rolling evidence

The archived schema-0005 pair `b22c1675f7eb5c32b52f39be39279c819ea5f21f` → `01643614c6ba142ef945e5a3c3607731293cf875` passed eight mixed-version checks through independently packed Neon Functions, including same-schema rollback. Those reports remain in the [qualification index](qualification/README.md). They qualify those artifacts on schema 0005 and do not imply that today's canonical runtime serves that layout.

The earlier stopped schema-0006 → 0007 qualification is likewise historical. It removed application PITR windows and disabled collection pending an explicit opt-in under provider/backup-owned object history. The compact layout preserves that recovery contract.
