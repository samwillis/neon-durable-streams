# Fork and lifecycle contract

A fork records its immutable source generation UUID, anchor offset, original sub-offset, and ancestry depth. It inherits capped source ranges and appends into its own offset space starting at the divergence point. Every ancestor range is capped again at the next edge, so later source appends never enter a fork. Source deletion/expiry does not alter inherited bytes or affect the fork's independent lifetime.

Replay captures root metadata, boundary validation, and a bounded lineage/frame plan in one REPEATABLE READ snapshot. Each ancestor contributes at most one page of candidate frame metadata before the final page budget is selected. Object downloads happen after that SQL snapshot closes. Their manifests identify the original owning stream UUID. JSON framing preserves original numeric lexemes across inherited, partial, and new frames.

## Creation

Fork creation requires target-create authorization and source-read authorization under the same authenticated tenant. Same-server source paths are canonicalized through the stream mount. Fork-specific position headers without a source are rejected. The source content type is inherited when omitted; explicit mismatches return 409. Expiry follows the pinned protocol's independent-lifetime table. Producer state and Stream-Seq are always fresh.

Stream rows use `FOR NO KEY UPDATE` because identity keys never change; subscription foreign-key checks can proceed alongside a writer. Both path claims are inserted and locked in binary path order before any stream row is locked. Source validation, target idempotency checks, parent reference acquisition, child creation, local initial frames, and change outbox publication are transactional. A matching repeated PUT does not acquire another source reference. An omitted fork offset is resolved once at creation and need not be re-resolved against a later source tail on reconfirmation. Explicit sub-offset zero is equivalent to omission.

A nonzero sub-offset inherits a prefix of the next stored frame: bytes for non-JSON, flattened message count for JSON. Prefix preparation captures that frame under the initial source-generation check, commits the SQL plan, and fetches any sealed bytes outside write locks. A second transaction revalidates both path generations and configurations before acquiring the parent edge and publishing the child-owned prefix. A replaced source generation returns 409; no reference or partial child leaks. This is a two-phase preparation protocol, without automatic retry of a creation commit whose response might have been lost.

Each prefix is at most one configured stored frame. Initial PUT bytes become a following child-owned frame. New offsets advance by the stored byte length plus five, preserving both ordinary read resumability and the source's original sub-offset for idempotent reconfirmation.

Ancestry depth is bounded to 128. Attempts beyond that bound fail with 413. This is an explicit resource limit; deployment measurements do not yet establish a capacity target for deep fork trees.

## Deletion and expiry

A stream with dependent forks becomes soft-deleted: direct access returns 410 and PUT returns 409. Its own source edge remains active while descendants need it. A stream with no dependents clears its own path pointer and releases its direct parent reference. UUID generations and immutable data remain available internally.

Detaching an edge updates only the direct parent's reference count while the child's own path/stream locks are held. It never acquires an ancestor path lock. Logical cleanup of a soft-deleted parent whose count reaches zero runs in a separate bounded maintenance transaction: lock that parent's path, recheck its stream, clear its pointer, and release its own parent edge. This permits cascading cleanup without reversing the path-lock order. Clients can observe 410 until maintenance changes it to 404, as allowed by the protocol.

Expiring streams have an indexed durable deadline row maintained in the same transaction as creation, TTL renewal, or retirement. Successful GET renews TTL at request start. Maintenance selects a bounded set of due deadlines and retired zero-reference streams, then rechecks lifecycle state under the normal path/stream locks. It does not scan every retained generation on each recovery tick. Absolute deadlines never move. Fork reads do not renew their source's TTL.

Physical object/frame deletion is disabled by default. Schema-0006 adds sealed replay pins, exact upload intents, publication/retirement barriers, and scoped collection. Schema 0007 delegates historical object recovery to Neon or matching backups. Old stream offsets remain available while a current stream or fork needs that history. A live descendant retains its parent generation, including the ability to seal that deleted parent’s HOT bytes. See the [reclamation contract](reclamation-v1.md) for bounded plan lifetimes and collection eligibility.

## Evidence

All 82 pinned upstream fork cases pass on real Neon with Realtime and background Object Storage sealing. Six further Neon tests cover concurrent duplicate partial forks, source deletion racing creation, source replacement during sealed-prefix preparation, multi-level cleanup, a captured sealed replay racing cleanup/recreation, and source authorization/tenant isolation. The additional tests assert stored reference counts against the active edge graph. They supplement the unfiltered full-suite gate.
