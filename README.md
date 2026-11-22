# Durable Streams server library for Neon

Design research dated **22 November 2026** for a production-grade TypeScript library. Users embed the library in their own server, deploy it to their own Neon Function, and operate their own Lakebase Postgres, Object Storage, and Realtime resources.

**Target:** full Durable Streams protocol conformance, durable recovery, safe upgrades, tenant isolation, bounded resource use, and documented operating limits. Production readiness requires evidence from fault testing, deployment tests, restore drills, rolling upgrades, and workload benchmarks, in addition to the conformance suite.

Start with the [production library design](docs/production-library.md) for the proposed package API, ownership boundaries, reliability contract, migration and recovery design, and release gates. The [protocol and platform investigation](docs/investigation.md) contains the underlying protocol/storage analysis. The [source manifest](docs/research-sources.json) records upstream source revisions and retrieved documentation hashes.

The [plan review](docs/plan-review.md) finds enough direction to start, with explicit transaction, lifecycle, subscription, and reclamation contracts to settle before those implementations are frozen. The [implementation plan](docs/implementation-plan.md) gives the current milestone order and acceptance criteria.

The main findings are:

- Current protocol scope includes recursive stream forks and a subscription control plane, alongside append, replay, long-poll, SSE, JSON mode, closure, TTL, and idempotent producers.
- The conformance CLI leaves six subscription tests disabled. The full target needs a programmatic harness with `subscriptions: true`.
- Neon Functions currently defaults to **100 concurrent invocations per account**. Treat open live reads as consumers of that budget until deployment measurements establish otherwise.
- Realtime requires **Postgres 18**, is in beta, and should signal changes to durable state. Reads should resume from persisted offsets.
- Sealing needs a recoverable Postgres-to-object-storage handoff. A database restore leaves Object Storage unchanged, so chunk retention must cover recoverable database history.
- The upstream Cloudflare server is already distributed as an embeddable library, providing a useful API precedent. Its platform coordination mechanisms need a transactional Postgres implementation here.
- Neon's published SLA covers Postgres compute endpoints; it does not establish an availability guarantee for the complete Functions/Storage/Realtime stack.

The quality target is a production library. Current status is design research: no library implementation, deployment, or passing test report exists yet.
