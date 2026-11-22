-- A schema-0005 runtime can cache readiness. Fence it before this schema can
-- admit readers whose object plans require durable replay pins.
UPDATE __SCHEMA__.installation SET serving_epoch = gen_random_uuid();

ALTER TABLE __SCHEMA__.chunks ADD COLUMN retired_at timestamptz;
ALTER TABLE __SCHEMA__.chunks ADD COLUMN collect_after timestamptz;
CREATE INDEX chunks_retired ON __SCHEMA__.chunks (retired_at, id) WHERE retired_at IS NOT NULL;
CREATE INDEX streams_reclaimable ON __SCHEMA__.streams (deleted_at, id)
  WHERE deleted_at IS NOT NULL AND ref_count = 0 AND NOT edge_active AND NOT cleanup_pending;
ALTER TABLE __SCHEMA__.streams ADD COLUMN reclaim_after timestamptz;
CREATE INDEX forks_all_sources ON __SCHEMA__.streams (source_id) WHERE source_id IS NOT NULL;

CREATE TABLE __SCHEMA__.replay_pins (
  id uuid NOT NULL,
  chunk_id uuid NOT NULL REFERENCES __SCHEMA__.chunks (id) ON DELETE CASCADE,
  serving_epoch uuid NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (id, chunk_id)
);
CREATE INDEX replay_pins_chunk_expiry ON __SCHEMA__.replay_pins (chunk_id, expires_at);
CREATE INDEX replay_pins_expiry ON __SCHEMA__.replay_pins (expires_at, id);

-- This ledger deliberately has no FK to a generation/chunk. It retains upload
-- intent and deletion retry identity after metadata cleanup or orphan discovery.
CREATE TABLE __SCHEMA__.chunk_objects (
  object_key text PRIMARY KEY,
  stream_id uuid NOT NULL,
  chunk_id uuid NOT NULL,
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  encoded_bytes integer NOT NULL CHECK (encoded_bytes BETWEEN 1 AND 134217728),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  retired_at timestamptz,
  collect_after timestamptz,
  collected_at timestamptz,
  orphan_candidate boolean NOT NULL DEFAULT false,
  quarantine_reason text,
  claim_token uuid,
  claim_epoch bigint NOT NULL DEFAULT 0,
  claim_until timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (collected_at IS NULL OR retired_at IS NOT NULL)
);
CREATE INDEX chunk_objects_chunk ON __SCHEMA__.chunk_objects (chunk_id);
CREATE INDEX chunk_objects_pending ON __SCHEMA__.chunk_objects (retired_at, object_key)
  WHERE retired_at IS NOT NULL AND collected_at IS NULL;
CREATE INDEX chunk_objects_due ON __SCHEMA__.chunk_objects (collect_after, object_key)
  WHERE retired_at IS NOT NULL AND collected_at IS NULL AND NOT orphan_candidate AND quarantine_reason IS NULL;
CREATE INDEX chunk_objects_orphans ON __SCHEMA__.chunk_objects (next_attempt_at, created_at, object_key)
  WHERE orphan_candidate AND retired_at IS NULL AND quarantine_reason IS NULL;

CREATE TABLE __SCHEMA__.reclamation_policy (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  enabled boolean NOT NULL DEFAULT false,
  recovery_window_ms bigint NOT NULL DEFAULT 691200000 CHECK (recovery_window_ms BETWEEN 0 AND 315576000000),
  safety_margin_ms integer NOT NULL DEFAULT 900000 CHECK (safety_margin_ms BETWEEN 900000 AND 2147483647),
  hold_until timestamptz NOT NULL DEFAULT clock_timestamp(),
  inventory_cursor text,
  inventory_completed_at timestamptz,
  work_phase smallint NOT NULL DEFAULT 0 CHECK (work_phase BETWEEN 0 AND 5),
  metadata_phase smallint NOT NULL DEFAULT 0 CHECK (metadata_phase BETWEEN 0 AND 7),
  configured_recovery_token uuid
);
INSERT INTO __SCHEMA__.reclamation_policy (singleton) VALUES (true);

ALTER TABLE __SCHEMA__.subscription_wakes ADD COLUMN token_version smallint NOT NULL DEFAULT 1 CHECK (token_version IN (1, 2));
-- One bounded scalar preserves increasing generations across name pruning.
CREATE SEQUENCE __SCHEMA__.wake_generations AS bigint MINVALUE 1 MAXVALUE 9007199254740991 NO CYCLE;
SELECT setval('__SCHEMA__.wake_generations', GREATEST(1, COALESCE(max(generation),0)), COALESCE(max(generation),0)>0) FROM __SCHEMA__.subscriptions;
-- Finite legacy receipts retain correct fencing after pre-0006 wake pruning.
CREATE TABLE __SCHEMA__.legacy_wake_tokens (
  wake_id uuid PRIMARY KEY,
  subscription_id uuid NOT NULL,
  subscription_name text NOT NULL,
  token_hash bytea NOT NULL CHECK (octet_length(token_hash)=32)
);
