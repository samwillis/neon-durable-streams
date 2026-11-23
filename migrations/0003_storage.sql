-- Chunk manifests and compact boundary directories. Frozen staging is owned
-- by immutable (stream_id, start_offset, end_offset] ranges.
CREATE FUNCTION __SCHEMA__.valid_directory(d integer[], n integer, bytes bigint, first_offset text, last_offset text)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT array_ndims(d)=1 AND array_lower(d,1)=1 AND cardinality(d)=n
    AND n BETWEEN 1 AND 1024 AND left(first_offset,16)=left(last_offset,16)
    AND d[n]::bigint=substring(last_offset,18)::bigint-substring(first_offset,18)::bigint
    AND d[n]::bigint=bytes+5::bigint*n
    AND NOT EXISTS (SELECT 1 FROM generate_subscripts(d,1) i
      WHERE d[i] IS NULL OR d[i]::bigint-CASE WHEN i=1 THEN 0 ELSE d[i-1] END < 6)
$$;

CREATE TABLE __SCHEMA__.chunks (
  id uuid PRIMARY KEY,
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  start_offset text COLLATE "C" NOT NULL,
  end_offset text COLLATE "C" NOT NULL,
  state text NOT NULL CHECK (state IN ('frozen', 'sealed')),
  frame_count integer NOT NULL CHECK (frame_count BETWEEN 1 AND 1024),
  payload_bytes bigint NOT NULL CHECK (payload_bytes > 0),
  end_deltas integer[],
  claim_token uuid,
  claim_epoch bigint NOT NULL DEFAULT 0,
  claim_until timestamptz,
  attempts bigint NOT NULL DEFAULT 0,
  object_key text,
  sha256 text,
  encoded_bytes integer,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  sealed_at timestamptz,
  UNIQUE (stream_id, id),
  UNIQUE (stream_id, end_offset),
  CHECK (start_offset < end_offset),
  CHECK (start_offset ~ '^[0-9]{16}_[0-9]{16}$' AND end_offset ~ '^[0-9]{16}_[0-9]{16}$'),
  CHECK ((state = 'frozen' AND object_key IS NULL AND sha256 IS NULL AND encoded_bytes IS NULL AND sealed_at IS NULL)
    OR (state = 'sealed' AND object_key IS NOT NULL AND sha256 ~ '^[0-9a-f]{64}$' AND encoded_bytes > 0 AND sealed_at IS NOT NULL)),
  retired_at timestamptz,
  CONSTRAINT chunk_claim CHECK ((claim_token IS NULL) = (claim_until IS NULL)),
  CONSTRAINT chunk_directory_shape CHECK
    (end_deltas IS NULL OR __SCHEMA__.valid_directory(end_deltas,frame_count,payload_bytes,start_offset,end_offset)),
  CONSTRAINT sealed_directory_required CHECK
    ((state='sealed' AND end_deltas IS NOT NULL) OR (state='frozen' AND end_deltas IS NULL))
);
CREATE INDEX chunks_pending ON __SCHEMA__.chunks (created_at) WHERE state = 'frozen';
CREATE INDEX chunks_retired ON __SCHEMA__.chunks (retired_at, id) WHERE retired_at IS NOT NULL;

-- Only the first append/rotation changes this queue, leaving stream counters
-- out of every index predicate so ordinary counter updates can be HOT.
CREATE TABLE __SCHEMA__.hot_chunks (
  stream_id uuid PRIMARY KEY REFERENCES __SCHEMA__.streams(id),
  due_at timestamptz NOT NULL
);
CREATE INDEX hot_chunks_due ON __SCHEMA__.hot_chunks(due_at,stream_id);

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
  collected_at timestamptz,
  orphan_candidate boolean NOT NULL DEFAULT false,
  quarantine_reason text,
  claim_token uuid,
  claim_epoch bigint NOT NULL DEFAULT 0,
  claim_until timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (collected_at IS NULL OR retired_at IS NOT NULL),
  CONSTRAINT object_claim CHECK ((claim_token IS NULL) = (claim_until IS NULL))
);
CREATE INDEX chunk_objects_chunk ON __SCHEMA__.chunk_objects (chunk_id);
CREATE INDEX chunk_objects_pending ON __SCHEMA__.chunk_objects (retired_at, object_key)
  WHERE retired_at IS NOT NULL AND collected_at IS NULL;
CREATE INDEX chunk_objects_orphans ON __SCHEMA__.chunk_objects (next_attempt_at, created_at, object_key)
  WHERE orphan_candidate AND retired_at IS NULL AND quarantine_reason IS NULL;

CREATE TABLE __SCHEMA__.reclamation_policy (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  enabled boolean NOT NULL DEFAULT false,
  inventory_cursor text,
  inventory_completed_at timestamptz,
  work_phase smallint NOT NULL DEFAULT 0 CHECK (work_phase BETWEEN 0 AND 5),
  metadata_phase smallint NOT NULL DEFAULT 0 CHECK (metadata_phase BETWEEN 0 AND 7),
  configured_recovery_token uuid
);
INSERT INTO __SCHEMA__.reclamation_policy (singleton) VALUES (true);

