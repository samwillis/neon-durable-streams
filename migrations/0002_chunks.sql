CREATE TABLE __SCHEMA__.chunks (
  id uuid PRIMARY KEY,
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  start_offset text COLLATE "C" NOT NULL,
  end_offset text COLLATE "C" NOT NULL,
  state text NOT NULL CHECK (state IN ('frozen', 'sealed')),
  frame_count integer NOT NULL CHECK (frame_count BETWEEN 1 AND 1024),
  payload_bytes bigint NOT NULL CHECK (payload_bytes > 0),
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
    OR (state = 'sealed' AND object_key IS NOT NULL AND sha256 ~ '^[0-9a-f]{64}$' AND encoded_bytes > 0 AND sealed_at IS NOT NULL))
);
CREATE INDEX chunks_pending ON __SCHEMA__.chunks (created_at) WHERE state = 'frozen';

ALTER TABLE __SCHEMA__.appends ADD COLUMN chunk_id uuid;
ALTER TABLE __SCHEMA__.appends ADD COLUMN payload_bytes integer;
UPDATE __SCHEMA__.appends SET payload_bytes = octet_length(payload);
ALTER TABLE __SCHEMA__.appends ALTER COLUMN payload_bytes SET NOT NULL;
ALTER TABLE __SCHEMA__.appends ADD CONSTRAINT append_payload_bytes CHECK (payload_bytes > 0 AND (payload IS NULL OR octet_length(payload) = payload_bytes));
ALTER TABLE __SCHEMA__.appends ALTER COLUMN payload DROP NOT NULL;
ALTER TABLE __SCHEMA__.appends ADD CONSTRAINT append_chunk_fk
  FOREIGN KEY (stream_id, chunk_id) REFERENCES __SCHEMA__.chunks (stream_id, id);
ALTER TABLE __SCHEMA__.appends ADD CONSTRAINT append_payload_location CHECK (payload IS NOT NULL OR chunk_id IS NOT NULL);
CREATE INDEX appends_hot ON __SCHEMA__.appends (stream_id, end_offset) WHERE chunk_id IS NULL;
CREATE INDEX appends_chunk ON __SCHEMA__.appends (chunk_id, end_offset) WHERE chunk_id IS NOT NULL;

ALTER TABLE __SCHEMA__.streams ADD COLUMN hot_bytes bigint NOT NULL DEFAULT 0 CHECK (hot_bytes >= 0);
ALTER TABLE __SCHEMA__.streams ADD COLUMN hot_frames integer NOT NULL DEFAULT 0 CHECK (hot_frames >= 0);
ALTER TABLE __SCHEMA__.streams ADD COLUMN seal_after timestamptz NOT NULL DEFAULT clock_timestamp();
UPDATE __SCHEMA__.streams s SET hot_bytes = frames.bytes, hot_frames = frames.count
  FROM (SELECT stream_id, sum(payload_bytes) AS bytes, count(*) AS count FROM __SCHEMA__.appends GROUP BY stream_id) frames
  WHERE s.id = frames.stream_id;
CREATE INDEX streams_hot ON __SCHEMA__.streams (seal_after, id) WHERE hot_frames > 0;
ALTER TABLE __SCHEMA__.installation ADD COLUMN storage_identity text;
