-- Stream generations, fork references, hot/frozen appends, and producer sessions.
CREATE TABLE __SCHEMA__.paths (
  tenant_id text COLLATE "C" NOT NULL CHECK (octet_length(tenant_id) BETWEEN 1 AND 512),
  path text COLLATE "C" NOT NULL CHECK (octet_length(path) BETWEEN 1 AND 2048),
  active_stream_id uuid,
  PRIMARY KEY (tenant_id, path)
);

CREATE TABLE __SCHEMA__.streams (
  id uuid PRIMARY KEY,
  tenant_id text COLLATE "C" NOT NULL,
  path text COLLATE "C" NOT NULL,
  content_type text NOT NULL,
  tail_offset text COLLATE "C" NOT NULL DEFAULT '0000000000000000_00000000000000000000000000000000'
    CHECK (tail_offset ~ '^[0-9]{16}_[0-9a-f]{32}$'),
  write_epoch uuid NOT NULL,
  write_history uuid NOT NULL,
  hot_bytes bigint NOT NULL DEFAULT 0 CHECK (hot_bytes >= 0),
  hot_frames integer NOT NULL DEFAULT 0 CHECK (hot_frames >= 0),
  hot_start_offset text COLLATE "C" NOT NULL DEFAULT '0000000000000000_00000000000000000000000000000000'
    CHECK (hot_start_offset ~ '^[0-9]{16}_[0-9a-f]{32}$'),
  change_version bigint NOT NULL DEFAULT 0 CHECK (change_version >= 0),
  last_seq text COLLATE "C",
  closed boolean NOT NULL DEFAULT false,
  closed_by jsonb,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  accessed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ttl_seconds integer CHECK (ttl_seconds >= 0),
  expires_at timestamptz,
  source_id uuid,
  fork_offset text COLLATE "C",
  fork_sub_offset bigint NOT NULL DEFAULT 0 CHECK (fork_sub_offset BETWEEN 0 AND 9007199254740991),
  fork_depth integer NOT NULL DEFAULT 0 CHECK (fork_depth BETWEEN 0 AND 128),
  edge_active boolean NOT NULL DEFAULT false,
  ref_count integer NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
  cleanup_pending boolean NOT NULL DEFAULT false,
  CHECK (ttl_seconds IS NULL OR expires_at IS NULL),
  UNIQUE (tenant_id, path, id),
  FOREIGN KEY (tenant_id, path) REFERENCES __SCHEMA__.paths (tenant_id, path),
  CONSTRAINT stream_tenant_id_unique UNIQUE (tenant_id, id),
  CONSTRAINT fork_source_fk FOREIGN KEY (tenant_id, source_id) REFERENCES __SCHEMA__.streams (tenant_id, id),
  CONSTRAINT fork_configuration CHECK (
    (source_id IS NULL AND fork_offset IS NULL AND fork_sub_offset = 0 AND fork_depth = 0 AND NOT edge_active)
    OR (source_id IS NOT NULL AND source_id <> id AND fork_offset ~ '^[0-9]{16}_[0-9a-f]{32}$' AND fork_depth > 0)
  )
);

ALTER TABLE __SCHEMA__.paths ADD CONSTRAINT active_stream_fk
  FOREIGN KEY (tenant_id, path, active_stream_id) REFERENCES __SCHEMA__.streams (tenant_id, path, id);

CREATE INDEX forks_by_source ON __SCHEMA__.streams (source_id) WHERE edge_active;
CREATE INDEX retired_without_forks ON __SCHEMA__.streams (deleted_at, id) WHERE cleanup_pending AND ref_count = 0;

CREATE INDEX streams_reclaimable ON __SCHEMA__.streams (deleted_at, id)
  WHERE deleted_at IS NOT NULL AND ref_count = 0 AND NOT edge_active AND NOT cleanup_pending;
CREATE INDEX forks_all_sources ON __SCHEMA__.streams (source_id) WHERE source_id IS NOT NULL;

-- Sealed frames live in object storage; only hot/frozen payloads remain here.
CREATE TABLE __SCHEMA__.appends (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  start_offset text COLLATE "C" NOT NULL,
  end_offset text COLLATE "C" NOT NULL,
  payload bytea NOT NULL CHECK (octet_length(payload) > 0),
  payload_bytes integer NOT NULL,
  message_ends integer[],
  PRIMARY KEY (stream_id, end_offset),
  CHECK (start_offset < end_offset),
  CHECK (start_offset ~ '^[0-9]{16}_[0-9a-f]{32}$' AND end_offset ~ '^[0-9]{16}_[0-9a-f]{32}$'),
  CHECK (message_ends IS NULL OR (cardinality(message_ends) > 0 AND message_ends[cardinality(message_ends)] = octet_length(payload))),
  CONSTRAINT append_payload_bytes CHECK (payload_bytes > 0 AND (payload IS NULL OR octet_length(payload) = payload_bytes))
);

CREATE TABLE __SCHEMA__.producers (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  producer_id text COLLATE "C" NOT NULL,
  epoch bigint NOT NULL CHECK (epoch BETWEEN 0 AND 9007199254740991),
  last_seq bigint NOT NULL CHECK (last_seq BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY (stream_id, producer_id)
);

CREATE TABLE __SCHEMA__.stream_expirations (
  stream_id uuid PRIMARY KEY REFERENCES __SCHEMA__.streams (id),
  due_at timestamptz NOT NULL
);
CREATE INDEX stream_expirations_due ON __SCHEMA__.stream_expirations (due_at, stream_id);
