CREATE TABLE __SCHEMA__.installation (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  id uuid NOT NULL UNIQUE,
  storage_version integer NOT NULL CHECK (storage_version = 1)
);

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
  tail_offset text COLLATE "C" NOT NULL DEFAULT '0000000000000000_0000000000000000'
    CHECK (tail_offset ~ '^[0-9]{16}_[0-9]{16}$'),
  change_version bigint NOT NULL DEFAULT 0 CHECK (change_version >= 0),
  last_seq text COLLATE "C",
  closed boolean NOT NULL DEFAULT false,
  closed_by jsonb,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  accessed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ttl_seconds integer CHECK (ttl_seconds >= 0),
  expires_at timestamptz,
  CHECK (ttl_seconds IS NULL OR expires_at IS NULL),
  UNIQUE (tenant_id, path, id),
  FOREIGN KEY (tenant_id, path) REFERENCES __SCHEMA__.paths (tenant_id, path)
);

ALTER TABLE __SCHEMA__.paths ADD CONSTRAINT active_stream_fk
  FOREIGN KEY (tenant_id, path, active_stream_id) REFERENCES __SCHEMA__.streams (tenant_id, path, id);

CREATE TABLE __SCHEMA__.appends (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  start_offset text COLLATE "C" NOT NULL,
  end_offset text COLLATE "C" NOT NULL,
  payload bytea NOT NULL CHECK (octet_length(payload) > 0),
  message_ends integer[],
  PRIMARY KEY (stream_id, end_offset),
  CHECK (start_offset < end_offset),
  CHECK (start_offset ~ '^[0-9]{16}_[0-9]{16}$' AND end_offset ~ '^[0-9]{16}_[0-9]{16}$'),
  CHECK (message_ends IS NULL OR (cardinality(message_ends) > 0 AND message_ends[cardinality(message_ends)] = octet_length(payload)))
);

CREATE TABLE __SCHEMA__.producers (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  producer_id text COLLATE "C" NOT NULL,
  epoch bigint NOT NULL CHECK (epoch BETWEEN 0 AND 9007199254740991),
  last_seq bigint NOT NULL CHECK (last_seq BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY (stream_id, producer_id)
);

CREATE TABLE __SCHEMA__.stream_changes (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  version bigint NOT NULL,
  before_offset text COLLATE "C" NOT NULL,
  after_offset text COLLATE "C" NOT NULL,
  kind text NOT NULL CHECK (kind IN ('created', 'appended', 'closed', 'deleted', 'expired')),
  processed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (stream_id, version)
);
CREATE INDEX stream_changes_pending ON __SCHEMA__.stream_changes (created_at)
  WHERE NOT processed;
