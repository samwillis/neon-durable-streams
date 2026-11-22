ALTER TABLE __SCHEMA__.streams ADD CONSTRAINT stream_tenant_id_unique UNIQUE (tenant_id, id);
ALTER TABLE __SCHEMA__.streams ADD COLUMN source_id uuid;
ALTER TABLE __SCHEMA__.streams ADD COLUMN fork_offset text COLLATE "C";
ALTER TABLE __SCHEMA__.streams ADD COLUMN fork_sub_offset bigint NOT NULL DEFAULT 0 CHECK (fork_sub_offset BETWEEN 0 AND 9007199254740991);
ALTER TABLE __SCHEMA__.streams ADD COLUMN fork_depth integer NOT NULL DEFAULT 0 CHECK (fork_depth BETWEEN 0 AND 128);
ALTER TABLE __SCHEMA__.streams ADD COLUMN edge_active boolean NOT NULL DEFAULT false;
ALTER TABLE __SCHEMA__.streams ADD COLUMN ref_count integer NOT NULL DEFAULT 0 CHECK (ref_count >= 0);
ALTER TABLE __SCHEMA__.streams ADD COLUMN cleanup_pending boolean NOT NULL DEFAULT false;
ALTER TABLE __SCHEMA__.streams ADD CONSTRAINT fork_source_fk FOREIGN KEY (tenant_id, source_id) REFERENCES __SCHEMA__.streams (tenant_id, id);
ALTER TABLE __SCHEMA__.streams ADD CONSTRAINT fork_configuration CHECK (
  (source_id IS NULL AND fork_offset IS NULL AND fork_sub_offset = 0 AND fork_depth = 0 AND NOT edge_active)
  OR (source_id IS NOT NULL AND source_id <> id AND fork_offset ~ '^[0-9]{16}_[0-9]{16}$' AND fork_depth > 0)
);
CREATE INDEX forks_by_source ON __SCHEMA__.streams (source_id) WHERE edge_active;
CREATE INDEX retired_without_forks ON __SCHEMA__.streams (deleted_at, id) WHERE cleanup_pending AND ref_count = 0;

CREATE TABLE __SCHEMA__.stream_expirations (
  stream_id uuid PRIMARY KEY REFERENCES __SCHEMA__.streams (id),
  due_at timestamptz NOT NULL
);
CREATE INDEX stream_expirations_due ON __SCHEMA__.stream_expirations (due_at, stream_id);
INSERT INTO __SCHEMA__.stream_expirations (stream_id, due_at)
  SELECT id, COALESCE(expires_at, accessed_at + ttl_seconds * interval '1 second')
  FROM __SCHEMA__.streams WHERE deleted_at IS NULL AND (expires_at IS NOT NULL OR ttl_seconds IS NOT NULL);
