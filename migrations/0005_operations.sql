ALTER TABLE __SCHEMA__.installation
  ADD COLUMN serving_epoch uuid NOT NULL DEFAULT gen_random_uuid(),
  ADD COLUMN serving_state text NOT NULL DEFAULT 'active' CHECK (serving_state IN ('active', 'recovering')),
  ADD COLUMN environment_id text CHECK (octet_length(environment_id) BETWEEN 1 AND 2048),
  ADD COLUMN recovery_token uuid,
  ADD COLUMN recovery_source_epoch uuid,
  ADD COLUMN completed_recovery_token uuid,
  ADD COLUMN recovery_target_environment text,
  ADD COLUMN recovery_target_storage text,
  ADD COLUMN recovery_target_public_base_url text,
  ADD COLUMN recovery_cursor uuid,
  ADD COLUMN recovery_verified bigint NOT NULL DEFAULT 0,
  ADD COLUMN recovery_complete boolean NOT NULL DEFAULT false,
  ADD COLUMN recovery_started_at timestamptz;

CREATE TABLE __SCHEMA__.live_capacity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  max_readers integer NOT NULL CHECK (max_readers BETWEEN 1 AND 100000)
);
INSERT INTO __SCHEMA__.live_capacity (max_readers) VALUES (80);
CREATE TABLE __SCHEMA__.live_leases (
  id uuid PRIMARY KEY,
  serving_epoch uuid NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE INDEX live_leases_expiry ON __SCHEMA__.live_leases (expires_at);
