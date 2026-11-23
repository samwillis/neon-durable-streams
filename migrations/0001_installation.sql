-- Installation identity, recovery fencing, and shared live-reader admission.
CREATE TABLE __SCHEMA__.installation (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  id uuid NOT NULL UNIQUE,
  storage_version integer NOT NULL CHECK (storage_version = 3),
  storage_identity text,
  serving_epoch uuid NOT NULL DEFAULT gen_random_uuid(),
  serving_state text NOT NULL DEFAULT 'active' CHECK (serving_state IN ('active', 'recovering')),
  environment_id text CHECK (octet_length(environment_id) BETWEEN 1 AND 2048),
  recovery_token uuid,
  recovery_source_epoch uuid,
  completed_recovery_token uuid,
  recovery_target_environment text,
  recovery_target_storage text,
  recovery_target_public_base_url text,
  recovery_cursor uuid,
  recovery_verified bigint NOT NULL DEFAULT 0,
  recovery_complete boolean NOT NULL DEFAULT false,
  recovery_started_at timestamptz,
  CONSTRAINT recovery_progress CHECK (recovery_verified >= 0),
  CONSTRAINT recovery_operation CHECK (
    (serving_state = 'active' AND recovery_token IS NULL)
    OR (serving_state = 'recovering' AND recovery_token IS NOT NULL
      AND recovery_source_epoch IS NOT NULL AND recovery_target_environment IS NOT NULL
      AND recovery_started_at IS NOT NULL))
);

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
