-- Subscription membership, durable wakes, signing keys, and change fanout.
CREATE TABLE __SCHEMA__.webhook_signing_keys (
  kid text PRIMARY KEY,
  public_jwk jsonb NOT NULL CHECK (NOT (public_jwk ? 'd')),
  private_key bytea NOT NULL,
  active boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX webhook_one_active_key ON __SCHEMA__.webhook_signing_keys (active) WHERE active;

CREATE TABLE __SCHEMA__.subscriptions (
  id uuid PRIMARY KEY,
  tenant_id text COLLATE "C" NOT NULL,
  name text COLLATE "C" NOT NULL,
  config jsonb NOT NULL,
  config_hash text NOT NULL,
  pattern_regex text,
  public_base_url text NOT NULL,
  generation bigint NOT NULL DEFAULT 0 CHECK (generation BETWEEN 0 AND 9007199254740991),
  current_wake uuid,
  holder text,
  lease_until timestamptz,
  retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  retry_at timestamptz,
  next_work_at timestamptz,
  member_count integer NOT NULL DEFAULT 0 CHECK (member_count >= 0),
  blocked_members integer NOT NULL DEFAULT 0 CHECK (blocked_members >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  deleted_at timestamptz,
  UNIQUE (tenant_id, id),
  CONSTRAINT subscription_lease_owner CHECK ((holder IS NULL) = (lease_until IS NULL)),
  CONSTRAINT subscription_lease_wake CHECK (current_wake IS NOT NULL OR holder IS NULL)
);
CREATE UNIQUE INDEX subscription_active_name ON __SCHEMA__.subscriptions (tenant_id, name) WHERE deleted_at IS NULL;
CREATE INDEX subscription_work_due ON __SCHEMA__.subscriptions (next_work_at, id) WHERE deleted_at IS NULL AND next_work_at IS NOT NULL;
CREATE INDEX subscriptions_tenant ON __SCHEMA__.subscriptions (tenant_id, id) WHERE deleted_at IS NULL;

CREATE TABLE __SCHEMA__.subscription_members (
  subscription_id uuid NOT NULL,
  tenant_id text COLLATE "C" NOT NULL,
  path text COLLATE "C" NOT NULL,
  stream_id uuid,
  explicit_link boolean NOT NULL DEFAULT false,
  glob_link boolean NOT NULL DEFAULT false,
  acked_offset text COLLATE "C" NOT NULL,
  PRIMARY KEY (subscription_id, path),
  FOREIGN KEY (tenant_id, subscription_id) REFERENCES __SCHEMA__.subscriptions (tenant_id, id),
  FOREIGN KEY (tenant_id, stream_id) REFERENCES __SCHEMA__.streams (tenant_id, id),
  CHECK (explicit_link OR glob_link),
  CHECK (__SCHEMA__.valid_offset(acked_offset))
);
CREATE INDEX subscription_members_stream ON __SCHEMA__.subscription_members (stream_id, subscription_id);

CREATE TABLE __SCHEMA__.subscription_wakes (
  id uuid PRIMARY KEY,
  subscription_id uuid NOT NULL,
  tenant_id text COLLATE "C" NOT NULL,
  generation bigint NOT NULL CHECK (generation BETWEEN 1 AND 9007199254740991),
  kind text NOT NULL CHECK (kind IN ('webhook', 'pull-wake')),
  token_version smallint NOT NULL DEFAULT 1 CHECK (token_version IN (1, 2)),
  token_hash bytea,
  token_cipher bytea,
  state text NOT NULL CHECK (state IN ('ready', 'delivered', 'completed', 'expired', 'fenced')),
  delivery_claim uuid,
  delivery_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (subscription_id, generation),
  UNIQUE (subscription_id, id),
  FOREIGN KEY (tenant_id, subscription_id) REFERENCES __SCHEMA__.subscriptions (tenant_id, id),
  CHECK ((token_hash IS NULL) = (token_cipher IS NULL)),
  CONSTRAINT wake_delivery_claim CHECK ((delivery_claim IS NULL) = (delivery_until IS NULL))
);
ALTER TABLE __SCHEMA__.subscriptions ADD CONSTRAINT current_wake_fk
  FOREIGN KEY (id, current_wake) REFERENCES __SCHEMA__.subscription_wakes (subscription_id, id);

CREATE TABLE __SCHEMA__.subscription_wake_streams (
  wake_id uuid NOT NULL REFERENCES __SCHEMA__.subscription_wakes (id),
  tenant_id text COLLATE "C" NOT NULL,
  path text COLLATE "C" NOT NULL,
  stream_id uuid,
  link_type text NOT NULL CHECK (link_type IN ('explicit', 'glob')),
  acked_offset text COLLATE "C" NOT NULL,
  tail_offset text COLLATE "C" NOT NULL,
  PRIMARY KEY (wake_id, path),
  FOREIGN KEY (tenant_id, stream_id) REFERENCES __SCHEMA__.streams (tenant_id, id),
  CHECK (acked_offset <= tail_offset)
);

CREATE TABLE __SCHEMA__.subscription_pending_links (
  subscription_id uuid NOT NULL,
  tenant_id text COLLATE "C" NOT NULL,
  stream_id uuid NOT NULL,
  before_offset text COLLATE "C" NOT NULL,
  due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (subscription_id, stream_id),
  FOREIGN KEY (tenant_id, subscription_id) REFERENCES __SCHEMA__.subscriptions (tenant_id, id),
  FOREIGN KEY (tenant_id, stream_id) REFERENCES __SCHEMA__.streams (tenant_id, id)
);
CREATE INDEX subscription_pending_links_due ON __SCHEMA__.subscription_pending_links (due_at, subscription_id, stream_id);

-- One bounded scalar preserves increasing generations across name pruning.
CREATE SEQUENCE __SCHEMA__.wake_generations AS bigint MINVALUE 1 MAXVALUE 9007199254740991 NO CYCLE;

CREATE TABLE __SCHEMA__.stream_changes (
  stream_id uuid NOT NULL REFERENCES __SCHEMA__.streams (id),
  version bigint NOT NULL,
  before_offset text COLLATE "C" NOT NULL,
  after_offset text COLLATE "C" NOT NULL,
  kind text NOT NULL CHECK (kind IN ('created', 'appended', 'closed', 'deleted', 'expired')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  fanout_after uuid,
  PRIMARY KEY (stream_id, version)
);
CREATE INDEX stream_changes_pending ON __SCHEMA__.stream_changes (created_at);
CREATE INDEX stream_changes_stream_pending ON __SCHEMA__.stream_changes (stream_id, version);
