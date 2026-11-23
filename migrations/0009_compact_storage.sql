-- The explicit resumable conversion must finish before canonical serving.
SELECT id FROM __SCHEMA__.installation FOR UPDATE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM __SCHEMA__.chunks WHERE state='sealed' AND end_deltas IS NULL)
    OR EXISTS (SELECT 1 FROM __SCHEMA__.appends a JOIN __SCHEMA__.chunks c ON c.id=a.chunk_id WHERE c.state='sealed') THEN
    RAISE EXCEPTION 'Run convertSealedStorage on the stopped installation before finalizing compact storage';
  END IF;
END $$;
UPDATE __SCHEMA__.installation SET serving_epoch=gen_random_uuid();
ALTER TABLE __SCHEMA__.chunks ADD CONSTRAINT sealed_directory_required CHECK
  ((state='sealed' AND end_deltas IS NOT NULL) OR (state='frozen' AND end_deltas IS NULL));
ALTER TABLE __SCHEMA__.appends ALTER COLUMN payload SET NOT NULL;
-- A frozen chunk owns its immutable (stream,start,end] staging range. The
-- stream lock and monotonic hot floor make per-frame assignment unnecessary.
ALTER TABLE __SCHEMA__.streams ADD COLUMN hot_start_offset text COLLATE "C" NOT NULL DEFAULT '0000000000000000_0000000000000000'
  CHECK (hot_start_offset ~ '^[0-9]{16}_[0-9]{16}$');
UPDATE __SCHEMA__.streams s SET hot_start_offset=COALESCE(
  (SELECT max(c.end_offset) FROM __SCHEMA__.chunks c WHERE c.stream_id=s.id),s.fork_offset,'0000000000000000_0000000000000000');
DROP INDEX __SCHEMA__.appends_hot;
DROP INDEX __SCHEMA__.appends_chunk;
-- Drops the redundant assignment FK/location check. stream_id retains its FK
-- and the primary (stream_id,end_offset) index serves all staging range work.
ALTER TABLE __SCHEMA__.appends DROP COLUMN chunk_id;
DROP INDEX __SCHEMA__.streams_hot;
ALTER TABLE __SCHEMA__.streams DROP COLUMN seal_after;

-- A stopped upgrade invalidates pre-upgrade wakes. Only signed v2 wake tokens
-- are issued/accepted by the canonical runtime; old callbacks remain fenced.
UPDATE __SCHEMA__.subscription_wakes SET state='fenced',delivery_claim=NULL,delivery_until=NULL WHERE state IN ('ready','delivered');
UPDATE __SCHEMA__.subscriptions SET current_wake=NULL,holder=NULL,lease_until=NULL,
  retry_count=0,retry_at=NULL,next_work_at=clock_timestamp() WHERE deleted_at IS NULL;
DROP TABLE __SCHEMA__.legacy_wake_tokens;
