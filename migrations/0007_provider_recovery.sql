-- Historical object recovery belongs to the storage provider. This is a
-- stopped upgrade: fence cached schema-0006 runtimes and require an explicit
-- collection opt-in under the new recovery contract.
UPDATE __SCHEMA__.installation SET serving_epoch = gen_random_uuid();
UPDATE __SCHEMA__.reclamation_policy SET enabled = false, configured_recovery_token = NULL;

ALTER TABLE __SCHEMA__.reclamation_policy
  DROP COLUMN recovery_window_ms,
  DROP COLUMN safety_margin_ms,
  DROP COLUMN hold_until;
ALTER TABLE __SCHEMA__.chunks DROP COLUMN collect_after;
ALTER TABLE __SCHEMA__.chunk_objects DROP COLUMN collect_after;
ALTER TABLE __SCHEMA__.streams DROP COLUMN reclaim_after;
