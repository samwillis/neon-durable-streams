-- Stopped upgrade. Old binaries must be drained before this DDL. Keep the
-- installation's recovery state/token; epoch fencing also stops warm runtimes.
SELECT id FROM __SCHEMA__.installation FOR UPDATE;
UPDATE __SCHEMA__.installation SET serving_epoch = gen_random_uuid();
ALTER TABLE __SCHEMA__.chunks ADD COLUMN end_deltas integer[];

-- A bounded native directory, checked on publication and old-data conversion.
CREATE FUNCTION __SCHEMA__.valid_directory(d integer[], n integer, bytes bigint, first_offset text, last_offset text)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT array_ndims(d)=1 AND array_lower(d,1)=1 AND cardinality(d)=n
    AND n BETWEEN 1 AND 1024 AND left(first_offset,16)=left(last_offset,16)
    AND d[n]::bigint=substring(last_offset,18)::bigint-substring(first_offset,18)::bigint
    AND d[n]::bigint=bytes+5::bigint*n
    AND NOT EXISTS (SELECT 1 FROM generate_subscripts(d,1) i
      WHERE d[i] IS NULL OR d[i]::bigint-CASE WHEN i=1 THEN 0 ELSE d[i-1] END < 6)
$$;
ALTER TABLE __SCHEMA__.chunks ADD CONSTRAINT chunk_directory_shape CHECK
  (end_deltas IS NULL OR __SCHEMA__.valid_directory(end_deltas,frame_count,payload_bytes,start_offset,end_offset));

-- Only the first append/rotation changes this queue, leaving stream counters
-- out of every index predicate so ordinary counter updates can be HOT.
CREATE TABLE __SCHEMA__.hot_chunks (
  stream_id uuid PRIMARY KEY REFERENCES __SCHEMA__.streams(id),
  due_at timestamptz NOT NULL
);
CREATE INDEX hot_chunks_due ON __SCHEMA__.hot_chunks(due_at,stream_id);
INSERT INTO __SCHEMA__.hot_chunks SELECT id,seal_after+interval '1 hour' FROM __SCHEMA__.streams WHERE hot_frames>0;
