-- Owner management decisions, separate from the fixed execution protocol.
-- Runtime can append/read; Workers receive no access. Retain historical outcomes.
CREATE TABLE IF NOT EXISTS run_continuation_receipts(
 request_id text PRIMARY KEY CHECK(length(request_id) BETWEEN 1 AND 128 AND request_id ~ '^[A-Za-z0-9_-]+$'),
 run_id text NOT NULL REFERENCES runs(id),
 request_hash text NOT NULL CHECK(request_hash ~ '^[0-9a-f]{64}$'),
 expected_failure_version text NOT NULL CHECK(expected_failure_version ~ '^[0-9a-f]{64}$'),
 http_status integer NOT NULL CHECK(http_status IN (202,409)),
 response jsonb NOT NULL CHECK(jsonb_typeof(response)='object' AND octet_length(response::text)<=8192),
 created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS run_continuation_receipts_run ON run_continuation_receipts(run_id,created_at,request_id);
