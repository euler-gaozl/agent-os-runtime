-- An immutable supplement to execution history, never a fabricated tool call.
ALTER TABLE app_runs ADD COLUMN IF NOT EXISTS evidence_source_run text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS app_evidence_bindings(
 run_id text PRIMARY KEY REFERENCES app_runs(run_id),
 source_run_id text NOT NULL REFERENCES app_runs(run_id),
 source_result_sha256 text NOT NULL CHECK(source_result_sha256 ~ '^[a-f0-9]{64}$'),
 target_result_sha256 text NOT NULL CHECK(target_result_sha256 ~ '^[a-f0-9]{64}$'),
 mode text NOT NULL CHECK(mode IN ('submission','retrospective')),
 reason text NOT NULL CHECK(length(reason) BETWEEN 1 AND 500),
 manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='array' AND jsonb_array_length(manifest) BETWEEN 1 AND 256 AND octet_length(manifest::text)<=262144),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK(run_id<>source_run_id));
