-- Agent-scoped persistent notes belong to the execution domain. Effects,
-- call receipts and run fencing must commit in the same transaction.
CREATE TABLE IF NOT EXISTS documents(
 id text PRIMARY KEY, kind text NOT NULL, title text NOT NULL, content text NOT NULL,
 source text NOT NULL, metadata jsonb NOT NULL, supersedes text REFERENCES documents(id),
 created_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE documents ADD COLUMN IF NOT EXISTS scope text NOT NULL DEFAULT '';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS valid_until timestamptz;
CREATE TABLE IF NOT EXISTS runs(
 id text PRIMARY KEY, submission_key text UNIQUE NOT NULL, input_hash text NOT NULL,
 parent_id text REFERENCES runs(id), snapshot jsonb NOT NULL, task text NOT NULL,
 status text NOT NULL DEFAULT 'queued', step integer NOT NULL DEFAULT 0,
 state jsonb NOT NULL DEFAULT '{}', constraints jsonb NOT NULL DEFAULT '[]', rule_revision bigint NOT NULL DEFAULT 1,
 owner text NOT NULL DEFAULT '', fence bigint NOT NULL DEFAULT 0,
 lease_until timestamptz, cancel_requested boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS runs_claim ON runs(status,lease_until,created_at);
CREATE INDEX IF NOT EXISTS runs_parent ON runs(parent_id);
CREATE TABLE IF NOT EXISTS events(
 seq bigserial PRIMARY KEY, run_id text NOT NULL REFERENCES runs(id), kind text NOT NULL,
 payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS events_run ON events(run_id,seq);
-- Events are the transactional outbox. Do not use a maximum-sequence cursor:
-- PostgreSQL sequence allocation order is not transaction commit order.
ALTER TABLE events ADD COLUMN IF NOT EXISTS telemetry_owner text NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN IF NOT EXISTS telemetry_fence bigint NOT NULL DEFAULT 0;
ALTER TABLE events ADD COLUMN IF NOT EXISTS telemetry_until timestamptz;
ALTER TABLE events ADD COLUMN IF NOT EXISTS telemetry_delivered_at timestamptz;
CREATE INDEX IF NOT EXISTS events_telemetry_pending ON events(seq) WHERE telemetry_delivered_at IS NULL;
CREATE TABLE IF NOT EXISTS calls(
 id text PRIMARY KEY, run_id text NOT NULL REFERENCES runs(id), kind text NOT NULL,
 request_hash text NOT NULL, request jsonb NOT NULL, status text NOT NULL,
 attempt text NOT NULL, fence bigint NOT NULL, response jsonb, error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS model_attempts(
 id text PRIMARY KEY, call_id text NOT NULL REFERENCES calls(id), status text NOT NULL,
 request jsonb NOT NULL, response jsonb, error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz);
CREATE TABLE IF NOT EXISTS instances(
 id text PRIMARY KEY, role text NOT NULL, hostname text NOT NULL, pid integer NOT NULL,
 heartbeat_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS business_runs(
 run_id text PRIMARY KEY REFERENCES runs(id), action_id text NOT NULL, action_revision integer NOT NULL,
 commodity text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS harness_exchanges(
 run_id text NOT NULL REFERENCES runs(id), seq integer NOT NULL, method text NOT NULL,
 input_hash text NOT NULL, request jsonb NOT NULL, rule_revision bigint NOT NULL,
 response jsonb, PRIMARY KEY(run_id,seq));
ALTER TABLE business_runs ADD COLUMN IF NOT EXISTS agent_id text;
ALTER TABLE business_runs ADD COLUMN IF NOT EXISTS agent_revision integer;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'legacy-go';
ALTER TABLE runs ALTER COLUMN engine SET DEFAULT 'dsh-0.2.0-rc.2';
CREATE TABLE IF NOT EXISTS app_runs(run_id text PRIMARY KEY REFERENCES runs(id),app text NOT NULL,metadata jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS app_runs_listing ON app_runs(app,created_at DESC);
INSERT INTO app_runs(run_id,app,metadata,created_at) SELECT run_id,'futures',jsonb_build_object('commodity',commodity),created_at FROM business_runs ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS calls_run_kind ON calls(run_id,kind);
CREATE INDEX IF NOT EXISTS model_attempts_call ON model_attempts(call_id);
-- Pending batches are a durable outbox. Only after Storage acknowledges the
-- immutable segments/manifest is the temporary batch released.
CREATE TABLE IF NOT EXISTS harness_archives(
 run_id text NOT NULL, seq integer NOT NULL, event_start integer NOT NULL,
 event_end integer NOT NULL, batch bytea, manifest jsonb,
 source_owner text NOT NULL, source_fence bigint NOT NULL,
 source_start integer NOT NULL, source_end integer NOT NULL,
 previous_hash text NOT NULL DEFAULT '',
 manifest_hash text NOT NULL DEFAULT '',
 PRIMARY KEY(run_id,seq),
 FOREIGN KEY(run_id,seq) REFERENCES harness_exchanges(run_id,seq),
 CHECK(event_start>=0 AND event_end>=event_start),
 CHECK((batch IS NOT NULL AND manifest IS NULL AND manifest_hash='') OR
       (batch IS NULL AND manifest IS NOT NULL AND length(manifest_hash)=64)));
