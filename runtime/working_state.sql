-- Structured, derived working sets are separate from the old Worker's runs.state.
-- Application credentials may append/read revisions but cannot rewrite history.
CREATE TABLE IF NOT EXISTS run_working_states(
 run_id text NOT NULL REFERENCES runs(id), revision bigint NOT NULL CHECK(revision>0),
 operation_id text NOT NULL, request_hash text NOT NULL CHECK(length(request_hash)=64),
 source_rule_revision bigint NOT NULL CHECK(source_rule_revision>0),
 source_owner text NOT NULL, source_fence bigint NOT NULL CHECK(source_fence>0),
 actor_agent text NOT NULL, reason text NOT NULL, body jsonb NOT NULL, evidence jsonb NOT NULL,
 receipt jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(run_id,revision), UNIQUE(run_id,operation_id),
 CHECK(octet_length(body::text)<=65536));
