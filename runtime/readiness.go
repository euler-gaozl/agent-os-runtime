package runtime

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Readiness(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("database required")
	}
	_, e := db.Exec(ctx, "SELECT r.engine,r.rule_revision,c.request_hash,m.status,h.input_hash,a.manifest_hash,a.batch,n.state,v.root_id,u.actor_id,z.request_hash,d.scope,b.agent_revision,p.app,i.heartbeat_at,e.seq,e.telemetry_fence,w.revision,w.receipt,q.request_id,q.run_id,q.request_hash,q.expected_failure_version,q.http_status,q.response,q.created_at FROM runs r,calls c,model_attempts m,harness_exchanges h,harness_archives a,note_roots n,note_versions v,note_audits u,note_admin_receipts z,documents d,business_runs b,app_runs p,instances i,events e,run_working_states w,run_continuation_receipts q WHERE false")
	return e
}
