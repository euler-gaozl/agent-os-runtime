package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Receipt struct {
	Status   string
	Response []byte
}

func (s *Store) ToolReceipt(ctx context.Context, runID, id string) (Receipt, error) {
	var v Receipt
	err := s.DB.QueryRow(ctx, "SELECT status,response FROM calls WHERE run_id=$1 AND id=$2 AND kind='tool'", runID, id).Scan(&v.Status, &v.Response)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return v, err
}

type Child struct {
	ID, Status string
	State      RunState
}

func (s *Store) Children(ctx context.Context, id string) ([]Child, error) {
	rows, e := s.DB.Query(ctx, "SELECT id,status,state FROM runs WHERE parent_id=$1 ORDER BY created_at", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Child{}
	for rows.Next() {
		var c Child
		var raw []byte
		if e = rows.Scan(&c.ID, &c.Status, &raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &c.State); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// CheckpointFailure preserves cancellation/unknown-effect convergence. A stopped
// worker or lost lease leaves a resumable task, never a fabricated terminal state.
func (s *Store) CheckpointFailure(ctx context.Context, r Run, cause error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if cause == nil {
		return nil
	}
	fresh, e := s.Run(ctx, r.ID)
	if e != nil {
		return e
	}
	if fresh.Owner != r.Owner || fresh.Fence != r.Fence {
		return ErrFence
	}
	r.State.Error = cause.Error()
	status := "failed"
	if errors.Is(cause, ErrUnknown) {
		status = "waiting_unknown"
	}
	if fresh.CancelRequested {
		status = "cancelled"
		var n int
		if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM calls WHERE run_id=$1 AND kind='tool' AND status IN('inflight','unknown')", r.ID).Scan(&n); e != nil {
			return e
		}
		if n > 0 {
			status = "waiting_unknown"
		} else {
			if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM runs WHERE parent_id=$1 AND status NOT IN('completed','failed','cancelled')", r.ID).Scan(&n); e != nil {
				return e
			}
			if n > 0 {
				status = "waiting_children"
			}
		}
	}
	return s.Save(ctx, r, status)
}
func Readiness(ctx context.Context, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	_, e := db.Exec(ctx, "SELECT r.engine,r.rule_revision,c.request_hash,m.status,h.input_hash,a.manifest_hash,a.batch,e.seq FROM runs r,calls c,model_attempts m,harness_exchanges h,harness_archives a,events e WHERE false")
	return e
}
