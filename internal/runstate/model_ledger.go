package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

type CallRecord struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Status   string          `json:"status"`
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
	Error    string          `json:"error"`
}

func (s *Store) Calls(ctx context.Context, runID string) ([]CallRecord, error) {
	rows, e := s.DB.Query(ctx, "SELECT id,kind,status,request,response,error FROM calls WHERE run_id=$1 ORDER BY created_at", runID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []CallRecord{}
	for rows.Next() {
		var c CallRecord
		if e = rows.Scan(&c.ID, &c.Kind, &c.Status, &c.Request, &c.Response, &c.Error); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Model attempts can be retried, but each retry remains a separate possibly billable attempt.
func (s *Store) BeginModel(ctx context.Context, r Run, id string, request []byte) (string, []byte, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return "", nil, e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false); e != nil {
		return "", nil, e
	}
	var revision int64
	var engine string
	if e = tx.QueryRow(ctx, "SELECT rule_revision,engine FROM runs WHERE id=$1", r.ID).Scan(&revision, &engine); e != nil {
		return "", nil, e
	}
	if revision != r.RuleRevision {
		return "", nil, ErrFence
	}
	if engine == EngineDSHBudget || r.Engine == EngineDSHBudget {
		if engine != r.Engine {
			return "", nil, errors.New("model engine differs from immutable run")
		}
		if e = checkBudgetModelIntent(ctx, tx, r, id, request, false); e != nil {
			return "", nil, e
		}
		rejected, err := InspectBudgetRequest(request, r.Snapshot.Agent.ContextTokens, r.Snapshot.Agent.Model.MaxTokens)
		if err != nil {
			return "", nil, err
		}
		if rejected != nil {
			return "", nil, errors.New("context rejection must be committed without a model attempt")
		}
	}
	if engine == EngineDSHNative || r.Engine == EngineDSHNative {
		if engine != r.Engine {
			return "", nil, errors.New("model engine differs from immutable run")
		}
		if e = checkNativeModelIntent(ctx, tx, r, id, request, false); e != nil {
			return "", nil, e
		}
	}
	var status, hash, callRun, kind string
	var response, previousRequest []byte
	e = tx.QueryRow(ctx, "SELECT status,request_hash,response,request,run_id,kind FROM calls WHERE id=$1", id).Scan(&status, &hash, &response, &previousRequest, &callRun, &kind)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return "", nil, e
	}
	if e == nil {
		if hasDurableModelOutcome(r.Engine) && (callRun != r.ID || kind != "model") {
			return "", nil, errors.New("model operation collides with another receipt")
		}
		if hash != Hash(request) && canonicalHash(previousRequest) != canonicalHash(request) {
			return "", nil, errors.New("model request changed within same logical call")
		}
		if status == "completed" {
			return "", response, tx.Commit(ctx)
		}
	}
	if engine == EngineDSHNative {
		// Completed receipts above remain replayable. Only a new attempt must
		// match the current rules, while the run row is still locked. Exchange
		// may have refreshed r.RuleRevision while retaining an older intent.
		seq, err := budgetExchangeSeq(r, id)
		if err != nil {
			return "", nil, err
		}
		var intentRevision int64
		if e = tx.QueryRow(ctx, "SELECT rule_revision FROM harness_exchanges WHERE run_id=$1 AND seq=$2", r.ID, seq).Scan(&intentRevision); e != nil {
			return "", nil, e
		}
		if intentRevision != revision {
			return "", nil, errors.New("native model intent constraint revision is outdated")
		}
	}
	if e = reserveModelBudget(ctx, tx, r.ID, request); e != nil {
		return "", nil, e
	}
	attempt := ID()
	_, e = tx.Exec(ctx, `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence) VALUES($1,$2,'model',$3,$4,'inflight',$5,$6)
 ON CONFLICT(id) DO UPDATE SET attempt=$5,fence=$6,status='inflight',updated_at=now()`, id, r.ID, Hash(request), request, attempt, r.Fence)
	if e != nil {
		return "", nil, e
	}
	_, e = tx.Exec(ctx, "UPDATE model_attempts SET status='abandoned',finished_at=now() WHERE call_id=$1 AND status='inflight'", id)
	if e != nil {
		return "", nil, e
	}
	_, e = tx.Exec(ctx, "INSERT INTO model_attempts(id,call_id,status,request) VALUES($1,$2,'inflight',$3)", attempt, id, request)
	if e != nil {
		return "", nil, e
	}
	if e = EmitEvent(ctx, tx, r.ID, "model_started", map[string]any{"call_id": id, "attempt": attempt}); e != nil {
		return "", nil, e
	}
	return attempt, nil, tx.Commit(ctx)
}
func (s *Store) EndModel(ctx context.Context, r Run, id, attempt string, response []byte, callErr error) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false); e != nil {
		return e
	}
	status, msg := "completed", ""
	if callErr != nil {
		status = "failed"
		msg = callErr.Error()
	}
	if len(response) == 0 {
		response = []byte("null")
	}
	tag, e := tx.Exec(ctx, "UPDATE calls SET status=$3,response=$4,error=$5,updated_at=now() WHERE id=$1 AND attempt=$2 AND fence=$6", id, attempt, status, response, msg, r.Fence)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrFence
	}
	_, e = tx.Exec(ctx, "UPDATE model_attempts SET status=$2,response=$3,error=$4,finished_at=now() WHERE id=$1", attempt, status, response, msg)
	if e != nil {
		return e
	}
	if e = EmitEvent(ctx, tx, r.ID, "model_"+status, map[string]any{"call_id": id, "attempt": attempt}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
