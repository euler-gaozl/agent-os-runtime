package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
)

func invocationHash(inv Invocation) string {
	var arguments any
	decoder := json.NewDecoder(strings.NewReader(string(inv.Arguments)))
	decoder.UseNumber()
	if e := decoder.Decode(&arguments); e != nil {
		return Hash(inv.Arguments)
	}
	return Hash(JSON([]any{inv.Name, arguments}))
}
func checkInvocation(ctx context.Context, tx pgx.Tx, inv Invocation) (Run, error) {
	if e := lockRun(ctx, tx, inv.RunID, inv.Owner, inv.Fence, false); e != nil {
		return Run{}, e
	}
	r, e := scanRun(tx.QueryRow(ctx, "SELECT "+runColumns+" FROM runs WHERE id=$1", inv.RunID))
	if e != nil {
		return r, e
	}
	if r.RuleRevision != inv.RuleRevision {
		return r, errors.New("constraints changed; old plan may not execute")
	}
	return r, nil
}
func existingCall(ctx context.Context, tx pgx.Tx, inv Invocation) ([]byte, string, error) {
	var hash, status string
	var result []byte
	e := tx.QueryRow(ctx, "SELECT request_hash,status,response FROM calls WHERE id=$1 FOR UPDATE", inv.OperationID).Scan(&hash, &status, &result)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if e != nil {
		return nil, "", e
	}
	if hash != invocationHash(inv) {
		return nil, "", errors.New("operation id reused with different arguments")
	}
	return result, status, nil
}
func insertCall(ctx context.Context, tx pgx.Tx, inv Invocation, status string, result []byte) error {
	if len(result) == 0 {
		result = []byte("null")
	}
	_, e := tx.Exec(ctx, `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,response) VALUES($1,$2,'tool',$3,$4,$5,$6,$7,$8)
 ON CONFLICT(id) DO UPDATE SET status=$5,attempt=$6,fence=$7,response=$8,updated_at=now()`, inv.OperationID, inv.RunID, invocationHash(inv), JSON(inv), status, ID(), inv.Fence, result)
	return e
}

// Resolve never guesses. The operator supplies a verified receipt or explicit proof of non-execution.
func (s *Store) Resolve(ctx context.Context, runID, callID, decision, evidence string, result json.RawMessage) error {
	if evidence == "" {
		return errors.New("reconciliation evidence required")
	}
	if decision != "completed" && decision != "not_executed" {
		return errors.New("invalid decision")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var status string
	if e = tx.QueryRow(ctx, "SELECT status FROM runs WHERE id=$1 FOR UPDATE", runID).Scan(&status); e != nil {
		return e
	}
	if status != "waiting_unknown" {
		return errors.New("run is not awaiting reconciliation")
	}
	target := "completed"
	if decision == "not_executed" {
		target = "retryable"
	}
	if len(result) == 0 {
		result = []byte("null")
	}
	tag, e := tx.Exec(ctx, "UPDATE calls SET status=$3,response=$4,updated_at=now() WHERE id=$1 AND run_id=$2 AND status IN('unknown','inflight')", callID, runID, target, result)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return errors.New("unresolved call not found")
	}
	_, e = tx.Exec(ctx, `UPDATE runs r SET status=CASE WHEN EXISTS(SELECT 1 FROM calls c WHERE c.run_id=r.id AND c.kind='tool' AND c.status IN('unknown','inflight')) THEN 'waiting_unknown' WHEN cancel_requested THEN CASE WHEN EXISTS(SELECT 1 FROM runs child WHERE child.parent_id=r.id AND child.status NOT IN('completed','failed','cancelled')) THEN 'waiting_children' ELSE 'cancelled' END ELSE 'queued' END,owner='',lease_until=NULL,updated_at=now() WHERE id=$1`, runID)
	if e != nil {
		return e
	}
	if e = event(ctx, tx, runID, "reconciled", map[string]any{"call_id": callID, "decision": decision, "evidence": evidence}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Constraints(ctx context.Context, id string, values []string) error {
	if len(JSON(values)) > 16000 {
		return errors.New("constraints exceed 16 KB")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	ids, e := lockFamily(ctx, tx, id)
	if e != nil {
		return e
	}
	tag, e := tx.Exec(ctx, "UPDATE runs SET constraints=COALESCE(snapshot#>'{agent,constraints}','[]'::jsonb)||$2::jsonb,rule_revision=rule_revision+1,updated_at=now() WHERE id=ANY($1) AND status IN('queued','running','waiting_unknown','waiting_children','failed')", ids, JSON(values))
	if e != nil {
		return e
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active run not found")
	}
	for _, member := range ids {
		if e = event(ctx, tx, member, "constraints_revised", map[string]any{"source_run": id, "values": values}); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
