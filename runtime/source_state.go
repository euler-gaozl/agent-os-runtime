package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
)

// sourceState records the exact read, success or validation rejection in the
// already-staged Exchange. State and bridge response use one transaction, so a
// lost HTTP reply cannot turn a rejection into a later success or refresh a read.
func (s *Store) sourceState(ctx context.Context, inv Invocation) (json.RawMessage, error) {
	prefix := inv.RunID + "/working-state/"
	seqText := strings.TrimPrefix(inv.OperationID, prefix)
	seq, err := strconv.Atoi(seqText)
	if err != nil || seq <= 0 || strconv.Itoa(seq) != seqText || !strings.HasPrefix(inv.OperationID, prefix) || len(inv.OperationID) > 240 ||
		(inv.Name != "working_state_read" && inv.Name != "working_state_update") {
		return nil, stateError(400, "invalid source-state operation identity")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err = lockRun(ctx, tx, inv.RunID, inv.Owner, inv.Fence, false); err != nil {
		return nil, err
	}
	run, err := scanRun(tx.QueryRow(ctx, "SELECT "+runColumns+" FROM runs WHERE id=$1", inv.RunID))
	if err != nil {
		return nil, err
	}
	if !isSourceEngine(run.Engine) {
		return nil, stateError(400, "working-state bridge requires the source engine")
	}
	var method, hash, manifest string
	var request, response []byte
	var plan int64
	err = tx.QueryRow(ctx, `SELECT e.method,e.input_hash,e.request,e.response,e.rule_revision,a.manifest_hash
 FROM harness_exchanges e JOIN harness_archives a ON a.run_id=e.run_id AND a.seq=e.seq
 WHERE e.run_id=$1 AND e.seq=$2`, inv.RunID, seq).Scan(&method, &hash, &request, &response, &plan, &manifest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, stateError(409, "source-state intent and archive required")
	}
	if err != nil {
		return nil, err
	}
	// Match the stored intent, never trust a caller-supplied current plan revision.
	if method != inv.Name || hash != canonicalHash(inv.Arguments) || canonicalHash(request) != hash || manifest == "" {
		return nil, stateError(409, "source-state intent mismatch or archive not acknowledged")
	}
	if response != nil {
		return json.RawMessage(response), tx.Commit(ctx)
	}
	inv.RuleRevision = plan
	inv.Arguments = request
	var value any
	if inv.Name == "working_state_update" {
		value, err = commitWorkingStateTx(ctx, tx, run, inv)
	} else if !bytes.Equal(bytes.TrimSpace(request), []byte("{}")) {
		err = stateError(400, "working-state read takes empty arguments")
	} else {
		var view WorkingStateView
		view, err = readWorkingState(ctx, tx, inv.RunID, 0)
		value = map[string]any{"operation_id": inv.OperationID, "view": view}
	}
	if err != nil {
		var status *servicehttp.StatusError
		if !errors.As(err, &status) || (status.Status != 400 && status.Status != 409) {
			return nil, err
		}
		value = map[string]any{"rejected": true, "status": status.Status, "error": status.Message}
	}
	response = JSON(value)
	if _, err = tx.Exec(ctx, "UPDATE harness_exchanges SET response=$3 WHERE run_id=$1 AND seq=$2", inv.RunID, seq, response); err != nil {
		return nil, err
	}
	return json.RawMessage(response), tx.Commit(ctx)
}

// Operator-triggered recovery is limited to the state protocol whose reads,
// successes and rejections are atomically frozen. Never use this to guess an
// unknown external tool outcome or to run a cancelled task again.
type SourceRecovery struct {
	RunID   string `json:"run_id"`
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
	method  string
}

// One read snapshot powers both the management preview and the locked action.
// A preview is advisory; resume always locks and rechecks before requeueing.
func readSourceRecovery(ctx context.Context, reader noteReader, id string) (SourceRecovery, error) {
	v := SourceRecovery{RunID: id}
	var status, engine, manifest string
	var cancelled, unresolved bool
	err := reader.QueryRow(ctx, `SELECT r.status,r.engine,r.cancel_requested,COALESCE(e.method,''),COALESCE(a.manifest_hash,''),
 EXISTS(SELECT 1 FROM calls c WHERE c.run_id=r.id AND c.status IN('inflight','unknown'))
 FROM runs r LEFT JOIN LATERAL (SELECT seq,method FROM harness_exchanges WHERE run_id=r.id ORDER BY seq DESC LIMIT 1) e ON true
 LEFT JOIN harness_archives a ON a.run_id=r.id AND a.seq=e.seq WHERE r.id=$1`, id).
		Scan(&status, &engine, &cancelled, &v.method, &manifest, &unresolved)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, stateError(404, "run not found")
	}
	if err != nil {
		return v, err
	}
	switch {
	case !isSourceEngine(engine):
		v.Reason = "source_engine_required"
	case cancelled:
		v.Reason = "cancelled"
	case status != "waiting_unknown":
		v.Reason = "not_waiting"
	case unresolved:
		v.Reason = "unresolved_calls"
	case v.method != "working_state_read" && v.method != "working_state_update":
		v.Reason = "no_state_exchange"
	case manifest == "":
		v.Reason = "archive_unconfirmed"
	default:
		v.Reason, v.Allowed = "ready", true
	}
	return v, nil
}

func (s *Store) resumeSourceState(ctx context.Context, id string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var locked string
	if err = tx.QueryRow(ctx, "SELECT id FROM runs WHERE id=$1 FOR UPDATE", id).Scan(&locked); err != nil {
		return err
	}
	preview, err := readSourceRecovery(ctx, tx, id)
	if err != nil {
		return err
	}
	if !preview.Allowed {
		return stateError(409, "source-state recovery refused: "+preview.Reason)
	}
	if _, err = tx.Exec(ctx, "UPDATE runs SET status='queued',owner='',lease_until=NULL,updated_at=now() WHERE id=$1", id); err != nil {
		return err
	}
	if err = event(ctx, tx, id, "source_state_resumed", map[string]string{"method": preview.method}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
