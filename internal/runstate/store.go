package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

// Store is internal to this owner module. Its database never crosses the public execution facade.
type Store struct {
	DB               *pgxpool.Pool
	Archives         ArtifactWriter
	SourceEnabled    bool
	ResidentEnabled  bool
	BudgetEnabled    bool
	NativeEnabled    bool
	FinalEnabled     bool
	GuardedEnabled   bool
	GuardedV6Enabled bool
}
type ArtifactWriter interface {
	PutArtifact(context.Context, []byte, string, string) (string, error)
}

func (s *Store) PutArtifact(ctx context.Context, b []byte, media, root string) (string, error) {
	if s.Archives == nil {
		return "", errors.New("artifact writer is not configured")
	}
	return s.Archives.PutArtifact(ctx, b, media, root)
}
func EmitEvent(ctx context.Context, tx pgx.Tx, id, kind string, v any) error {
	_, e := tx.Exec(ctx, "INSERT INTO events(run_id,kind,payload) VALUES($1,$2,$3)", id, kind, JSON(v))
	return e
}
func InsertRun(ctx context.Context, tx pgx.Tx, key, task string, snap Snapshot, parent *string, pinned []string) (string, error) {
	return insertRunEngine(ctx, tx, key, task, snap, parent, pinned, EngineDSHEvidence)
}

// InsertRootRun selects an engine only at first acceptance. Deployment defaults
// are deliberately absent from input_hash: a retry after an upgrade must return
// the original run without changing its engine. Children always inherit theirs.
func InsertRootRun(ctx context.Context, tx pgx.Tx, key, task string, snap Snapshot, pinned []string, engine string) (string, error) {
	if engine != EngineDSHEvidence && !IsSourceEngine(engine) {
		return "", errors.New("unsupported root submission engine")
	}
	return insertRunEngine(ctx, tx, key, task, snap, nil, pinned, engine)
}

func insertRunEngine(ctx context.Context, tx pgx.Tx, key, task string, snap Snapshot, parent *string, pinned []string, engine string) (string, error) {
	constraints := append(append([]string{}, snap.Agent.Constraints...), pinned...)
	id := ID()
	inputHash := Hash(JSON([]any{task, snap, parent, pinned}))
	state := RunState{Pinned: pinned, Messages: []Message{{Role: "user", Content: task}}}
	var actual, oldHash string
	if parent != nil {
		if e := tx.QueryRow(ctx, "SELECT engine FROM runs WHERE id=$1", *parent).Scan(&engine); e != nil {
			return "", e
		}
	}
	// A native-capable deployment still serves frozen legacy Agent revisions.
	// All-native trees use v4; wholly legacy trees keep the fixed v3 protocol.
	// Mixed trees remain rejected by native validation, never silently rewritten.
	if parent == nil && engine == EngineDSHFinal && !hasFinalOutput(snap) {
		engine = EngineDSHNative
	}
	legacyOnNative := parent == nil && IsNativeEngine(engine) && !hasNativeCapabilities(snap)
	if legacyOnNative {
		engine = EngineDSHBudget
	}
	e := tx.QueryRow(ctx, `INSERT INTO runs(id,submission_key,input_hash,parent_id,snapshot,task,state,constraints,engine) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT(submission_key) DO UPDATE SET submission_key=EXCLUDED.submission_key RETURNING id,input_hash`, id, key, inputHash, parent, JSON(snap), task, JSON(state), JSON(constraints), engine).Scan(&actual, &oldHash)
	if e != nil {
		return "", e
	}
	if oldHash != inputHash {
		return "", errors.New("idempotency key reused with different input")
	}
	if actual == id {
		if err := validateFinalSnapshot(snap, engine); err != nil {
			return "", err
		}
		if legacyOnNative {
			if err := validateLegacyModelTree(snap); err != nil {
				return "", err
			}
		}
		taskLimit := 40000
		if IsNativeEngine(engine) {
			taskLimit = 16 << 20
		}
		if len(task) > taskLimit {
			return "", errors.New("new task exceeds execution transport capacity")
		}
		if IsNativeEngine(engine) {
			if err := ValidateNativeSnapshot(snap); err != nil {
				return "", err
			}
		} else if hasNativeCapabilities(snap) {
			return "", errors.New("native model capabilities require the native execution engine")
		}
		if e = EmitEvent(ctx, tx, id, "accepted", map[string]any{"agent": snap.AgentRef}); e != nil {
			return "", e
		}
	}
	return actual, nil
}

func validateLegacyModelTree(snap Snapshot) error {
	a := snap.Agent
	// Agent definitions were validated before Snapshot appended bound prompts.
	// Revalidating the expanded prompt here would change legacy acceptance.
	// Only guard the frozen legacy model ranges against missing native metadata;
	// zero retains the historical default semantics, without rewriting the snapshot.
	if (a.ContextTokens != 0 && (a.ContextTokens < 8192 || a.ContextTokens > 1000000)) || a.Model.MaxTokens < 0 || a.Model.MaxTokens > 32768 {
		return errors.New("model capabilities required for native context or output sizes")
	}
	for _, child := range snap.Children {
		if err := validateLegacyModelTree(child); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Submit(ctx context.Context, key, task string, snap Snapshot, pinned []string) (string, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	id, e := InsertRun(ctx, tx, key, task, snap, nil, pinned)
	if e != nil {
		return "", e
	}
	return id, tx.Commit(ctx)
}

const RunColumns = "id,parent_id,task,snapshot,status,step,state,owner,fence,constraints,rule_revision,cancel_requested,created_at,updated_at,engine"

func ScanRun(row pgx.Row) (Run, error) {
	var r Run
	var snap, state, constraints []byte
	e := row.Scan(&r.ID, &r.ParentID, &r.Task, &snap, &r.Status, &r.Step, &state, &r.Owner, &r.Fence, &constraints, &r.RuleRevision, &r.CancelRequested, &r.CreatedAt, &r.UpdatedAt, &r.Engine)
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(snap, &r.Snapshot); e != nil {
		return r, e
	}
	if e = json.Unmarshal(constraints, &r.Constraints); e != nil {
		return r, e
	}
	e = json.Unmarshal(state, &r.State)
	return r, e
}
func (s *Store) Run(ctx context.Context, id string) (Run, error) {
	return ScanRun(s.DB.QueryRow(ctx, "SELECT "+RunColumns+" FROM runs WHERE id=$1", id))
}
func (s *Store) Runs(ctx context.Context) ([]Run, error) {
	rows, e := s.DB.Query(ctx, `SELECT id,parent_id,left(task,500),jsonb_build_object('agent',jsonb_build_object('name',snapshot#>>'{agent,name}'),'agent_ref',snapshot->'agent_ref'),status,step,'{}'::jsonb,owner,fence,'[]'::jsonb,rule_revision,cancel_requested,created_at,updated_at,engine FROM runs ORDER BY created_at DESC LIMIT 100`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Run{}
	for rows.Next() {
		r, e := ScanRun(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) Claim(ctx context.Context, owner string, lease time.Duration) (Run, error) {
	engines := []string{"legacy-go", "dsh-0.2.0-rc.2", EngineDSHEvidence}
	if s.SourceEnabled {
		engines = append(engines, EngineDSHSource)
	}
	if s.ResidentEnabled {
		engines = append(engines, EngineDSHResident)
	}
	if s.BudgetEnabled {
		engines = append(engines, EngineDSHBudget)
	}
	if s.NativeEnabled {
		engines = append(engines, EngineDSHNative)
	}
	if s.FinalEnabled {
		engines = append(engines, EngineDSHFinal)
	}
	if s.GuardedEnabled {
		engines = append(engines, EngineDSHGuarded)
	}
	if s.GuardedV6Enabled {
		engines = append(engines, EngineDSHGuardedV6)
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return Run{}, e
	}
	defer tx.Rollback(ctx)
	r, e := ScanRun(tx.QueryRow(ctx, `UPDATE runs SET status='running',owner=$1,fence=fence+1,lease_until=clock_timestamp()+$2::interval,updated_at=now()
 WHERE id=(SELECT id FROM runs r WHERE engine=ANY($3) AND (status='queued' OR (status='running' AND lease_until<clock_timestamp()) OR (status='waiting_children' AND NOT EXISTS(SELECT 1 FROM runs c WHERE c.parent_id=r.id AND c.status NOT IN('completed','failed','cancelled')))) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING `+RunColumns, owner, fmt.Sprintf("%f seconds", lease.Seconds()), engines))
	if e != nil {
		return r, e
	}
	if e = EmitEvent(ctx, tx, r.ID, "claimed", map[string]any{"owner": owner, "fence": r.Fence}); e != nil {
		return r, e
	}
	return r, tx.Commit(ctx)
}
func LockRun(ctx context.Context, tx pgx.Tx, id, owner string, fence int64, allowCancel bool, expectedEngine ...string) error {
	if len(expectedEngine) > 1 {
		return errors.New("one expected execution engine required")
	}
	var valid bool
	var engine string
	e := tx.QueryRow(ctx, "SELECT owner=$2 AND fence=$3 AND status='running' AND lease_until>clock_timestamp() AND ($4 OR NOT cancel_requested),engine FROM runs WHERE id=$1 FOR UPDATE", id, owner, fence, allowCancel).Scan(&valid, &engine)
	if e != nil {
		return e
	}
	if !valid {
		return ErrFence
	}
	if len(expectedEngine) == 1 && engine != expectedEngine[0] {
		return errors.New("execution engine differs from the locked run")
	}
	return nil
}
func (s *Store) Heartbeat(ctx context.Context, r Run, lease time.Duration) error {
	tag, e := s.DB.Exec(ctx, "UPDATE runs SET lease_until=clock_timestamp()+$4::interval WHERE id=$1 AND owner=$2 AND fence=$3 AND status='running' AND NOT cancel_requested AND lease_until>clock_timestamp()", r.ID, r.Owner, r.Fence, fmt.Sprintf("%f seconds", lease.Seconds()))
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrFence
	}
	return nil
}
func (s *Store) Save(ctx context.Context, r Run, status string) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, status == "cancelled" || status == "waiting_unknown" || status == "waiting_children"); e != nil {
		return e
	}
	if status == "completed" {
		var revision int64
		var referenceOnly bool
		if e = tx.QueryRow(ctx, "SELECT rule_revision,engine=ANY($2) AND COALESCE(snapshot#>'{agent,final_output}','null'::jsonb)<>'null'::jsonb FROM runs WHERE id=$1", r.ID, []string{EngineDSHFinal, EngineDSHGuarded}).Scan(&revision, &referenceOnly); e != nil {
			return e
		}
		if referenceOnly {
			return errors.New("receipt finalization requires atomic finish_ref")
		}
		if revision != r.RuleRevision {
			return errors.New("constraints changed before completion; resume with current constraints")
		}
	}
	_, e = tx.Exec(ctx, "UPDATE runs SET state=$2,step=$3,status=$4,updated_at=now() WHERE id=$1", r.ID, JSON(r.State), r.Step, status)
	if e != nil {
		return e
	}
	if e = EmitEvent(ctx, tx, r.ID, "checkpoint", map[string]any{"step": r.Step, "status": status, "result": r.State.Result, "error": r.State.Error}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Cancel(ctx context.Context, id string) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	ids, e := LockFamily(ctx, tx, id)
	if e != nil {
		return e
	}
	// An unknown source-state delivery has no separate tool call to reconcile.
	// The run lock serializes cancellation against its atomic state transaction;
	// cancellation never retries that operation. Unknown external effects retain
	// their existing reconciliation requirement.
	_, e = tx.Exec(ctx, `UPDATE runs r SET cancel_requested=true,status=CASE WHEN status IN('queued','waiting_children','failed')
 OR (status='waiting_unknown' AND engine=ANY($2)
 AND ((SELECT method FROM harness_exchanges h WHERE h.run_id=r.id ORDER BY seq DESC LIMIT 1) IN('working_state_read','working_state_update','final_output_select','finish_ref')
 OR (engine=ANY($3) AND (SELECT method FROM harness_exchanges h WHERE h.run_id=r.id ORDER BY seq DESC LIMIT 1)='checkpoint'))
 AND NOT EXISTS(SELECT 1 FROM calls c WHERE c.run_id=r.id AND c.status IN('inflight','unknown')))
 THEN CASE WHEN EXISTS(SELECT 1 FROM runs c WHERE c.parent_id=r.id AND c.status NOT IN('completed','failed','cancelled')) THEN 'waiting_children' ELSE 'cancelled' END ELSE status END,updated_at=now() WHERE id=ANY($1) AND status NOT IN('completed','cancelled')`, ids, []string{EngineDSHSource, EngineDSHResident, EngineDSHBudget, EngineDSHNative, EngineDSHFinal, EngineDSHGuarded}, []string{EngineDSHFinal, EngineDSHGuarded})
	if e != nil {
		return e
	}
	if e = EmitEvent(ctx, tx, id, "cancel_requested", map[string]any{}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

// Lock parents before discovering children. A concurrent spawn must commit before
// we enumerate its children, or observe the new cancellation/constraint revision.
func LockFamily(ctx context.Context, tx pgx.Tx, id string) ([]string, error) {
	var root string
	if e := tx.QueryRow(ctx, "SELECT id FROM runs WHERE id=$1 FOR UPDATE", id).Scan(&root); e != nil {
		return nil, e
	}
	ids, frontier := []string{root}, []string{root}
	for len(frontier) > 0 {
		rows, e := tx.Query(ctx, "SELECT id FROM runs WHERE parent_id=ANY($1) ORDER BY id FOR UPDATE", frontier)
		if e != nil {
			return nil, e
		}
		next := []string{}
		for rows.Next() {
			var child string
			if e = rows.Scan(&child); e != nil {
				rows.Close()
				return nil, e
			}
			next = append(next, child)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		ids = append(ids, next...)
		frontier = next
	}
	return ids, nil
}
func (s *Store) Events(ctx context.Context, id string, after int64) ([]Event, error) {
	rows, e := s.DB.Query(ctx, "SELECT seq,kind,payload,created_at FROM events WHERE run_id=$1 AND seq>$2 ORDER BY seq LIMIT 500", id, after)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var v Event
		if e = rows.Scan(&v.Seq, &v.Kind, &v.Payload, &v.CreatedAt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
