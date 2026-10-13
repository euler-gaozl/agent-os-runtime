package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
)

// WorkingSet is derived task data, never an alternative source of instructions.
// The original task and current constraints are projected from runs on each read.
type WorkingSet struct {
	Format       int                  `json:"format"`
	Phase        string               `json:"phase"`
	Questions    []WorkingQuestion    `json:"questions"`
	Observations []WorkingObservation `json:"observations"`
	Conflicts    []WorkingConflict    `json:"conflicts"`
	NextSteps    []WorkingStep        `json:"next_steps"`
}
type WorkingQuestion struct {
	ID     string `json:"id"`
	Text   string `json:"text"`
	Status string `json:"status"` // open, answered, deferred
}
type WorkingObservation struct {
	ID       string   `json:"id"`
	Kind     string   `json:"kind"` // observation, plan, third_party_forecast
	Text     string   `json:"text"`
	Receipts []string `json:"receipts"`
}
type WorkingConflict struct {
	ID           string   `json:"id"`
	Text         string   `json:"text"`
	Observations []string `json:"observations"`
}
type WorkingStep struct {
	ID     string `json:"id"`
	Text   string `json:"text"`
	Status string `json:"status"` // pending, done, blocked
}
type WorkingStateChange struct {
	ExpectedRevision int64      `json:"expected_revision"`
	Reason           string     `json:"reason"`
	State            WorkingSet `json:"state"`
}
type WorkingEvidence struct {
	OperationID    string `json:"operation_id"`
	ResponseSHA256 string `json:"response_sha256"`
}
type WorkingStateReceipt struct {
	RunID        string `json:"run_id"`
	OperationID  string `json:"operation_id"`
	Revision     int64  `json:"revision"`
	RuleRevision int64  `json:"rule_revision"`
	StateSHA256  string `json:"state_sha256"`
}
type WorkingStateView struct {
	RunID              string            `json:"run_id"`
	Task               string            `json:"task"`
	Constraints        []string          `json:"constraints"`
	RuleRevision       int64             `json:"rule_revision"`
	Revision           int64             `json:"revision"`
	SourceRuleRevision int64             `json:"source_rule_revision"`
	Stale              bool              `json:"stale"`
	State              *WorkingSet       `json:"state"`
	Evidence           []WorkingEvidence `json:"evidence"`
}

func stateError(status int, message string) error {
	return &servicehttp.StatusError{Status: status, Message: message}
}

// encoding/json merges duplicate object fields when decoding structs but keeps
// only the last value in maps. Refuse that ambiguity before validating/hashing.
func uniqueWorkingJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	var value func(int) error
	value = func(depth int) error {
		if depth > 16 {
			return errors.New("working-state JSON nesting exceeded")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if delim != '{' && delim != '[' {
			return errors.New("invalid working-state JSON delimiter")
		}
		keys := map[string]bool{}
		for d.More() {
			if delim == '{' {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || keys[name] {
					return errors.New("duplicate working-state JSON key")
				}
				keys[name] = true
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("expected one working-state JSON value")
	}
	return nil
}

func workingChange(raw []byte) (WorkingStateChange, []string, error) {
	var in WorkingStateChange
	if len(raw) > 32<<10 || !utf8.Valid(raw) {
		return in, nil, stateError(400, "working-state change exceeds 32 KiB")
	}
	if err := uniqueWorkingJSON(raw); err != nil {
		return in, nil, stateError(400, "invalid or ambiguous working-state JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&in); err != nil {
		return in, nil, stateError(400, "invalid working-state change")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return in, nil, stateError(400, "expected one working-state change")
	}
	v := in.State
	if in.ExpectedRevision < 0 || !boundedText(in.Reason, 500) || v.Format != 1 || !boundedText(v.Phase, 200) ||
		len(v.Questions) > 16 || len(v.Observations) > 32 || len(v.Conflicts) > 16 || len(v.NextSteps) > 16 {
		return in, nil, stateError(400, "invalid working-state format, bounds or revision")
	}
	ids := map[string]bool{}
	entry := func(id, text string) bool {
		if !boundedText(id, 80) || strings.TrimSpace(id) != id || !utf8.ValidString(id) || !utf8.ValidString(text) || !boundedText(text, 1200) || ids[id] {
			return false
		}
		ids[id] = true
		return true
	}
	for _, q := range v.Questions {
		if !entry(q.ID, q.Text) || (q.Status != "open" && q.Status != "answered" && q.Status != "deferred") {
			return in, nil, stateError(400, "invalid or duplicate working question")
		}
	}
	observations, receipts := map[string]bool{}, map[string]bool{}
	for _, o := range v.Observations {
		if !entry(o.ID, o.Text) || (o.Kind != "observation" && o.Kind != "plan" && o.Kind != "third_party_forecast") || len(o.Receipts) == 0 || len(o.Receipts) > 8 {
			return in, nil, stateError(400, "observation requires a valid kind and 1-8 committed source receipts")
		}
		observations[o.ID] = true
		seen := map[string]bool{}
		for _, id := range o.Receipts {
			if !boundedText(id, 240) || seen[id] {
				return in, nil, stateError(400, "invalid or duplicate evidence receipt")
			}
			seen[id], receipts[id] = true, true
		}
	}
	for _, c := range v.Conflicts {
		if !entry(c.ID, c.Text) || len(c.Observations) < 2 || len(c.Observations) > 8 {
			return in, nil, stateError(400, "conflict requires 2-8 observations")
		}
		seen := map[string]bool{}
		for _, id := range c.Observations {
			if !observations[id] || seen[id] {
				return in, nil, stateError(400, "conflict contains a missing or duplicate observation")
			}
			seen[id] = true
		}
	}
	for _, step := range v.NextSteps {
		if !entry(step.ID, step.Text) || (step.Status != "pending" && step.Status != "done" && step.Status != "blocked") {
			return in, nil, stateError(400, "invalid or duplicate next step")
		}
	}
	keys := make([]string, 0, len(receipts))
	for id := range receipts {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	return in, keys, nil
}

// commitWorkingState is an internal fenced primitive for the forthcoming source
// engine. It is intentionally not a new tool or a public mutation route: current
// engines retain their historical tool sets and replay semantics.
func (s *Store) commitWorkingState(ctx context.Context, inv Invocation) (WorkingStateReceipt, error) {
	var receipt WorkingStateReceipt
	if inv.Name != "working_state_update" || !strings.HasPrefix(inv.OperationID, inv.RunID+"/working-state/") || len(inv.OperationID) > 240 {
		return receipt, stateError(400, "invalid working-state operation identity")
	}
	in, references, err := workingChange(inv.Arguments)
	if err != nil {
		return receipt, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback(ctx)
	run, err := checkInvocation(ctx, tx, inv)
	if err != nil {
		return receipt, err
	}
	// Hash exactly the validated value that will be persisted, never a separate
	// map interpretation of raw JSON or the replaceable owner/fence identity.
	hash := Hash(JSON(struct {
		Name   string             `json:"name"`
		Change WorkingStateChange `json:"change"`
	}{Name: inv.Name, Change: in}))
	var oldHash string
	var oldReceipt []byte
	err = tx.QueryRow(ctx, "SELECT request_hash,receipt FROM run_working_states WHERE run_id=$1 AND operation_id=$2", inv.RunID, inv.OperationID).Scan(&oldHash, &oldReceipt)
	if err == nil {
		if oldHash != hash {
			return receipt, stateError(409, "working-state operation reused with different content")
		}
		if err = json.Unmarshal(oldReceipt, &receipt); err != nil {
			return receipt, err
		}
		return receipt, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return receipt, err
	}
	var current int64
	if err = tx.QueryRow(ctx, "SELECT COALESCE(max(revision),0) FROM run_working_states WHERE run_id=$1", inv.RunID).Scan(&current); err != nil {
		return receipt, err
	}
	if current != in.ExpectedRevision {
		return receipt, stateError(409, "working-state revision changed; read the current state")
	}
	evidence := make([]WorkingEvidence, 0, len(references))
	if len(references) > 0 {
		// Validate in one bounded query, avoiding a database round trip per source.
		// No cross-run, model, uncommitted or checkpoint receipt is admitted. The
		// hash proves retained content, not success or factual truth.
		rows, err := tx.Query(ctx, `SELECT id,response FROM calls WHERE id=ANY($1) AND run_id=$2 AND kind='tool' AND status='completed'
 AND response IS NOT NULL AND request->>'name' IS DISTINCT FROM 'working_state_update'`, references, inv.RunID)
		if err != nil {
			return receipt, err
		}
		digests := make(map[string]string, len(references))
		for rows.Next() {
			var id string
			var response []byte
			if err := rows.Scan(&id, &response); err != nil {
				rows.Close()
				return receipt, err
			}
			digests[id] = canonicalHash(response)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return receipt, err
		}
		if len(digests) != len(references) {
			return receipt, stateError(400, "source receipt is not a committed tool result in this run")
		}
		for _, id := range references {
			evidence = append(evidence, WorkingEvidence{OperationID: id, ResponseSHA256: digests[id]})
		}
	}
	body := JSON(in.State)
	receipt = WorkingStateReceipt{RunID: inv.RunID, OperationID: inv.OperationID, Revision: current + 1, RuleRevision: run.RuleRevision, StateSHA256: canonicalHash(body)}
	_, err = tx.Exec(ctx, `INSERT INTO run_working_states(run_id,revision,operation_id,request_hash,source_rule_revision,source_owner,source_fence,actor_agent,reason,body,evidence,receipt)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, inv.RunID, receipt.Revision, inv.OperationID, hash, run.RuleRevision, inv.Owner, inv.Fence, run.Snapshot.AgentRef.ID, in.Reason, body, JSON(evidence), JSON(receipt))
	if err != nil {
		return receipt, err
	}
	if err = event(ctx, tx, inv.RunID, "working_state_committed", map[string]any{"receipt": receipt, "agent_id": run.Snapshot.AgentRef.ID}); err != nil {
		return receipt, err
	}
	return receipt, tx.Commit(ctx)
}

// WorkingReceipt can recover a committed response after cancellation/takeover.
// Reading a receipt never grants permission to resume writing or execute effects.
func (s *Store) WorkingReceipt(ctx context.Context, runID, operationID string) (WorkingStateReceipt, error) {
	var receipt WorkingStateReceipt
	var raw []byte
	err := s.DB.QueryRow(ctx, "SELECT receipt FROM run_working_states WHERE run_id=$1 AND operation_id=$2", runID, operationID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return receipt, stateError(404, "working-state receipt not found")
	}
	if err != nil {
		return receipt, err
	}
	err = json.Unmarshal(raw, &receipt)
	return receipt, err
}

// WorkingState reads authoritative instructions and one derived revision in a
// single SQL snapshot. A newer constraint revision marks the old working set
// stale; an empty ledger does not invent an initialized or persisted checkpoint.
func (s *Store) WorkingState(ctx context.Context, runID string, revision int64) (WorkingStateView, error) {
	v := WorkingStateView{RunID: runID, Evidence: []WorkingEvidence{}}
	if revision < 0 {
		return v, stateError(400, "invalid working-state revision")
	}
	var constraints, body, evidence []byte
	err := s.DB.QueryRow(ctx, `SELECT r.task,r.constraints,r.rule_revision,COALESCE(w.revision,0),COALESCE(w.source_rule_revision,0),w.body,w.evidence
 FROM runs r LEFT JOIN LATERAL (SELECT revision,source_rule_revision,body,evidence FROM run_working_states
 WHERE run_id=r.id AND ($2::bigint=0 OR revision=$2) ORDER BY revision DESC LIMIT 1) w ON true WHERE r.id=$1`, runID, revision).
		Scan(&v.Task, &constraints, &v.RuleRevision, &v.Revision, &v.SourceRuleRevision, &body, &evidence)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && revision > 0 && v.Revision == 0) {
		return v, stateError(404, "run or working-state revision not found")
	}
	if err != nil {
		return v, err
	}
	if err = json.Unmarshal(constraints, &v.Constraints); err != nil {
		return v, err
	}
	if v.Revision > 0 {
		if err = json.Unmarshal(body, &v.State); err != nil {
			return v, err
		}
		if err = json.Unmarshal(evidence, &v.Evidence); err != nil {
			return v, err
		}
		v.Stale = v.SourceRuleRevision != v.RuleRevision
	}
	return v, nil
}
