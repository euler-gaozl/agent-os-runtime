package runstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/toolcontract"
	"github.com/jackc/pgx/v5"
)

// Recovery of these methods never reconciles an unknown external effect.
func IsFinalRecoveryMethod(engine, method string) bool {
	if !IsFinalEngine(engine) {
		return false
	}
	switch method {
	case "checkpoint", "working_state_read", "working_state_update", "final_output_select", "finish_ref":
		return true
	}
	return false
}

func hasFinalOutput(s Snapshot) bool {
	if s.Agent.FinalOutput != nil {
		return true
	}
	for _, c := range s.Children {
		if hasFinalOutput(c) {
			return true
		}
	}
	return false
}
func validateFinalSnapshot(s Snapshot, engine string) error {
	if s.Agent.FinalOutput != nil {
		if !IsFinalEngine(engine) {
			return errors.New("final output policy requires final-v5")
		}
		if err := s.Agent.FinalOutput.Validate(); err != nil {
			return err
		}
		tools, err := toolcontract.ToolDefinitions(s)
		if err != nil {
			return err
		}
		found := false
		for _, t := range tools {
			if t.Name == contracts.FinalOutputToolName || t.Name == "working_state_update" {
				return errors.New("reserved final engine tool collision")
			}
			if t.Name == s.Agent.FinalOutput.Tool {
				found = true
			}
		}
		if !found {
			return errors.New("final candidate tool is not bound in snapshot")
		}
	}
	for _, c := range s.Children {
		if err := validateFinalSnapshot(c, engine); err != nil {
			return err
		}
	}
	return nil
}

// Never trim/stringify. The checked string is the original UTF-8 value, even
// when it happens to contain JSON. Readiness is structural, not factual truth.
func finalCandidateText(p *contracts.FinalOutputPolicy, arguments, response []byte) (string, bool, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(arguments, &fields) != nil {
		return "", false, false
	}
	raw := fields[p.InputField]
	var text string
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &text) != nil {
		return "", false, false
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(response, &result) != nil || result == nil {
		return text, false, true
	}
	if b, ok := result["isError"]; ok && !bytes.Equal(bytes.TrimSpace(b), []byte("false")) {
		return text, false, true
	}
	cursor := json.RawMessage(response)
	for _, key := range p.ReadyPath {
		var object map[string]json.RawMessage
		if json.Unmarshal(cursor, &object) != nil || object == nil {
			return text, false, true
		}
		cursor = object[key]
	}
	return text, bytes.Equal(bytes.TrimSpace(cursor), []byte("true")), true
}

type finalSelection struct {
	Selected     bool   `json:"selected"`
	SelectionID  string `json:"selection_id"`
	OperationID  string `json:"operation_id"`
	SHA256       string `json:"sha256"`
	PolicySHA256 string `json:"policy_sha256"`
	RuleRevision int64  `json:"rule_revision"`
	CallID       string `json:"call_id"`
}
type finalSelectInput struct {
	OperationID string `json:"operation_id"`
	SHA256      string `json:"sha256"`
	CallID      string `json:"call_id"`
}
type finalFinishInput struct {
	SelectionID string `json:"selection_id"`
}

func finalDecode(raw []byte, target any) error {
	if len(raw) > 2048 || !json.Valid(raw) {
		return errors.New("invalid final output request")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func finalCandidate(ctx context.Context, tx pgx.Tx, r Run, operation string) (string, error) {
	p := r.Snapshot.Agent.FinalOutput
	if p == nil {
		return "", errors.New("no frozen final output policy")
	}
	seq, err := budgetExchangeSeq(r, operation)
	if err != nil {
		return "", err
	}
	var latest int
	if err = tx.QueryRow(ctx, `SELECT seq FROM harness_exchanges WHERE run_id=$1 AND method='tool' AND request->>'name'=$2 ORDER BY seq DESC LIMIT 1`, r.ID, p.Tool).Scan(&latest); err != nil {
		return "", err
	}
	if latest != seq {
		return "", errors.New("candidate is not the latest submission")
	}
	var request, callRequest, response []byte
	var revision int64
	var manifest, status, kind, callRun, callError string
	err = tx.QueryRow(ctx, `SELECT e.request,e.rule_revision,a.manifest_hash,c.request,c.response,c.status,c.kind,c.run_id,c.error
 FROM harness_exchanges e JOIN harness_archives a USING(run_id,seq)
 JOIN calls c ON c.id=$3 WHERE e.run_id=$1 AND e.seq=$2`, r.ID, seq, operation).Scan(&request, &revision, &manifest, &callRequest, &response, &status, &kind, &callRun, &callError)
	if err != nil {
		return "", err
	}
	if revision != r.RuleRevision || manifest == "" || status != "completed" || kind != "tool" || callRun != r.ID || callError != "" {
		return "", errors.New("candidate lacks a current committed tool receipt")
	}
	var invocation Invocation
	var in struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(request, &in) != nil || json.Unmarshal(callRequest, &invocation) != nil || in.Name != p.Tool || invocation.Name != p.Tool || invocation.RunID != r.ID || invocation.OperationID != operation || invocation.RuleRevision != revision || canonicalHash(in.Arguments) != canonicalHash(invocation.Arguments) {
		return "", errors.New("candidate request differs from committed tool input")
	}
	text, ready, valid := finalCandidateText(p, invocation.Arguments, response)
	if !valid || !ready {
		return "", errors.New("latest candidate is not ready for delivery")
	}
	return text, nil
}

// Bind selection to the actual last committed generation, including sole-call
// semantics. A forged bridge call cannot manufacture a model decision.
func finalModelCall(ctx context.Context, tx pgx.Tx, r Run, in finalSelectInput) error {
	var raw []byte
	var revision int64
	var purpose string
	if err := tx.QueryRow(ctx, `SELECT response,rule_revision,request->>'purpose' FROM harness_exchanges WHERE run_id=$1 AND method='model' ORDER BY seq DESC LIMIT 1`, r.ID).Scan(&raw, &revision, &purpose); err != nil {
		return err
	}
	var reply ModelCompletion
	if json.Unmarshal(raw, &reply) != nil || reply.Kind != "completion" || reply.Format != 2 || purpose != "generation" || revision != r.RuleRevision || len(reply.Message.ToolCalls) != 1 {
		return errors.New("selection lacks a current sole model tool call")
	}
	call := reply.Message.ToolCalls[0]
	var args struct {
		OperationID string `json:"operation_id"`
		SHA256      string `json:"sha256"`
	}
	if call.Function.Name != contracts.FinalOutputToolName || call.ID != in.CallID || in.CallID == "" || len(in.CallID) > 240 || finalDecode([]byte(call.Function.Arguments), &args) != nil || args.OperationID != in.OperationID || args.SHA256 != in.SHA256 {
		return errors.New("selection differs from the committed model call")
	}
	return nil
}
func finalUnresolved(ctx context.Context, tx pgx.Tx, r Run) error {
	var pending bool
	err := tx.QueryRow(ctx, `WITH RECURSIVE family AS(SELECT id,status FROM runs WHERE id=$1 UNION ALL SELECT r.id,r.status FROM runs r JOIN family f ON r.parent_id=f.id)
 SELECT EXISTS(SELECT 1 FROM family WHERE id<>$1 AND status NOT IN('completed','failed','cancelled'))
 OR EXISTS(SELECT 1 FROM calls c JOIN family f ON f.id=c.run_id WHERE c.status IN('inflight','unknown'))`, r.ID).Scan(&pending)
	if err != nil {
		return err
	}
	if pending {
		return errors.New("unresolved calls or children prevent final delivery")
	}
	return nil
}

// V5 adds a small projection to checkpoint receipts. Original event bytes stay
// in the existing immutable archive. Finish verifies the actual successful
// tool result and completed turn were archived AFTER the selected receipt.
type finalEvent struct {
	Type          string `json:"type"`
	Turn          int    `json:"turn"`
	CallID        string `json:"call_id,omitempty"`
	Success       bool   `json:"success,omitempty"`
	ReceiptSHA256 string `json:"receipt_sha256,omitempty"`
}

func finalEvents(raw []byte) []finalEvent {
	var events []struct {
		Type string `json:"type"`
		Data struct {
			Turn   int `json:"turn"`
			Reason struct {
				Kind string `json:"kind"`
			} `json:"reason"`
			Message struct {
				IsError *bool `json:"isError"`
				Source  struct {
					CallID string `json:"callId"`
				} `json:"source"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &events) != nil {
		return nil
	}
	out := []finalEvent{}
	for _, e := range events {
		switch e.Type {
		case "turn/start", "assistant/message", "assistant/attempt":
			out = append(out, finalEvent{Type: e.Type, Turn: e.Data.Turn})
		case "turn/end":
			out = append(out, finalEvent{Type: e.Type, Turn: e.Data.Turn, Success: e.Data.Reason.Kind == "completed"})
		case "tool/result":
			m := e.Data.Message
			entry := finalEvent{Type: e.Type, Turn: e.Data.Turn, CallID: m.Source.CallID}
			if m.IsError != nil && !*m.IsError && len(m.Content) == 1 && m.Content[0].Type == "text" && json.Valid([]byte(m.Content[0].Text)) {
				entry.Success = true
				entry.ReceiptSHA256 = canonicalHash([]byte(m.Content[0].Text))
			}
			out = append(out, entry)
		}
	}
	return out
}
func finalEndArchived(ctx context.Context, tx pgx.Tx, r Run, selection finalSelection, selectionRaw []byte, seq int) error {
	selectedSeq, err := budgetExchangeSeq(r, selection.SelectionID)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT e.response FROM harness_exchanges e JOIN harness_archives a USING(run_id,seq) WHERE e.run_id=$1 AND e.seq>$2 AND e.seq<$3 AND e.method='checkpoint' AND a.manifest_hash<>'' ORDER BY e.seq`, r.ID, selectedSeq, seq)
	if err != nil {
		return err
	}
	defer rows.Close()
	success, ended := false, false
	turn := 0
	for rows.Next() {
		var raw []byte
		var receipt struct {
			Events []finalEvent `json:"finalization_events"`
		}
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		if json.Unmarshal(raw, &receipt) != nil {
			return errors.New("invalid final checkpoint")
		}
		for _, e := range receipt.Events {
			switch e.Type {
			case "turn/start", "assistant/message", "assistant/attempt":
				success, ended = false, false
			case "tool/result":
				if e.CallID == selection.CallID {
					success = e.Success && e.ReceiptSHA256 == canonicalHash(selectionRaw)
					ended = false
					turn = e.Turn
				}
			case "turn/end":
				ended = success && e.Success && e.Turn == turn
			}
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if !ended {
		return errors.New("successful final tool result and turn end not durably acknowledged")
	}
	return nil
}

func (s *Store) finalExchange(ctx context.Context, r *Run, f Frame, plan int64) (any, bool, error) {
	if !IsFinalEngine(r.Engine) {
		return nil, false, errors.New("final protocol requires a final-capable engine")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	if err = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false, r.Engine); err != nil {
		return nil, false, err
	}
	current, err := ScanRun(tx.QueryRow(ctx, "SELECT "+RunColumns+" FROM runs WHERE id=$1", r.ID))
	if err != nil {
		return nil, false, err
	}
	var raw, response []byte
	var method, manifest string
	var revision int64
	err = tx.QueryRow(ctx, `SELECT e.request,e.response,e.method,e.rule_revision,a.manifest_hash FROM harness_exchanges e JOIN harness_archives a USING(run_id,seq) WHERE e.run_id=$1 AND e.seq=$2`, r.ID, f.ID).Scan(&raw, &response, &method, &revision, &manifest)
	if err != nil {
		return nil, false, err
	}
	if method != f.Method || canonicalHash(raw) != canonicalHash(f.Params) || manifest == "" || revision != plan {
		return nil, false, errors.New("final intent or archive mismatch")
	}
	if response != nil {
		var v any
		if err = json.Unmarshal(response, &v); err != nil {
			return nil, false, err
		}
		return v, false, tx.Commit(ctx)
	}
	var value any
	var result string
	done := false
	validation := validateFinalSnapshot(current.Snapshot, current.Engine)
	if validation == nil && (current.Snapshot.Agent.FinalOutput == nil || revision != current.RuleRevision || r.State.PlanRevision != current.RuleRevision) {
		validation = errors.New("final selection must use current frozen policy and constraints")
	}
	if validation == nil {
		validation = finalUnresolved(ctx, tx, current)
	}
	if f.Method == "final_output_select" {
		var in finalSelectInput
		if validation == nil {
			validation = finalDecode(raw, &in)
		}
		if validation == nil {
			validation = finalModelCall(ctx, tx, current, in)
		}
		if validation == nil {
			result, validation = finalCandidate(ctx, tx, current, in.OperationID)
		}
		if validation == nil && Hash([]byte(result)) != in.SHA256 {
			validation = errors.New("candidate hash mismatch")
		}
		if validation == nil {
			value = finalSelection{true, fmt.Sprintf("%s/dsh/%d", r.ID, f.ID), in.OperationID, in.SHA256, Hash(JSON(current.Snapshot.Agent.FinalOutput)), current.RuleRevision, in.CallID}
		}
	} else {
		var in finalFinishInput
		var selected finalSelection
		var selectedRaw []byte
		var selectedRevision int64
		var selectedManifest string
		if validation == nil {
			validation = finalDecode(raw, &in)
		}
		var selectSeq int
		if validation == nil {
			selectSeq, validation = budgetExchangeSeq(current, in.SelectionID)
		}
		if validation == nil && selectSeq >= f.ID {
			validation = errors.New("selection must precede final delivery")
		}
		if validation == nil {
			validation = tx.QueryRow(ctx, `SELECT e.response,e.rule_revision,a.manifest_hash FROM harness_exchanges e JOIN harness_archives a USING(run_id,seq) WHERE e.run_id=$1 AND e.seq=$2 AND e.method='final_output_select'`, r.ID, selectSeq).Scan(&selectedRaw, &selectedRevision, &selectedManifest)
		}
		if validation == nil && (json.Unmarshal(selectedRaw, &selected) != nil || !selected.Selected || selected.SelectionID != in.SelectionID || selectedManifest == "" || selectedRevision != current.RuleRevision || selected.RuleRevision != current.RuleRevision || selected.PolicySHA256 != Hash(JSON(current.Snapshot.Agent.FinalOutput))) {
			validation = errors.New("selection is absent or stale")
		}
		if validation == nil {
			validation = finalModelCall(ctx, tx, current, finalSelectInput{selected.OperationID, selected.SHA256, selected.CallID})
		}
		if validation == nil {
			result, validation = finalCandidate(ctx, tx, current, selected.OperationID)
		}
		if validation == nil && Hash([]byte(result)) != selected.SHA256 {
			validation = errors.New("selected input hash differs")
		}
		if validation == nil {
			validation = finalEndArchived(ctx, tx, current, selected, selectedRaw, f.ID)
		}
		if validation == nil {
			value = map[string]any{"done": true, "selection_id": selected.SelectionID, "sha256": selected.SHA256, "policy_sha256": selected.PolicySHA256, "operation_id": selected.OperationID, "rule_revision": current.RuleRevision}
			done = true
		}
	}
	if validation != nil {
		// Database/transport failures must remain retryable, never a frozen business rejection.
		if !errors.Is(validation, pgx.ErrNoRows) {
			var pgErr interface{ SQLState() string }
			if errors.As(validation, &pgErr) || ctx.Err() != nil {
				return nil, false, validation
			}
		}
		value = map[string]any{"continue": "Final delivery rejected: " + validation.Error() + ". Recheck the latest candidate under current constraints."}
		if f.Method == "final_output_select" {
			value.(map[string]any)["selected"] = false
		} else {
			value.(map[string]any)["done"] = false
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE harness_exchanges SET response=$3 WHERE run_id=$1 AND seq=$2", r.ID, f.ID, JSON(value)); err != nil {
		return nil, false, err
	}
	current.State.HarnessArtifact = manifest
	current.State.Error = ""
	if done {
		current.State.Result = result
		if _, err = tx.Exec(ctx, "UPDATE runs SET state=$2,status='completed',updated_at=now() WHERE id=$1", r.ID, JSON(current.State)); err != nil {
			return nil, false, err
		}
		if err = EmitEvent(ctx, tx, r.ID, "final_output_completed", value); err != nil {
			return nil, false, err
		}
		if err = EmitEvent(ctx, tx, r.ID, "checkpoint", map[string]any{"step": current.Step, "status": "completed", "result": result, "error": ""}); err != nil {
			return nil, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	if done {
		*r = current
		r.Status = "completed"
	}
	return value, done, nil
}

func (s *Store) replayFinalCompletion(ctx context.Context, r *Run, current Run, f Frame) (any, bool, error) {
	var in finalFinishInput
	if finalDecode(f.Params, &in) != nil {
		return nil, false, errors.New("invalid final replay")
	}
	var response []byte
	var hash, method string
	if err := s.DB.QueryRow(ctx, "SELECT input_hash,method,response FROM harness_exchanges WHERE run_id=$1 AND seq=$2", r.ID, f.ID).Scan(&hash, &method, &response); err != nil {
		return nil, false, err
	}
	var value struct {
		Done        bool   `json:"done"`
		SelectionID string `json:"selection_id"`
		SHA256      string `json:"sha256"`
	}
	if hash != canonicalHash(f.Params) || method != "finish_ref" || json.Unmarshal(response, &value) != nil || !value.Done || value.SelectionID != in.SelectionID || value.SHA256 != Hash([]byte(current.State.Result)) {
		return nil, false, errors.New("completed final replay mismatch")
	}
	*r = current
	return json.RawMessage(response), true, nil
}
