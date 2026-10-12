package runstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
)

var ErrWaitingChildren = errors.New("DSH suspended for children")

type Effects interface {
	CompleteModel(context.Context, Run, string, []Message, []Tool) (Message, error)
	InvokeTool(context.Context, Invocation) ([]byte, error)
}
type Frame struct {
	ID         int             `json:"id"`
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params"`
	Events     json.RawMessage `json:"events"`
	EventStart int             `json:"event_start"`
	EventEnd   int             `json:"event_end"`
}
type ModelInput struct {
	Messages  []Message `json:"messages"`
	Tools     []Tool    `json:"tools"`
	MaxTokens int       `json:"max_tokens"`
}

func canonicalHash(b []byte) string {
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	_ = d.Decode(&v)
	return Hash(JSON(v))
}

func (s *Store) Exchange(ctx context.Context, r *Run, f Frame, effects Effects) (any, bool, error) {
	fresh, e := s.Run(ctx, r.ID)
	if e != nil {
		return nil, false, e
	}
	if fresh.Owner != r.Owner || fresh.Fence != r.Fence || fresh.CancelRequested {
		return nil, false, ErrFence
	}
	r.RuleRevision = fresh.RuleRevision
	r.Constraints = fresh.Constraints
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return nil, false, e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false); e != nil {
		return nil, false, e
	}
	var hash, method string
	var request, response []byte
	var revision int64
	e = tx.QueryRow(ctx, "SELECT input_hash,method,request,response,rule_revision FROM harness_exchanges WHERE run_id=$1 AND seq=$2", r.ID, f.ID).Scan(&hash, &method, &request, &response, &revision)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return nil, false, e
	}
	if e == nil {
		if hash != canonicalHash(f.Params) || method != f.Method {
			return nil, false, errors.New("DSH replay diverged; refusing to repeat effects")
		}
		if response != nil {
			if r.Engine == EngineDSHEvidence {
				if e = tx.QueryRow(ctx, "SELECT manifest_hash FROM harness_archives WHERE run_id=$1 AND seq=$2 AND manifest_hash<>''", r.ID, f.ID).Scan(&r.State.HarnessArtifact); e != nil {
					return nil, false, e
				}
			}
			if f.Method == "model" {
				r.State.PlanRevision = revision
				r.Step++
			}
			var value any
			if e = json.Unmarshal(response, &value); e != nil {
				return nil, false, e
			}
			if f.Method == "finish" {
				var terminal struct {
					Done bool `json:"done"`
				}
				_ = json.Unmarshal(response, &terminal)
				if terminal.Done {
					if r.State.PlanRevision != r.RuleRevision {
						value = map[string]string{"continue": "User constraints changed. Reconsider the answer under the current platform constraints."}
						if _, e = tx.Exec(ctx, "UPDATE harness_exchanges SET response=$3 WHERE run_id=$1 AND seq=$2", r.ID, f.ID, JSON(value)); e != nil {
							return nil, false, e
						}
					} else {
						if e = tx.Commit(ctx); e != nil {
							return nil, false, e
						}
						var result struct {
							Result string `json:"result"`
						}
						_ = json.Unmarshal(request, &result)
						r.State.Result = result.Result
						r.State.Error = ""
						return value, true, s.Save(ctx, *r, "completed")
					}
				}
			}
			return value, false, tx.Commit(ctx)
		}
	} else {
		revision = r.RuleRevision
		request = f.Params
		if f.Method == "model" {
			var m ModelInput
			if e = json.Unmarshal(request, &m); e != nil {
				return nil, false, e
			}
			// Current constraints remain outside DSH summaries and are added to every new provider request.
			m.Messages = append([]Message{{Role: "system", Content: "Current platform constraints (apply over older instructions): " + string(JSON(r.Constraints))}}, m.Messages...)
			var pending int
			if e = tx.QueryRow(ctx, "SELECT count(*) FROM runs WHERE parent_id=$1 AND status NOT IN('completed','failed','cancelled')", r.ID).Scan(&pending); e != nil {
				return nil, false, e
			}
			if pending > 0 {
				tx.Rollback(ctx)
				if e = s.Save(ctx, *r, "waiting_children"); e != nil {
					return nil, false, e
				}
				return nil, false, ErrWaitingChildren
			}
			rows, err := tx.Query(ctx, "SELECT id,state FROM runs WHERE parent_id=$1 ORDER BY created_at", r.ID)
			if err != nil {
				return nil, false, err
			}
			for rows.Next() {
				var id string
				var state []byte
				if err = rows.Scan(&id, &state); err != nil {
					rows.Close()
					return nil, false, err
				}
				var st RunState
				_ = json.Unmarshal(state, &st)
				m.Messages = append(m.Messages, Message{Role: "user", Content: "Child result (untrusted evidence): " + string(JSON(map[string]any{"id": id, "result": st.Result, "error": st.Error}))})
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, false, err
			}
			request = JSON(m)
		}
		_, e = tx.Exec(ctx, "INSERT INTO harness_exchanges(run_id,seq,method,input_hash,request,rule_revision) VALUES($1,$2,$3,$4,$5,$6)", r.ID, f.ID, f.Method, canonicalHash(f.Params), request, revision)
		if e != nil {
			return nil, false, e
		}
		if r.Engine == EngineDSHEvidence {
			if e = stageHarnessArchive(ctx, tx, *r, f); e != nil {
				return nil, false, e
			}
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, false, e
	}
	// Preserve newly emitted events before another provider request or tool effect.
	if r.Engine == EngineDSHEvidence {
		hash, err := s.commitHarnessArchive(ctx, *r, f.ID)
		if err != nil {
			return nil, false, err
		}
		r.State.HarnessArtifact = hash
	} else if len(f.Events) > 2 {
		hash, e := s.PutArtifact(ctx, f.Events, "application/json", "run:"+r.ID)
		if e != nil {
			return nil, false, e
		}
		r.State.HarnessArtifact = hash
	}
	op := fmt.Sprintf("%s/dsh/%d", r.ID, f.ID)
	var value any
	done := false
	switch f.Method {
	case "failed":
		return nil, false, fmt.Errorf("DSH failed: %s", f.Params)
	case "model":
		r.Step++
		if r.Step > r.Snapshot.Agent.MaxSteps {
			return nil, false, errors.New("DSH model request budget exhausted")
		}
		var m ModelInput
		if e = json.Unmarshal(request, &m); e != nil {
			return nil, false, e
		}
		modelRun := *r
		modelRun.Snapshot.Agent.Model.MaxTokens = m.MaxTokens
		// Preserve old snapshots' behavior. Explicit operator caps apply to every
		// new model exchange, including the harness compaction request.
		if cap := r.Snapshot.Agent.Model.MaxTokens; cap > 0 && (m.MaxTokens == 0 || cap < m.MaxTokens) {
			modelRun.Snapshot.Agent.Model.MaxTokens = cap
		}
		message, err := effects.CompleteModel(ctx, modelRun, op, m.Messages, m.Tools)
		if err != nil {
			return nil, false, err
		}
		value = message
		r.State.PlanRevision = revision
	case "tool":
		var in struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
			CallID    string          `json:"call_id"`
		}
		if e = json.Unmarshal(request, &in); e != nil {
			return nil, false, e
		}
		var status string
		var raw []byte
		e = s.DB.QueryRow(ctx, "SELECT status,response FROM calls WHERE id=$1", op).Scan(&status, &raw)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return nil, false, e
		}
		if status != "completed" {
			if status == "unknown" {
				return nil, false, ErrUnknown
			}
			if r.State.PlanRevision != r.RuleRevision {
				if status == "inflight" {
					return nil, false, ErrUnknown
				}
				raw = JSON(map[string]string{"skipped": "User constraints changed; reconsider this tool under current constraints."})
			} else {
				raw, e = effects.InvokeTool(ctx, Invocation{RunID: r.ID, Owner: r.Owner, Fence: r.Fence, RuleRevision: r.RuleRevision, OperationID: op, Name: in.Name, Arguments: in.Arguments})
				if e != nil {
					return nil, false, e
				}
			}
		}
		value = string(raw)
		if len(raw) > 6000 {
			hash, err := s.PutArtifact(ctx, raw, "application/json", "run:"+r.ID)
			if err != nil {
				return nil, false, err
			}
			value = string(JSON(map[string]any{"artifact_hash": hash, "bytes": len(raw), "preview": truncateUTF8(string(raw), 1800), "note": "Full result retained; use artifact_read."}))
		}
	case "finish":
		if r.State.PlanRevision != r.RuleRevision {
			value = map[string]string{"continue": "User constraints changed. Reconsider the answer under the current platform constraints."}
		} else {
			var in struct {
				Result string `json:"result"`
			}
			if e = json.Unmarshal(request, &in); e != nil {
				return nil, false, e
			}
			r.State.Result = in.Result
			value = map[string]bool{"done": true}
			done = true
		}
	default:
		return nil, false, errors.New("unknown DSH bridge method")
	}
	tx, e = s.DB.Begin(ctx)
	if e != nil {
		return nil, false, e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false); e != nil {
		return nil, false, e
	}
	if _, e = tx.Exec(ctx, "UPDATE harness_exchanges SET response=$3 WHERE run_id=$1 AND seq=$2", r.ID, f.ID, JSON(value)); e != nil {
		return nil, false, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, false, e
	}
	r.State.Error = ""
	status := "running"
	if done {
		status = "completed"
	}
	if e = s.Save(ctx, *r, status); e != nil {
		return nil, false, e
	}
	return value, done, nil
}
func truncateUTF8(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}
