package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/euler-gaozl/agent-os-sdk/toolcontract"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
)

var builtinTool = toolcontract.Builtin
var ToolDefinitions = toolcontract.ToolDefinitions
var compileSchema = toolcontract.CompileSchema
var validateArgs = toolcontract.ValidateArgs
var connectorForTool = toolcontract.ConnectorForTool
var safeName = contracts.SafeName

func tool(name, description, schema string) Tool {
	return Tool{Name: name, Description: description, InputSchema: json.RawMessage(schema)}
}
func authorize(r Run, inv Invocation) error {
	ts, e := ToolDefinitions(r.Snapshot)
	if e != nil {
		return e
	}
	for _, t := range ts {
		if t.Name == inv.Name {
			return validateArgs(t, inv.Arguments)
		}
	}
	return errors.New("tool is not bound to this agent revision")
}

// Local effects and operation receipts commit together while the run fence is locked.
func (s *Store) LocalTool(ctx context.Context, inv Invocation) ([]byte, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	r, e := checkInvocation(ctx, tx, inv)
	if e != nil {
		return nil, e
	}
	if e = authorize(r, inv); e != nil {
		return nil, e
	}
	old, status, e := existingCall(ctx, tx, inv)
	if e != nil {
		return nil, e
	}
	if status == "completed" {
		return old, tx.Commit(ctx)
	}
	if status != "" && status != "retryable" {
		return nil, ErrUnknown
	}
	var result any
	noteMutation := inv.Name == "note_revise" || inv.Name == "note_retire"
	if noteMutation {
		if _, e = tx.Exec(ctx, "SAVEPOINT note_effect"); e != nil {
			return nil, e
		}
	}
	switch inv.Name {
	case "run_recall":
		result, e = recallRunEvidence(ctx, tx, r, inv.Arguments)
	case "knowledge_search":
		var args struct{ Query, Kind string }
		if e = json.Unmarshal(inv.Arguments, &args); e == nil {
			result, e = searchDocuments(ctx, tx, args.Query, args.Kind, r.Snapshot.AgentRef.ID)
		}
	case "note_save":
		var d Document
		if e = json.Unmarshal(inv.Arguments, &d); e == nil {
			d.Scope = r.Snapshot.AgentRef.ID
			if e = setNoteActor(ctx, tx, noteActor{Kind: "agent", ID: r.Snapshot.AgentRef.ID, Reason: "new sourced observation", RunID: &r.ID, OperationID: &inv.OperationID}); e != nil {
				return nil, e
			}
			result, e = putDocument(ctx, tx, d)
		}
	case "note_revise":
		var in NoteRevision
		if e = json.Unmarshal(inv.Arguments, &in); e == nil {
			result, e = reviseNote(ctx, tx, in, noteActor{Kind: "agent", ID: r.Snapshot.AgentRef.ID, RunID: &r.ID, OperationID: &inv.OperationID})
		}
	case "note_retire":
		var in NoteRetirement
		if e = json.Unmarshal(inv.Arguments, &in); e == nil {
			result, e = retireNote(ctx, tx, in, noteActor{Kind: "agent", ID: r.Snapshot.AgentRef.ID, RunID: &r.ID, OperationID: &inv.OperationID})
		}
	case "document_read":
		var args struct {
			ID     string
			Offset int
			Limit  int
		}
		if e = json.Unmarshal(inv.Arguments, &args); e != nil {
			return nil, e
		}
		var d Document
		var metadata []byte
		e = tx.QueryRow(ctx, "SELECT id,kind,title,content,source,metadata,supersedes,scope,valid_until FROM documents WHERE id=$1 AND (scope='' OR scope=$2)", args.ID, r.Snapshot.AgentRef.ID).Scan(&d.ID, &d.Kind, &d.Title, &d.Content, &d.Source, &metadata, &d.Supersedes, &d.Scope, &d.ValidUntil)
		if e == nil {
			if e = json.Unmarshal(metadata, &d.Metadata); e != nil {
				return nil, e
			}
			chars := []rune(d.Content)
			if args.Offset > len(chars) {
				return nil, errors.New("offset exceeds document length")
			}
			if args.Limit == 0 {
				args.Limit = 1200
			}
			end := min(args.Offset+args.Limit, len(chars))
			d.Content = string(chars[args.Offset:end])
			var state NoteLifecycle
			state, e = noteLifecycle(ctx, tx, d.ID)
			result = map[string]any{"document": d, "next_offset": end, "has_more": end < len(chars), "total_characters": len(chars), "lifecycle": state}
		}
	case "artifact_read":
		var args struct {
			Hash   string
			Offset int
			Limit  int
		}
		if e = json.Unmarshal(inv.Arguments, &args); e != nil {
			return nil, e
		}
		if s.StorageURL != "" {
			if s.Remote == nil {
				return nil, errors.New("artifact store client is not configured")
			}
			rows, err := tx.Query(ctx, `WITH RECURSIVE visible AS (
SELECT id FROM runs WHERE id=$1 UNION ALL SELECT child.id FROM runs child JOIN visible parent ON child.parent_id=parent.id
) SELECT 'run:'||id FROM visible LIMIT 129`, r.ID)
			if err != nil {
				return nil, err
			}
			roots := []string{}
			for rows.Next() {
				var root string
				if err = rows.Scan(&root); err != nil {
					rows.Close()
					return nil, err
				}
				roots = append(roots, root)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			if len(roots) > 128 {
				return nil, errors.New("artifact visibility hierarchy exceeds limit")
			}
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			e = s.Remote.Request(readCtx, "POST", s.StorageURL+"/v1/artifacts/page", artifactPageRequest{Hash: args.Hash, Offset: args.Offset, Limit: args.Limit, Roots: roots, AdoptRoot: "run:" + r.ID}, &result)
			cancel()
			break
		}
		return nil, errors.New("artifact service is not configured")
	case "spawn_agent":
		var args struct {
			AgentID string `json:"agent_id"`
			Task    string `json:"task"`
		}
		if e = json.Unmarshal(inv.Arguments, &args); e != nil {
			return nil, e
		}
		child, ok := r.Snapshot.Children[args.AgentID]
		if !ok {
			return nil, errors.New("child agent not permitted by snapshot")
		}
		var n int
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM runs WHERE parent_id=$1", r.ID).Scan(&n); e != nil {
			return nil, e
		}
		if n >= r.Snapshot.Agent.MaxChildren {
			return nil, errors.New("child agent budget exhausted")
		}
		var id string
		id, e = insertRun(ctx, tx, "spawn:"+inv.OperationID, args.Task, child, &r.ID, r.Constraints)
		result = map[string]string{"child_run_id": id}
	default:
		return nil, errors.New("not a local transactional tool")
	}
	if e != nil && noteMutation {
		var pg *pgconn.PgError
		var status *servicehttp.StatusError
		conflict := errors.As(e, &pg) && pg.Code == "P4090" || errors.As(e, &status) && status.Status == 409
		if conflict {
			// A CAS loser has a known, empty effect. Roll back the failed SQL
			// subtransaction, then commit a replayable rejection as its receipt.
			// HTTP 409 is reserved by the execution transport for UNKNOWN.
			if _, e = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT note_effect"); e != nil {
				return nil, e
			}
			result = map[string]any{"outcome": "conflict", "changed": false, "message": "The requested note is no longer the active head, is retired, or has unresolved branches. Read its lifecycle before deciding whether a new logical operation is appropriate."}
		}
	}
	if e != nil {
		return nil, e
	}
	if noteMutation {
		if _, e = tx.Exec(ctx, "RELEASE SAVEPOINT note_effect"); e != nil {
			return nil, e
		}
	}
	body := JSON(result)
	if e = insertCall(ctx, tx, inv, "completed", body); e != nil {
		return nil, e
	}
	if e = event(ctx, tx, r.ID, "tool_completed", map[string]any{"operation_id": inv.OperationID, "name": inv.Name}); e != nil {
		return nil, e
	}
	return body, tx.Commit(ctx)
}
func (s *Store) ExternalTool(ctx context.Context, inv Invocation, execute func(context.Context, Run) ([]byte, error)) ([]byte, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	r, e := checkInvocation(ctx, tx, inv)
	if e != nil {
		return nil, e
	}
	if e = authorize(r, inv); e != nil {
		return nil, e
	}
	old, status, e := existingCall(ctx, tx, inv)
	if e != nil {
		return nil, e
	}
	if status == "completed" {
		return old, tx.Commit(ctx)
	}
	retrySafe := inv.Name == "sandbox_exec" || inv.Name == "kb_search" || inv.Name == "document_read" || inv.Name == "skill_file_read" // Sandbox reconciles deterministic ids; retrieval is read-only.
	if c, _, found := connectorForTool(r.Snapshot, inv.Name); found && c.ReadOnly {
		retrySafe = true
	}
	if (status == "inflight" || status == "unknown") && !retrySafe {
		_, e = tx.Exec(ctx, "UPDATE calls SET status='unknown',updated_at=now() WHERE id=$1", inv.OperationID)
		if e != nil {
			return nil, e
		}
		if e = tx.Commit(ctx); e != nil {
			return nil, e
		}
		return nil, ErrUnknown
	}
	if e = insertCall(ctx, tx, inv, "inflight", nil); e != nil {
		return nil, e
	}
	if e = event(ctx, tx, r.ID, "tool_started", map[string]any{"operation_id": inv.OperationID, "name": inv.Name}); e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	result, execErr := execute(ctx, r)
	// An HTTP response alone is not a checkpoint. Persist it under the same valid fence first.
	tx, e = s.DB.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	if _, e = checkInvocation(ctx, tx, inv); e != nil {
		return nil, e
	}
	if execErr != nil {
		target := "unknown"
		if retrySafe {
			target = "retryable"
		}
		_, e = tx.Exec(ctx, "UPDATE calls SET status=$2,error=$3,updated_at=now() WHERE id=$1", inv.OperationID, target, execErr.Error())
		if e != nil {
			return nil, e
		}
		if e = event(ctx, tx, r.ID, "tool_"+target, map[string]any{"operation_id": inv.OperationID, "error": execErr.Error()}); e != nil {
			return nil, e
		}
		if e = tx.Commit(ctx); e != nil {
			return nil, e
		}
		if !retrySafe {
			return nil, ErrUnknown
		}
		return nil, execErr
	}
	if e = insertCall(ctx, tx, inv, "completed", result); e != nil {
		return nil, e
	}
	if e = event(ctx, tx, r.ID, "tool_completed", map[string]any{"operation_id": inv.OperationID, "name": inv.Name}); e != nil {
		return nil, e
	}
	return result, tx.Commit(ctx)
}
