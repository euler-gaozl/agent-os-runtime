package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type noteActor struct {
	Kind        string  `json:"kind"`
	ID          string  `json:"id"`
	Reason      string  `json:"reason"`
	RunID       *string `json:"run_id,omitempty"`
	OperationID *string `json:"operation_id,omitempty"`
	RequestID   *string `json:"request_id,omitempty"`
}
type NoteRevision struct {
	ExpectedCurrentID string         `json:"expected_current_id"`
	Title             string         `json:"title"`
	Content           string         `json:"content"`
	Source            string         `json:"source"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	ValidUntil        *time.Time     `json:"valid_until,omitempty"`
	Reason            string         `json:"reason"`
}
type NoteRetirement struct {
	ExpectedCurrentID string `json:"expected_current_id"`
	Reason            string `json:"reason"`
}
type NoteLifecycle struct {
	RootID    string     `json:"root_id"`
	CurrentID *string    `json:"current_id"`
	State     string     `json:"state"`
	IsCurrent bool       `json:"is_current"`
	Expired   bool       `json:"expired"`
	Effective bool       `json:"effective"`
	RetiredAt *time.Time `json:"retired_at,omitempty"`
	Reason    string     `json:"reason,omitempty"`
}
type noteReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func noteLifecycle(ctx context.Context, q noteReader, id string) (NoteLifecycle, error) {
	var v NoteLifecycle
	e := q.QueryRow(ctx, `SELECT n.root_id,n.current_id,n.state,n.current_id IS NOT DISTINCT FROM d.id,
 COALESCE(d.valid_until<=now(),false),
 n.state='active' AND n.current_id=d.id AND (d.valid_until IS NULL OR d.valid_until>now()),n.retired_at,n.reason
 FROM documents d JOIN note_versions v ON v.document_id=d.id JOIN note_roots n ON n.root_id=v.root_id WHERE d.id=$1`, id).
		Scan(&v.RootID, &v.CurrentID, &v.State, &v.IsCurrent, &v.Expired, &v.Effective, &v.RetiredAt, &v.Reason)
	return v, e
}

func setNoteActor(ctx context.Context, tx pgx.Tx, a noteActor) error {
	_, e := tx.Exec(ctx, "SELECT set_config('agentos.note_mutation',$1,true)", string(JSON(a)))
	return e
}

func boundedText(s string, max int) bool {
	return strings.TrimSpace(s) != "" && utf8.RuneCountInString(s) <= max
}
func validateNoteMutation(id, reason string) error {
	if !boundedText(id, 200) || !boundedText(reason, 2000) {
		return &servicehttp.StatusError{Status: 400, Message: "expected_current_id and a reason of 1–2000 characters are required"}
	}
	return nil
}

// A model can read global observations but cannot revise them. Agent scope
// comes from the immutable run snapshot, never from model-supplied fields.
func noteTarget(ctx context.Context, tx pgx.Tx, id string, a noteActor) (Document, error) {
	var d Document
	e := tx.QueryRow(ctx, "SELECT id,kind,scope FROM documents WHERE id=$1", id).Scan(&d.ID, &d.Kind, &d.Scope)
	if e != nil {
		return d, e
	}
	if strings.HasPrefix(d.Scope, "knowledge:") {
		return d, &servicehttp.StatusError{Status: 403, Message: "historical Knowledge copies are read-only; manage the source in Knowledge"}
	}
	if a.Kind == "agent" && (d.Scope == "" || d.Scope != a.ID) {
		return d, &servicehttp.StatusError{Status: 403, Message: "note is outside the agent's writable scope"}
	}
	return d, nil
}

func reviseNote(ctx context.Context, tx pgx.Tx, in NoteRevision, a noteActor) (any, error) {
	if e := validateNoteMutation(in.ExpectedCurrentID, in.Reason); e != nil {
		return nil, e
	}
	if !boundedText(in.Title, 300) || !boundedText(in.Content, 20000) || !boundedText(in.Source, 2000) {
		return nil, &servicehttp.StatusError{Status: 400, Message: "revision title, content or source is missing or too long"}
	}
	metadata, e := json.Marshal(in.Metadata)
	if e != nil || len(metadata) > 32<<10 {
		return nil, &servicehttp.StatusError{Status: 400, Message: "revision metadata exceeds 32 KiB or is invalid"}
	}
	if in.ValidUntil != nil && (in.ValidUntil.Year() < 1970 || in.ValidUntil.Year() > 9999) {
		return nil, &servicehttp.StatusError{Status: 400, Message: "valid_until must be an RFC3339 date between 1970 and 9999"}
	}
	previous, e := noteTarget(ctx, tx, in.ExpectedCurrentID, a)
	if e != nil {
		return nil, e
	}
	a.Reason = in.Reason
	if e = setNoteActor(ctx, tx, a); e != nil {
		return nil, e
	}
	d, e := putDocument(ctx, tx, Document{Kind: previous.Kind, Scope: previous.Scope, Title: in.Title, Content: in.Content, Source: in.Source, Metadata: in.Metadata, ValidUntil: in.ValidUntil, Supersedes: &previous.ID})
	if e != nil {
		return nil, e
	}
	state, e := noteLifecycle(ctx, tx, d.ID)
	return map[string]any{"document": d, "lifecycle": state}, e
}

func retireNote(ctx context.Context, tx pgx.Tx, in NoteRetirement, a noteActor) (any, error) {
	if e := validateNoteMutation(in.ExpectedCurrentID, in.Reason); e != nil {
		return nil, e
	}
	d, e := noteTarget(ctx, tx, in.ExpectedCurrentID, a)
	if e != nil {
		return nil, e
	}
	// Lock the lineage, then read in a separate statement after any wait.
	if _, e = tx.Exec(ctx, `SELECT 1 FROM note_roots n JOIN note_versions v ON v.root_id=n.root_id WHERE v.document_id=$1 FOR UPDATE OF n`, d.ID); e != nil {
		return nil, e
	}
	state, e := noteLifecycle(ctx, tx, d.ID)
	if e != nil {
		return nil, e
	}
	if state.State != "active" || !state.IsCurrent {
		return nil, &servicehttp.StatusError{Status: 409, Message: "note is no longer the active head, is retired, or has unresolved branches"}
	}
	if _, e = tx.Exec(ctx, "UPDATE note_roots SET state='retired',retired_at=now(),reason=$2 WHERE root_id=$1", state.RootID, in.Reason); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO note_audits(root_id,document_id,action,reason,actor_kind,actor_id,run_id,operation_id,request_id) VALUES($1,$2,'retire',$3,$4,$5,$6,$7,$8)`, state.RootID, d.ID, in.Reason, a.Kind, a.ID, a.RunID, a.OperationID, a.RequestID); e != nil {
		return nil, e
	}
	state, e = noteLifecycle(ctx, tx, d.ID)
	return map[string]any{"document_id": d.ID, "lifecycle": state}, e
}

// Administration and model tools use the same DB trigger and lineage lock.
// Legacy POST /documents keeps its shape; the audit explicitly identifies its
// missing human reason rather than inventing one.
func (s *Store) putAdminDocument(ctx context.Context, d Document, actor string) (Document, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return d, e
	}
	defer tx.Rollback(ctx)
	if e = setNoteActor(ctx, tx, noteActor{Kind: "service", ID: actor, Reason: "management document write; legacy protocol supplies no reason"}); e != nil {
		return d, e
	}
	d, e = putDocument(ctx, tx, d)
	if e != nil {
		return d, e
	}
	return d, tx.Commit(ctx)
}

// New management mutations require a stable request_id. Retrying after a lost
// response returns the original committed result, before checking the head.
type noteRejection struct {
	status int
	cause  error
}

func (e *noteRejection) Error() string { return e.cause.Error() }
func (e *noteRejection) Unwrap() error { return e.cause }

func (s *Store) AdminNoteMutation(ctx context.Context, requestID, kind, actor string, input json.RawMessage) ([]byte, error) {
	if !boundedText(requestID, 200) {
		return nil, errors.New("request_id of 1–200 characters is required")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "note-admin:"+requestID); e != nil {
		return nil, e
	}
	hash := Hash(JSON([]any{kind, actor, json.RawMessage(input)}))
	var previousHash string
	var previous []byte
	e = tx.QueryRow(ctx, "SELECT request_hash,response FROM note_admin_receipts WHERE request_id=$1", requestID).Scan(&previousHash, &previous)
	if e == nil {
		if previousHash != hash {
			return nil, &servicehttp.StatusError{Status: 409, Message: "request_id already used with different input"}
		}
		return previous, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	a := noteActor{Kind: "service", ID: actor, RequestID: &requestID}
	var result any
	switch kind {
	case "revise":
		var in NoteRevision
		if e = json.Unmarshal(input, &in); e == nil {
			result, e = reviseNote(ctx, tx, in, a)
		}
	case "retire":
		var in NoteRetirement
		if e = json.Unmarshal(input, &in); e == nil {
			result, e = retireNote(ctx, tx, in, a)
		}
	default:
		return nil, errors.New("unsupported note mutation")
	}
	if e != nil {
		// Only classify a rejection after the receipt lookup found no prior
		// success. Authentication, transport and ambiguous commit errors must
		// never authorize the UI to discard its pending request identity.
		var status *servicehttp.StatusError
		var pg *pgconn.PgError
		if errors.As(e, &status) && (status.Status == 400 || status.Status == 403 || status.Status == 409) {
			return nil, &noteRejection{status.Status, e}
		}
		if errors.As(e, &pg) && pg.Code == "P4090" {
			return nil, &noteRejection{409, e}
		}
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, &noteRejection{404, e}
		}
		return nil, e
	}
	body := JSON(result)
	if _, e = tx.Exec(ctx, "INSERT INTO note_admin_receipts(request_id,request_hash,response) VALUES($1,$2,$3)", requestID, hash, body); e != nil {
		return nil, e
	}
	return body, tx.Commit(ctx)
}

// Search only the unique active head. An expired/retired new head never makes
// its predecessor current again; a conflicted historical lineage stays out.
func searchNotes(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, query, kind string, scopes ...string) ([]Document, error) {
	scope := ""
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	pattern := "%" + strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(query) + "%"
	rows, e := q.Query(ctx, `SELECT d.id,d.kind,d.title,left(d.content,6000),d.source,d.metadata,d.supersedes,d.scope,d.valid_until,length(d.content)>6000
 FROM note_roots n JOIN documents d ON d.id=n.current_id
 WHERE n.state='active' AND ($2='' OR d.kind=$2) AND (d.title ILIKE $1 OR d.content ILIKE $1)
 AND ($3='' OR d.scope='' OR d.scope=$3) AND (d.valid_until IS NULL OR d.valid_until>now()) ORDER BY d.created_at DESC,d.id LIMIT 30`, pattern, kind, scope)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Document{}
	for rows.Next() {
		var d Document
		var m []byte
		if e = rows.Scan(&d.ID, &d.Kind, &d.Title, &d.Content, &d.Source, &m, &d.Supersedes, &d.Scope, &d.ValidUntil, &d.Truncated); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(m, &d.Metadata); e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
