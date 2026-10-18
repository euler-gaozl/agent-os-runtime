package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const evidenceVersionSQL = `encode(sha256(convert_to(response::text,'UTF8')),'hex')`

// Recheck both versions in the read statement, not only a preceding lookup.
func boundReceiptPredicate(arg int) string {
	return fmt.Sprintf(`($%d::jsonb->id->>'version')=`+evidenceVersionSQL+` AND ($%d::jsonb->id->>'request_version')=encode(sha256(convert_to(request::text,'UTF8')),'hex') AND ($%d::jsonb->id->>'run_id')=run_id`, arg, arg, arg)
}
func boundOperationIDs(refs map[string]evidenceRef) []string {
	ids := make([]string, 0, len(refs))
	for id := range refs {
		ids = append(ids, id)
	}
	return ids
}

type evidenceDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type evidenceRef struct {
	OperationID    string `json:"operation_id"`
	RunID          string `json:"run_id"`
	Version        string `json:"version"`
	RequestVersion string `json:"request_version"`
}
type evidenceBinding struct {
	SourceRunID        string    `json:"source_run_id"`
	SourceResultSHA256 string    `json:"source_result_sha256"`
	TargetResultSHA256 string    `json:"target_result_sha256"`
	Mode               string    `json:"mode"`
	Reason             string    `json:"reason"`
	ManifestSHA256     string    `json:"manifest_sha256"`
	ReceiptCount       int       `json:"receipt_count"`
	CreatedAt          time.Time `json:"created_at"`
}
type bindEvidenceRequest struct {
	SourceRunID                string `json:"source_run_id"`
	ExpectedSourceResultSHA256 string `json:"expected_source_result_sha256"`
	Reason                     string `json:"reason"`
}

func resultDigest(result string) string {
	h := sha256.Sum256([]byte(result))
	return hex.EncodeToString(h[:])
}

func readEvidenceBinding(ctx context.Context, db evidenceDB, root string) (*evidenceBinding, []evidenceRef, error) {
	var b evidenceBinding
	var manifest []byte
	err := db.QueryRow(ctx, `SELECT source_run_id,source_result_sha256,target_result_sha256,mode,reason,manifest,manifest_sha256,created_at FROM app_evidence_bindings WHERE run_id=$1`, root).Scan(
		&b.SourceRunID, &b.SourceResultSHA256, &b.TargetResultSHA256, &b.Mode, &b.Reason, &manifest, &b.ManifestSHA256, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var refs []evidenceRef
	if json.Unmarshal(manifest, &refs) != nil || len(refs) < 1 || len(refs) > 256 || canonicalHash(manifest) != b.ManifestSHA256 {
		return nil, nil, stateError(409, "evidence binding manifest is invalid")
	}
	b.ReceiptCount = len(refs)
	return &b, refs, nil
}

// Only document receipts are inherited. Drafts and all other tool/model data
// continue to use the original task-tree authorization in appEvidenceRuns.
func boundEvidenceVersions(ctx context.Context, db evidenceDB, app, root string) (map[string]evidenceRef, error) {
	b, refs, err := readEvidenceBinding(ctx, db, root)
	if err != nil {
		return nil, err
	}
	versions := map[string]evidenceRef{}
	if b == nil {
		return versions, nil
	}
	// Re-check application membership; a persisted link is not a cross-app grant.
	var allowed bool
	err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_runs a JOIN app_runs b ON a.app=b.app WHERE a.run_id=$1 AND b.run_id=$2 AND a.app=$3)`, root, b.SourceRunID, app).Scan(&allowed)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, stateError(404, "evidence source is outside this application")
	}
	var count int
	err = db.QueryRow(ctx, `SELECT count(*) FROM jsonb_to_recordset($1::jsonb) AS ref(operation_id text,run_id text,version text,request_version text) JOIN calls ON calls.id=ref.operation_id AND calls.run_id=ref.run_id WHERE `+documentReceipt+` AND `+evidenceVersionSQL+`=ref.version AND encode(sha256(convert_to(request::text,'UTF8')),'hex')=ref.request_version`, JSON(refs)).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count != len(refs) {
		return nil, stateError(409, "bound document receipts changed or are unavailable")
	}
	for _, ref := range refs {
		versions[ref.OperationID] = ref
	}
	if len(versions) != len(refs) {
		return nil, stateError(409, "duplicate bound document receipts")
	}
	return versions, nil
}

// Caller holds the target app_runs row lock (creation already holds it). The
// manifest is flattened once, so subsequent revisions do not recursively fan out.
func bindAppEvidence(ctx context.Context, tx pgx.Tx, app, target, mode string, in bindEvidenceRequest) (*evidenceBinding, error) {
	if !safeName.MatchString(in.SourceRunID) || in.SourceRunID == target || strings.TrimSpace(in.Reason) == "" || len([]rune(in.Reason)) > 500 ||
		(in.ExpectedSourceResultSHA256 != "" && (len(in.ExpectedSourceResultSHA256) != 64 || strings.Trim(in.ExpectedSourceResultSHA256, "0123456789abcdef") != "")) {
		return nil, stateError(400, "valid source task, digest and reason required")
	}
	var targetStatus, targetResult string
	var targetAt time.Time
	err := tx.QueryRow(ctx, `SELECT r.status,coalesce(r.state->>'result',''),r.created_at FROM app_runs a JOIN runs r ON r.id=a.run_id WHERE a.app=$1 AND a.run_id=$2 FOR UPDATE OF a`, app, target).Scan(&targetStatus, &targetResult, &targetAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, stateError(404, "app task not found")
	}
	if err != nil {
		return nil, err
	}
	existing, _, err := readEvidenceBinding(ctx, tx, target)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.SourceRunID != in.SourceRunID || existing.Reason != in.Reason || existing.Mode != mode || (in.ExpectedSourceResultSHA256 != "" && existing.SourceResultSHA256 != in.ExpectedSourceResultSHA256) {
			return nil, stateError(409, "evidence binding is immutable")
		}
		return existing, nil
	}
	terminal := func(status string) bool { return status == "completed" || status == "failed" || status == "cancelled" }
	if mode == "retrospective" && !terminal(targetStatus) {
		return nil, stateError(409, "only a terminal task can receive a retrospective evidence binding")
	}
	var sourceStatus, sourceResult string
	var sourceAt time.Time
	err = tx.QueryRow(ctx, `SELECT r.status,coalesce(r.state->>'result',''),r.created_at FROM app_runs a JOIN runs r ON r.id=a.run_id WHERE a.app=$1 AND a.run_id=$2`, app, in.SourceRunID).Scan(&sourceStatus, &sourceResult, &sourceAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, stateError(404, "app source task not found")
	}
	if err != nil {
		return nil, err
	}
	if !terminal(sourceStatus) || !sourceAt.Before(targetAt) {
		return nil, stateError(409, "evidence source must be an earlier terminal task")
	}
	digest := resultDigest(sourceResult)
	if in.ExpectedSourceResultSHA256 != "" && digest != in.ExpectedSourceResultSHA256 {
		return nil, stateError(409, "source result changed")
	}
	ids, err := appEvidenceTree(ctx, tx, app, in.SourceRunID)
	if err != nil {
		return nil, err
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE id=ANY($1) AND status NOT IN ('completed','failed','cancelled'))`, ids).Scan(&active); err != nil {
		return nil, err
	}
	if active {
		return nil, stateError(409, "source task tree is still active")
	}
	versions, err := boundEvidenceVersions(ctx, tx, app, in.SourceRunID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id,run_id,`+evidenceVersionSQL+`,encode(sha256(convert_to(request::text,'UTF8')),'hex') FROM calls WHERE (run_id=ANY($1) OR id=ANY($3)) AND (NOT ($2::jsonb ? id) OR (`+boundReceiptPredicate(2)+`)) AND `+documentReceipt+` ORDER BY id LIMIT 257`, ids, JSON(versions), boundOperationIDs(versions))
	if err != nil {
		return nil, err
	}
	refs := []evidenceRef{}
	for rows.Next() {
		var ref evidenceRef
		if err = rows.Scan(&ref.OperationID, &ref.RunID, &ref.Version, &ref.RequestVersion); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, ref)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(refs) < 1 || len(refs) > 256 {
		return nil, stateError(409, "source evidence manifest must contain 1 to 256 retained document receipts")
	}
	manifest := JSON(refs)
	if len(manifest) > 200000 {
		return nil, stateError(409, "source evidence manifest exceeds storage bound")
	}
	_, err = tx.Exec(ctx, `INSERT INTO app_evidence_bindings(run_id,source_run_id,source_result_sha256,target_result_sha256,mode,reason,manifest,manifest_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, target, in.SourceRunID, digest, resultDigest(targetResult), mode, in.Reason, manifest, canonicalHash(manifest))
	if err != nil {
		return nil, err
	}
	result, _, err := readEvidenceBinding(ctx, tx, target)
	return result, err
}

func (s *Server) evidenceBindingRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/apps/{app}/runs/{id}/evidence-binding", wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in bindEvidenceRequest
		if err := decodeLimit(w, r, &in, 4096); err != nil {
			return err
		}
		if in.ExpectedSourceResultSHA256 == "" {
			return stateError(400, "expected source result digest required")
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		tx, err := s.Store.DB.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		b, err := bindAppEvidence(ctx, tx, r.PathValue("app"), r.PathValue("id"), "retrospective", in)
		if err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, b)
		return nil
	}))
}
