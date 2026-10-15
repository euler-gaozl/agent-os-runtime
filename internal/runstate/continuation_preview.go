package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrContinuationRunNotFound distinguishes an absent requested run from a
// missing/inconsistent internal receipt. Only the former is an HTTP 404.
var ErrContinuationRunNotFound = errors.New("continuation run not found")

const ContinuationPreviewProtocol = "failed-run-continuation-preview-v1"

type ContinuationBasis struct {
	Agent               *Ref   `json:"agent,omitempty"`
	ModelRounds         *int64 `json:"model_rounds,omitempty"`
	MaxSteps            *int   `json:"max_steps,omitempty"`
	FamilyModelAttempts *int64 `json:"family_model_attempts,omitempty"`
	MaxModelAttempts    *int   `json:"max_model_attempts,omitempty"`
	RetryHTTPStatus     *int   `json:"retry_http_status,omitempty"`
}

// A candidate is a read-only observation, never an execution permission. This
// version deliberately provides no mutation endpoint or automatic retry policy.
type ContinuationPreview struct {
	Protocol        string             `json:"protocol"`
	RunID           string             `json:"run_id"`
	Engine          string             `json:"engine"`
	Status          string             `json:"status"`
	Assessment      string             `json:"assessment"`
	Reason          string             `json:"reason"`
	ReadOnly        bool               `json:"read_only"`
	ActionAvailable bool               `json:"action_available"`
	ObservedAt      time.Time          `json:"observed_at"`
	Basis           *ContinuationBasis `json:"basis,omitempty"`
}

// NativeReceipt adds a small fixed JSON envelope to its bounded body. Twice the
// native response bound also accommodates PostgreSQL's JSONB text formatting;
// ReadNativeReceipt still enforces the original body limit after decoding.
const continuationReceiptBytes = 2 * NativeResponseBytes

func continuationClassify(v *ContinuationPreview, assessment, reason string) {
	v.Assessment, v.Reason = assessment, reason
}

// ValidateNativeSnapshot does not check MaxSteps and Agent.Validate fills in
// defaults. Inspect already-frozen bounds without modifying them, recursively.
func validContinuationBounds(snap Snapshot, depth int) bool {
	if depth > 2 || snap.Agent.MaxSteps < 1 || snap.Agent.MaxSteps > 100 ||
		snap.Agent.MaxModelAttempts < 1 || snap.Agent.MaxModelAttempts > 500 {
		return false
	}
	for _, child := range snap.Children {
		if !validContinuationBounds(child, depth+1) {
			return false
		}
	}
	return true
}

// FailedContinuationPreview inspects one consistent, bounded Owner snapshot.
// It neither locks execution rows nor reserves budget; even a positive result
// cannot be used to authorize a later mutation without a fresh locked check.
func (s *Store) FailedContinuationPreview(ctx context.Context, id string) (ContinuationPreview, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ContinuationPreview{}, err
	}
	defer tx.Rollback(ctx)
	v, err := readFailedContinuationPreview(ctx, tx, id)
	if err != nil {
		return ContinuationPreview{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ContinuationPreview{}, err
	}
	return v, nil
}

func readFailedContinuationPreview(ctx context.Context, tx pgx.Tx, id string) (ContinuationPreview, error) {
	v := ContinuationPreview{Protocol: ContinuationPreviewProtocol, RunID: id, ReadOnly: true}
	r := Run{ID: id}
	var parent *string
	var snapshot []byte
	var snapshotBytes int64
	// Do not read task/state/model history just to determine continuation. CASE
	// prevents an oversized frozen snapshot from crossing the DB boundary.
	err := tx.QueryRow(ctx, `SELECT parent_id,engine,status,cancel_requested,rule_revision,
 octet_length(snapshot::text),CASE WHEN octet_length(snapshot::text)<=$2 THEN snapshot END,transaction_timestamp()
 FROM runs WHERE id=$1`, id, NativeRequestBytes).Scan(&parent, &r.Engine, &r.Status,
		&r.CancelRequested, &r.RuleRevision, &snapshotBytes, &snapshot, &v.ObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrContinuationRunNotFound
	}
	if err != nil {
		return v, err
	}
	v.Engine, v.Status = r.Engine, r.Status
	if r.CancelRequested || r.Status == "cancelled" {
		continuationClassify(&v, "blocked", "cancelled")
		return v, nil
	}
	if r.Status == "completed" {
		continuationClassify(&v, "not_applicable", "already_completed")
		return v, nil
	}
	if r.Status != "failed" && r.Status != "waiting_unknown" {
		continuationClassify(&v, "not_applicable", "not_failed")
		return v, nil
	}
	familyRoot := id
	if parent != nil {
		// Resolve only lineage metadata, not the root snapshot or model bodies.
		// UNION also bounds malformed cyclic ancestry to its distinct records.
		err = tx.QueryRow(ctx, `WITH RECURSIVE lineage AS (
 SELECT id,parent_id FROM runs WHERE id=$1
 UNION SELECT r.id,r.parent_id FROM runs r JOIN lineage l ON l.parent_id=r.id
 ) SELECT id FROM lineage WHERE parent_id IS NULL`, id).Scan(&familyRoot)
		if errors.Is(err, pgx.ErrNoRows) {
			continuationClassify(&v, "manual_review", "child_requires_family_review")
			return v, nil
		}
		if err != nil {
			return v, err
		}
	}
	// Root-only positive assessments keep family membership unambiguous. All
	// attempt states count; only metadata is aggregated, not their large bodies.
	var unresolvedTool, unsettledModel, activeChildren bool
	var attempts int64
	err = tx.QueryRow(ctx, `WITH RECURSIVE family AS (
 SELECT id,status FROM runs WHERE id=$1
 UNION SELECT r.id,r.status FROM runs r JOIN family f ON r.parent_id=f.id
 ) SELECT
 EXISTS(SELECT 1 FROM family f JOIN calls c ON c.run_id=f.id WHERE c.kind='tool' AND c.status IN('unknown','inflight')),
 EXISTS(SELECT 1 FROM family f JOIN calls c ON c.run_id=f.id WHERE c.kind='model' AND
   (c.status IN('unknown','inflight') OR EXISTS(SELECT 1 FROM model_attempts a WHERE a.call_id=c.id AND a.status='inflight'))),
 EXISTS(SELECT 1 FROM family WHERE id<>$1 AND status NOT IN('completed','failed','cancelled')),
 (SELECT count(*) FROM family f JOIN calls c ON c.run_id=f.id AND c.kind='model' JOIN model_attempts a ON a.call_id=c.id)`, familyRoot).
		Scan(&unresolvedTool, &unsettledModel, &activeChildren, &attempts)
	if err != nil {
		return v, err
	}
	if unresolvedTool {
		continuationClassify(&v, "blocked", "unresolved_tool_effect")
		return v, nil
	}
	if r.Status == "waiting_unknown" {
		continuationClassify(&v, "blocked", "reconciliation_required")
		return v, nil
	}
	if r.Engine != EngineDSHNative {
		continuationClassify(&v, "manual_review", "engine_not_assessed")
		return v, nil
	}
	if parent != nil {
		continuationClassify(&v, "manual_review", "child_requires_family_review")
		return v, nil
	}
	if activeChildren {
		continuationClassify(&v, "blocked", "children_not_settled")
		return v, nil
	}
	if unsettledModel {
		continuationClassify(&v, "manual_review", "unsettled_model_attempt")
		return v, nil
	}
	if snapshotBytes > NativeRequestBytes {
		continuationClassify(&v, "manual_review", "preview_record_too_large")
		return v, nil
	}
	if json.Unmarshal(snapshot, &r.Snapshot) != nil || !validContinuationBounds(r.Snapshot, 0) || ValidateNativeSnapshot(r.Snapshot) != nil {
		continuationClassify(&v, "manual_review", "invalid_frozen_configuration")
		return v, nil
	}
	v.Basis = &ContinuationBasis{Agent: &r.Snapshot.AgentRef, MaxSteps: &r.Snapshot.Agent.MaxSteps,
		FamilyModelAttempts: &attempts, MaxModelAttempts: &r.Snapshot.Agent.MaxModelAttempts}
	var exchanges, rounds, first, last int64
	err = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE method='model'),COALESCE(min(seq),0),COALESCE(max(seq),0)
 FROM harness_exchanges WHERE run_id=$1`, id).Scan(&exchanges, &rounds, &first, &last)
	if err != nil {
		return v, err
	}
	v.Basis.ModelRounds = &rounds
	if first != 1 || last <= 0 || exchanges != last {
		continuationClassify(&v, "manual_review", "unsupported_checkpoint")
		return v, nil
	}
	var method string
	var revision int64
	var unanswered bool
	var intentBytes int64
	var intent []byte
	err = tx.QueryRow(ctx, `SELECT method,rule_revision,response IS NULL,octet_length(request::text),
 CASE WHEN octet_length(request::text)<=$3 THEN request END
 FROM harness_exchanges WHERE run_id=$1 AND seq=$2`, id, last, NativeRequestBytes).
		Scan(&method, &revision, &unanswered, &intentBytes, &intent)
	if err != nil {
		return v, err
	}
	if method != "model" || !unanswered {
		continuationClassify(&v, "manual_review", "unsupported_checkpoint")
		return v, nil
	}
	var earlierUnfrozen, archiveUnconfirmed bool
	err = tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM harness_exchanges WHERE run_id=$1 AND seq<$2 AND response IS NULL),
 EXISTS(SELECT 1 FROM harness_exchanges e LEFT JOIN harness_archives a USING(run_id,seq)
   WHERE e.run_id=$1 AND (a.seq IS NULL OR length(a.manifest_hash)<>64 OR a.manifest IS NULL OR a.batch IS NOT NULL))`, id, last).
		Scan(&earlierUnfrozen, &archiveUnconfirmed)
	if err != nil {
		return v, err
	}
	if earlierUnfrozen {
		continuationClassify(&v, "manual_review", "unsupported_checkpoint")
		return v, nil
	}
	if archiveUnconfirmed {
		continuationClassify(&v, "manual_review", "archive_unconfirmed")
		return v, nil
	}
	if revision != r.RuleRevision {
		continuationClassify(&v, "manual_review", "constraints_changed")
		return v, nil
	}
	if intentBytes > NativeRequestBytes {
		continuationClassify(&v, "manual_review", "preview_record_too_large")
		return v, nil
	}
	// A failed retry of the last model frame is still that same logical round.
	// A newly staged frame beyond MaxSteps can have no call/attempt at all.
	if rounds > int64(r.Snapshot.Agent.MaxSteps) {
		continuationClassify(&v, "blocked", "step_budget_exhausted")
		return v, nil
	}
	if attempts >= int64(r.Snapshot.Agent.MaxModelAttempts) {
		continuationClassify(&v, "blocked", "attempt_budget_exhausted")
		return v, nil
	}
	op := fmt.Sprintf("%s/dsh/%d", id, last)
	var kind, status, attemptStatus string
	var matching bool
	var requestBytes, responseBytes int64
	var request, response []byte
	err = tx.QueryRow(ctx, `SELECT c.kind,c.status,COALESCE(a.status,''),
 COALESCE(a.call_id=c.id AND a.request=c.request AND a.response IS NOT DISTINCT FROM c.response,false),
 octet_length(c.request::text),COALESCE(octet_length(c.response::text),0),
 CASE WHEN octet_length(c.request::text)<=$3 THEN c.request END,
 CASE WHEN octet_length(c.response::text)<=$4 THEN c.response END
 FROM calls c LEFT JOIN model_attempts a ON a.id=c.attempt WHERE c.run_id=$1 AND c.id=$2`,
		id, op, NativeRequestBytes, continuationReceiptBytes).
		Scan(&kind, &status, &attemptStatus, &matching, &requestBytes, &responseBytes, &request, &response)
	if errors.Is(err, pgx.ErrNoRows) {
		continuationClassify(&v, "manual_review", "failure_not_assessed")
		return v, nil
	}
	if err != nil {
		return v, err
	}
	if requestBytes > NativeRequestBytes || responseBytes > continuationReceiptBytes {
		continuationClassify(&v, "manual_review", "preview_record_too_large")
		return v, nil
	}
	if kind != "model" || status != "failed" || attemptStatus != "failed" || !matching {
		continuationClassify(&v, "manual_review", "failure_not_assessed")
		return v, nil
	}
	receipt, err := ReadNativeReceipt(response)
	if err != nil || receipt.HTTPStatus != 503 {
		continuationClassify(&v, "manual_review", "failure_not_assessed")
		return v, nil
	}
	// Classify unsupported/corrupt frozen data using the same pure native
	// validations before the existing guard, which also performs DB reads.
	// This leaves execution rules unchanged and keeps DB errors unavailable.
	var input ModelInput
	if json.Unmarshal(intent, &input) != nil ||
		(input.Purpose != "generation" && input.Purpose != "compaction") ||
		input.MaxTokens != r.Snapshot.Agent.Model.MaxTokens {
		continuationClassify(&v, "manual_review", "unsupported_checkpoint")
		return v, nil
	}
	if InspectNativeRequest(request, r.Snapshot) != nil ||
		canonicalHash(request) != canonicalHash(NativeModelRequest(r.Snapshot.Agent.Model, input.Messages, input.Tools)) {
		continuationClassify(&v, "manual_review", "failure_not_assessed")
		return v, nil
	}
	// All known data validations have passed in this same repeatable snapshot;
	// snapshot/intent blobs are bounded and the final response is SQL NULL.
	// Never disguise failures of this read-only guard as manual classifications.
	if err = checkNativeModelIntent(ctx, tx, r, op, request, false); err != nil {
		return v, err
	}
	v.Basis.RetryHTTPStatus = &receipt.HTTPStatus
	continuationClassify(&v, "same_run_candidate", "retained_transient_model_failure")
	return v, nil
}
