package runstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

const (
	ContinuationActionContextProtocol = "failed-run-continuation-action-context-v1"
	ContinuationReceiptProtocol       = "failed-run-continuation-receipt-v1"
	continuationVersionProtocol       = "failed-run-continuation-version-v1"
	continuationActionCapacity        = 128
	continuationActionReceiptBytes    = 8192
)

var (
	ErrContinuationInput           = errors.New("invalid continuation request")
	ErrContinuationReceiptMissing  = errors.New("continuation receipt not found")
	ErrContinuationRequestConflict = errors.New("request_id_conflict")
	ErrExplicitContinuationNeeded  = errors.New("explicit_continuation_request_required")
)

type ContinuationActionContext struct {
	Protocol       string    `json:"protocol"`
	RunID          string    `json:"run_id"`
	Engine         string    `json:"engine"`
	Status         string    `json:"status"`
	Assessment     string    `json:"assessment"`
	Reason         string    `json:"reason"`
	ObservedAt     time.Time `json:"observed_at"`
	FailureVersion string    `json:"failure_version,omitempty"`
}

type ContinuationRequest struct {
	RequestID              string `json:"request_id"`
	ExpectedFailureVersion string `json:"expected_failure_version"`
}

// Body is always the persisted response representation, including the initial
// INSERT RETURNING path. It describes a historical decision, not current state.
type ContinuationResult struct {
	HTTPStatus int
	Body       json.RawMessage
}

type ContinuationActionReceipt struct {
	Protocol           string     `json:"protocol"`
	RequestID          string     `json:"request_id"`
	RunID              string     `json:"run_id"`
	Outcome            string     `json:"outcome"`
	FailureVersion     string     `json:"failure_version"`
	Committed          bool       `json:"committed"`
	Assessment         string     `json:"assessment,omitempty"`
	Reason             string     `json:"reason,omitempty"`
	StatusAtAcceptance string     `json:"status_at_acceptance,omitempty"`
	AcceptedAt         *time.Time `json:"accepted_at,omitempty"`
	RejectedAt         *time.Time `json:"rejected_at,omitempty"`
}

func ValidContinuationRunID(id string) bool {
	return len(id) > 0 && len(id) <= 512 && utf8.ValidString(id) && !strings.ContainsRune(id, 0)
}

func ValidContinuationRequestID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range []byte(id) {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func ValidContinuationFailureVersion(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, c := range []byte(v) {
		if !((c >= 'a' && c <= 'f') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func continuationRequestHash(id string, in ContinuationRequest) string {
	// The actor is fixed by this Owner control-only operation, never supplied in
	// the body. Request identity is the separate global receipt primary key.
	return Hash(JSON([]any{ContinuationReceiptProtocol, "control", id, in.ExpectedFailureVersion}))
}

func continuationFailureVersion(id, engine string, revision, fence, seq int64, callID, attemptID string) string {
	return Hash(JSON([]any{continuationVersionProtocol, id, engine, revision, fence, seq, callID, attemptID}))
}

func readContinuationFailureVersion(ctx context.Context, tx pgx.Tx, id string) (string, error) {
	var engine, callID, attemptID string
	var revision, fence, seq int64
	err := tx.QueryRow(ctx, `SELECT r.engine,r.rule_revision,r.fence,h.seq,c.id,c.attempt
 FROM runs r JOIN LATERAL (SELECT seq FROM harness_exchanges WHERE run_id=r.id ORDER BY seq DESC LIMIT 1) h ON true
 JOIN calls c ON c.run_id=r.id AND c.id=r.id||'/dsh/'||h.seq::text WHERE r.id=$1`, id).
		Scan(&engine, &revision, &fence, &seq, &callID, &attemptID)
	if err != nil {
		return "", err
	}
	return continuationFailureVersion(id, engine, revision, fence, seq, callID, attemptID), nil
}

// FailedContinuationContext is a thin, same-snapshot observation. The version
// is not authorization, a budget reservation, or a guarantee of later admission.
func (s *Store) FailedContinuationContext(ctx context.Context, id string) (ContinuationActionContext, error) {
	if !ValidContinuationRunID(id) {
		return ContinuationActionContext{}, ErrContinuationInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ContinuationActionContext{}, err
	}
	defer tx.Rollback(ctx)
	// Missing action schema must not advertise a usable context before the
	// separately coordinated Owner migration. This is a zero-row read, no DDL.
	if _, err = tx.Exec(ctx, "SELECT request_id,run_id,request_hash,expected_failure_version,http_status,response,created_at FROM run_continuation_receipts WHERE false"); err != nil {
		return ContinuationActionContext{}, err
	}
	v, err := readFailedContinuationPreview(ctx, tx, id)
	if err != nil {
		return ContinuationActionContext{}, err
	}
	out := ContinuationActionContext{Protocol: ContinuationActionContextProtocol, RunID: v.RunID, Engine: v.Engine,
		Status: v.Status, Assessment: v.Assessment, Reason: v.Reason, ObservedAt: v.ObservedAt}
	if v.Assessment == "same_run_candidate" {
		out.FailureVersion, err = readContinuationFailureVersion(ctx, tx, id)
		if err != nil {
			return ContinuationActionContext{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return ContinuationActionContext{}, err
	}
	return out, nil
}

func decodeContinuationReceipt(raw []byte) (ContinuationActionReceipt, error) {
	var receipt ContinuationActionReceipt
	trimmed := bytes.TrimSpace(raw)
	if len(raw) > continuationActionReceiptBytes || !utf8.Valid(raw) || len(trimmed) == 0 || trimmed[0] != '{' {
		return receipt, errors.New("invalid stored continuation receipt")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&receipt); err != nil {
		return ContinuationActionReceipt{}, errors.New("invalid stored continuation receipt")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ContinuationActionReceipt{}, errors.New("invalid stored continuation receipt")
	}
	return receipt, nil
}

func readContinuationReceipt(ctx context.Context, tx pgx.Tx, requestID string) (string, string, ContinuationResult, error) {
	var hash, runID, expected string
	var size int64
	var result ContinuationResult
	err := tx.QueryRow(ctx, `SELECT request_hash,run_id,expected_failure_version,http_status,octet_length(response::text),
 CASE WHEN octet_length(response::text)<=$2 THEN response END FROM run_continuation_receipts WHERE request_id=$1`, requestID, continuationActionReceiptBytes).
		Scan(&hash, &runID, &expected, &result.HTTPStatus, &size, &result.Body)
	if err != nil {
		return "", "", result, err
	}
	receipt, decodeErr := decodeContinuationReceipt(result.Body)
	if size > continuationActionReceiptBytes || !ValidContinuationFailureVersion(hash) || !ValidContinuationFailureVersion(expected) ||
		hash != continuationRequestHash(runID, ContinuationRequest{ExpectedFailureVersion: expected}) ||
		decodeErr != nil || receipt.Protocol != ContinuationReceiptProtocol ||
		receipt.RequestID != requestID || receipt.RunID != runID || receipt.FailureVersion != expected || !receipt.Committed {
		return "", "", ContinuationResult{}, errors.New("invalid stored continuation receipt")
	}
	valid := result.HTTPStatus == 202 && receipt.Outcome == "queued" && receipt.StatusAtAcceptance == "queued" &&
		receipt.AcceptedAt != nil && receipt.RejectedAt == nil && receipt.Assessment == "" && receipt.Reason == ""
	valid = valid || (result.HTTPStatus == 409 && receipt.Outcome == "rejected" && receipt.StatusAtAcceptance == "" &&
		receipt.AcceptedAt == nil && receipt.RejectedAt != nil && receipt.Assessment != "" && receipt.Reason != "")
	if !valid {
		return "", "", ContinuationResult{}, errors.New("invalid stored continuation outcome")
	}
	return hash, runID, result, nil
}

// ContinuationReceipt returns a prior committed decision without evaluating
// current eligibility. No row observed is not proof a racing POST did not commit.
func (s *Store) ContinuationReceipt(ctx context.Context, id, requestID string) (ContinuationResult, error) {
	if !ValidContinuationRunID(id) || !ValidContinuationRequestID(requestID) {
		return ContinuationResult{}, ErrContinuationInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ContinuationResult{}, err
	}
	defer tx.Rollback(ctx)
	_, runID, result, err := readContinuationReceipt(ctx, tx, requestID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && runID != id) {
		return ContinuationResult{}, ErrContinuationReceiptMissing
	}
	if err != nil {
		return ContinuationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ContinuationResult{}, err
	}
	return result, nil
}

// Lock parents before discovering children, matching LockFamily. A rejected
// overflow may lock one sentinel beyond the 128-run positive capacity. Never
// hold open Rows while taking the next lock or advisory budget lock 35.
func lockContinuationFamily(ctx context.Context, tx pgx.Tx, id string) (bool, error) {
	var parent *string
	var engine string
	if err := tx.QueryRow(ctx, "SELECT parent_id,engine FROM runs WHERE id=$1 FOR UPDATE", id).Scan(&parent, &engine); err != nil {
		return false, err
	}
	if parent != nil || engine != EngineDSHNative {
		return false, errors.New("continuation root changed")
	}
	seen, frontier := map[string]bool{id: true}, []string{id}
	for len(frontier) > 0 {
		remaining := continuationActionCapacity - len(seen)
		rows, err := tx.Query(ctx, "SELECT id FROM runs WHERE parent_id=ANY($1) ORDER BY id LIMIT $2 FOR UPDATE", frontier, remaining+1)
		if err != nil {
			return false, err
		}
		next := make([]string, 0, remaining+1)
		for rows.Next() {
			var child string
			if err = rows.Scan(&child); err != nil {
				rows.Close()
				return false, err
			}
			if seen[child] {
				rows.Close()
				return false, errors.New("inconsistent continuation family")
			}
			seen[child] = true
			next = append(next, child)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, err
		}
		if len(seen) > continuationActionCapacity {
			return true, nil
		}
		frontier = next
	}
	return false, nil
}

func saveContinuationReceipt(ctx context.Context, tx pgx.Tx, id string, in ContinuationRequest, hash, assessment, reason string) (ContinuationResult, error) {
	var stamp time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&stamp); err != nil {
		return ContinuationResult{}, err
	}
	receipt := ContinuationActionReceipt{Protocol: ContinuationReceiptProtocol, RequestID: in.RequestID,
		RunID: id, FailureVersion: in.ExpectedFailureVersion, Committed: true}
	result := ContinuationResult{HTTPStatus: 409}
	if reason == "" {
		result.HTTPStatus = 202
		receipt.Outcome, receipt.StatusAtAcceptance, receipt.AcceptedAt = "queued", "queued", &stamp
	} else {
		receipt.Outcome, receipt.Assessment, receipt.Reason, receipt.RejectedAt = "rejected", assessment, reason, &stamp
	}
	err := tx.QueryRow(ctx, `INSERT INTO run_continuation_receipts(request_id,run_id,request_hash,expected_failure_version,http_status,response,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING response`, in.RequestID, id, hash, in.ExpectedFailureVersion, result.HTTPStatus, JSON(receipt), stamp).Scan(&result.Body)
	if err != nil {
		return ContinuationResult{}, err
	}
	// A commit failure may mean an unknown outcome. Never return a fabricated
	// committed rejection; callers must retain the request ID and read/retry it.
	if err = tx.Commit(ctx); err != nil {
		return ContinuationResult{}, err
	}
	return result, nil
}

// RequestFailedContinuation accepts an explicit control request, not a preview
// token. Request lock -> parent-first family rows -> budget35 -> fresh locked
// assessment. It neither adds attempts nor modifies immutable task snapshots.
func (s *Store) RequestFailedContinuation(ctx context.Context, id string, in ContinuationRequest) (ContinuationResult, error) {
	if !ValidContinuationRunID(id) || !ValidContinuationRequestID(in.RequestID) || !ValidContinuationFailureVersion(in.ExpectedFailureVersion) {
		return ContinuationResult{}, ErrContinuationInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return ContinuationResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='8s'"); err != nil {
		return ContinuationResult{}, err
	}
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "run-continuation:"+in.RequestID); err != nil {
		return ContinuationResult{}, err
	}
	hash := continuationRequestHash(id, in)
	oldHash, _, old, err := readContinuationReceipt(ctx, tx, in.RequestID)
	if err == nil {
		if oldHash != hash {
			return ContinuationResult{}, ErrContinuationRequestConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return ContinuationResult{}, err
		}
		return old, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ContinuationResult{}, err
	}
	var parent *string
	var engine string
	err = tx.QueryRow(ctx, "SELECT parent_id,engine FROM runs WHERE id=$1", id).Scan(&parent, &engine)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContinuationResult{}, ErrContinuationRunNotFound
	}
	if err != nil {
		return ContinuationResult{}, err
	}
	if parent != nil {
		return saveContinuationReceipt(ctx, tx, id, in, hash, "manual_review", "child_requires_family_review")
	}
	if engine != EngineDSHNative {
		return saveContinuationReceipt(ctx, tx, id, in, hash, "manual_review", "engine_not_assessed")
	}
	overflow, err := lockContinuationFamily(ctx, tx, id)
	if err != nil {
		return ContinuationResult{}, err
	}
	if overflow {
		return saveContinuationReceipt(ctx, tx, id, in, hash, "manual_review", "family_exceeds_action_capacity")
	}
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,35))", id); err != nil {
		return ContinuationResult{}, err
	}
	// READ COMMITTED starts a new statement snapshot after every waited lock.
	// Reuse the bounded native data/receipt checks, not the public RR Store API.
	v, err := readFailedContinuationPreview(ctx, tx, id)
	if err != nil {
		return ContinuationResult{}, err
	}
	if v.Assessment != "same_run_candidate" {
		return saveContinuationReceipt(ctx, tx, id, in, hash, v.Assessment, v.Reason)
	}
	version, err := readContinuationFailureVersion(ctx, tx, id)
	if err != nil {
		return ContinuationResult{}, err
	}
	if version != in.ExpectedFailureVersion {
		return saveContinuationReceipt(ctx, tx, id, in, hash, "blocked", "failure_version_changed")
	}
	tag, err := tx.Exec(ctx, "UPDATE runs SET status='queued',owner='',lease_until=NULL,updated_at=now() WHERE id=$1 AND status='failed' AND NOT cancel_requested", id)
	if err != nil {
		return ContinuationResult{}, err
	}
	if tag.RowsAffected() != 1 {
		return ContinuationResult{}, errors.New("locked continuation status changed")
	}
	if err = EmitEvent(ctx, tx, id, "continuation_queued", map[string]any{"request_id": in.RequestID, "failure_version": version}); err != nil {
		return ContinuationResult{}, err
	}
	return saveContinuationReceipt(ctx, tx, id, in, hash, "", "")
}

// ResumeFailedLegacy preserves historical-engine behavior while preventing
// native runs from bypassing explicit continuation request identity/receipts.
func (s *Store) ResumeFailedLegacy(ctx context.Context, id string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var engine string
	err = tx.QueryRow(ctx, "SELECT engine FROM runs WHERE id=$1 FOR UPDATE", id).Scan(&engine)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("only failed, non-cancelled runs may resume; unknown calls require reconciliation")
	}
	if err != nil {
		return err
	}
	if IsNativeEngine(engine) {
		return ErrExplicitContinuationNeeded
	}
	tag, err := tx.Exec(ctx, "UPDATE runs SET status='queued',owner='',lease_until=NULL,updated_at=now() WHERE id=$1 AND status='failed' AND NOT cancel_requested", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("only failed, non-cancelled runs may resume; unknown calls require reconciliation")
	}
	return tx.Commit(ctx)
}
