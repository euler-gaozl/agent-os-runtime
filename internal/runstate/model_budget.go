package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

type ModelUsage struct {
	RootID               string `json:"root_id"`
	Attempts             int    `json:"attempts"`
	UnknownUsageAttempts int    `json:"unknown_usage_attempts"`
	PromptTokens         int64  `json:"prompt_tokens"`
	CompletionTokens     int64  `json:"completion_tokens"`
	CachedPromptTokens   int64  `json:"cached_prompt_tokens"`
	AccountedTokens      int64  `json:"accounted_tokens"`
	MaxAttempts          int    `json:"max_attempts"`
	TokenBudget          int    `json:"token_budget"`
}

const rootRunSQL = `WITH RECURSIVE lineage AS (
 SELECT id,parent_id,snapshot FROM runs WHERE id=$1
 UNION ALL SELECT r.id,r.parent_id,r.snapshot FROM runs r JOIN lineage l ON r.id=l.parent_id
) SELECT id,snapshot FROM lineage WHERE parent_id IS NULL`
const familyAttemptsSQL = `WITH RECURSIVE family AS (
 SELECT id FROM runs WHERE id=$1 UNION ALL SELECT r.id FROM runs r JOIN family f ON r.parent_id=f.id
) SELECT a.request,a.response FROM family f JOIN calls c ON c.run_id=f.id AND c.kind='model' JOIN model_attempts a ON a.call_id=c.id`

func requestReservation(request []byte) int64 {
	var r struct {
		MaxTokens int `json:"max_tokens"`
	}
	_ = json.Unmarshal(request, &r)
	if r.MaxTokens <= 0 {
		r.MaxTokens = 4096
	}
	// UTF-8 request bytes plus output cap and framing allowance is deliberately
	// conservative. This is a token guard, not an exact money/price estimate.
	return int64(len(request) + r.MaxTokens + 512)
}
func usageFor(ctx context.Context, tx pgx.Tx, runID string) (ModelUsage, error) {
	var out ModelUsage
	var raw []byte
	if e := tx.QueryRow(ctx, rootRunSQL, runID).Scan(&out.RootID, &raw); e != nil {
		return out, e
	}
	var snap Snapshot
	if e := json.Unmarshal(raw, &snap); e != nil {
		return out, e
	}
	out.MaxAttempts = snap.Agent.MaxModelAttempts
	out.TokenBudget = snap.Agent.ModelTokenBudget
	rows, e := tx.Query(ctx, familyAttemptsSQL, out.RootID)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var request, response []byte
		if e = rows.Scan(&request, &response); e != nil {
			return out, e
		}
		out.Attempts++
		var receipt struct {
			Usage *struct {
				Prompt     int64 `json:"prompt_tokens"`
				Completion int64 `json:"completion_tokens"`
				Cached     int64 `json:"prompt_cache_hit_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(response, &receipt) == nil && receipt.Usage != nil && receipt.Usage.Prompt >= 0 && receipt.Usage.Completion >= 0 {
			u := receipt.Usage
			out.PromptTokens += u.Prompt
			out.CompletionTokens += u.Completion
			out.CachedPromptTokens += u.Cached
			out.AccountedTokens += u.Prompt + u.Completion
		} else {
			out.UnknownUsageAttempts++
			out.AccountedTokens += requestReservation(request)
		}
	}
	return out, rows.Err()
}
func reserveModelBudget(ctx context.Context, tx pgx.Tx, runID string, request []byte) error {
	var root string
	var snapshot []byte
	if e := tx.QueryRow(ctx, rootRunSQL, runID).Scan(&root, &snapshot); e != nil {
		return e
	}
	var snap Snapshot
	if e := json.Unmarshal(snapshot, &snap); e != nil {
		return e
	}
	if snap.Agent.MaxModelAttempts == 0 && snap.Agent.ModelTokenBudget == 0 {
		return nil
	} // immutable historical policy
	// Serializes reservations across replicas and all descendants. The lock is
	// committed with the attempt, before any provider HTTP request can start.
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,35))", root); e != nil {
		return e
	}
	u, e := usageFor(ctx, tx, runID)
	if e != nil {
		return e
	}
	if u.MaxAttempts > 0 && u.Attempts >= u.MaxAttempts {
		return errors.New("family model attempt budget exhausted (retries included)")
	}
	if u.TokenBudget > 0 && u.AccountedTokens+requestReservation(request) > int64(u.TokenBudget) {
		return errors.New("family model token budget exhausted (unknown attempts reserved)")
	}
	return nil
}
func (s *Store) Usage(ctx context.Context, runID string) (ModelUsage, error) {
	tx, e := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return ModelUsage{}, e
	}
	defer tx.Rollback(ctx)
	u, e := usageFor(ctx, tx, runID)
	if e != nil {
		return u, e
	}
	return u, tx.Commit(ctx)
}
