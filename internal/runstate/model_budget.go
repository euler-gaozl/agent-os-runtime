package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"math"
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
	return saturatingTokens(int64(len(request)), int64(r.MaxTokens), 512)
}

// Saturation fails closed under malformed provider counters; accounting must
// never wrap negative and open a budget after a very large receipt.
func saturatingTokens(values ...int64) int64 {
	var total int64
	for _, n := range values {
		if n < 0 || n > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += n
	}
	return total
}

type ProviderUsage struct {
	PromptTokens       int64 `json:"prompt_tokens"`
	CompletionTokens   int64 `json:"completion_tokens"`
	CachedPromptTokens int64 `json:"cached_prompt_tokens"`
}

// Parse the immutable provider receipt, not a transport's default-valued struct.
// A missing cache field means no reported cache hits; missing required totals,
// null/fractional/negative/inconsistent counts mean unknown usage.
func providerUsage(response []byte) *ProviderUsage {
	var receipt struct {
		Usage map[string]json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(response, &receipt) != nil || receipt.Usage == nil {
		return nil
	}
	read := func(name string, required bool) (int64, bool) {
		raw, ok := receipt.Usage[name]
		if !ok {
			return 0, !required
		}
		var value *int64
		if json.Unmarshal(raw, &value) != nil || value == nil || *value < 0 {
			return 0, false
		}
		return *value, true
	}
	prompt, p := read("prompt_tokens", true)
	completion, c := read("completion_tokens", true)
	cached, h := read("prompt_cache_hit_tokens", false)
	if !p || !c || !h || cached > prompt || completion > math.MaxInt64-prompt {
		return nil
	}
	if _, exists := receipt.Usage["total_tokens"]; exists {
		total, ok := read("total_tokens", true)
		if !ok || total != prompt+completion {
			return nil
		}
	}
	return &ProviderUsage{prompt, completion, cached}
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
		if snap.Agent.Model.Limits != nil {
			if receipt, err := ReadNativeReceipt(response); err == nil {
				response = receipt.Body
			} else {
				response = nil
			}
		}
		if u := providerUsage(response); u != nil {
			out.PromptTokens = saturatingTokens(out.PromptTokens, u.PromptTokens)
			out.CompletionTokens = saturatingTokens(out.CompletionTokens, u.CompletionTokens)
			out.CachedPromptTokens = saturatingTokens(out.CachedPromptTokens, u.CachedPromptTokens)
			out.AccountedTokens = saturatingTokens(out.AccountedTokens, u.PromptTokens, u.CompletionTokens)
		} else {
			out.UnknownUsageAttempts++
			out.AccountedTokens = saturatingTokens(out.AccountedTokens, requestReservation(request))
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
	if u.TokenBudget > 0 && saturatingTokens(u.AccountedTokens, requestReservation(request)) > int64(u.TokenBudget) {
		return fmt.Errorf("family model token budget exhausted (accounted=%d, next_reservation=%d, limit=%d, unknown_attempts=%d)", u.AccountedTokens, requestReservation(request), u.TokenBudget, u.UnknownUsageAttempts)
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
