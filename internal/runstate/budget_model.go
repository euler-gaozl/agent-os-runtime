package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/jackc/pgx/v5"
)

// v3 only: return the exact final provider request after either a pre-send
// rejection or a committed model receipt. Runtime derives the envelope and usage
// from its own ledger under the original fence, never from caller counters.
type BudgetModelEffects interface {
	CompleteBudgetModel(context.Context, Run, string, []Message, []Tool) (json.RawMessage, error)
}

type ModelContextRejection struct {
	Format               int    `json:"format"`
	Kind                 string `json:"kind"`
	Code                 string `json:"code"`
	NotSent              bool   `json:"not_sent"`
	RequestSHA256        string `json:"request_sha256"`
	EstimatedInputTokens int    `json:"estimated_input_tokens"`
	MaxTokens            int    `json:"max_tokens"`
	ContextTokens        int    `json:"context_tokens"`
}

type ModelCompletion struct {
	Format  int            `json:"format"`
	Kind    string         `json:"kind"`
	Message Message        `json:"message"`
	Usage   *ProviderUsage `json:"usage,omitempty"`
}

// v3's request serialization is part of the versioned execution protocol. Old
// Workers keep their previous builder and replay bytes. Secrets/endpoints are
// deliberately absent from this provider body.
func BudgetModelRequest(config ModelConfig, messages []Message, tools []Tool) []byte {
	body := map[string]any{"model": config.Model, "messages": messages, "stream": false, "max_tokens": config.MaxTokens}
	if config.Thinking != "" {
		body["thinking"] = map[string]string{"type": config.Thinking}
	}
	if config.ReasoningEffort != "" {
		body["reasoning_effort"] = config.ReasoningEffort
	}
	if len(tools) > 0 {
		defs := make([]any, 0, len(tools))
		for _, t := range tools {
			defs = append(defs, map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.InputSchema}})
		}
		body["tools"] = defs
	}
	return JSON(body)
}

func budgetOutputLimit(snap Snapshot, input ModelInput) (int, error) {
	cap := snap.Agent.Model.MaxTokens
	if cap == 0 {
		cap = 4096
	}
	if input.Purpose == "compaction" && cap > 2048 {
		cap = 2048
	}
	if input.Purpose != "generation" && input.Purpose != "compaction" || input.MaxTokens != cap {
		return 0, errors.New("budget model output limit differs from the immutable purpose policy")
	}
	return cap, nil
}

// This is a bounded heuristic over the final JSON, including escaped text,
// tools, constraints and child results. It is not the provider tokenizer.
// Invalid configuration and malformed requests are errors, never overflow.
func InspectBudgetRequest(request []byte, contextTokens, maxTokens int) (*ModelContextRejection, error) {
	if contextTokens == 0 {
		contextTokens = 32000
	}
	if contextTokens < 8192 || contextTokens > 1000000 || maxTokens < 1 || maxTokens > 32768 || maxTokens+512 >= contextTokens {
		return nil, errors.New("invalid source model output/context configuration")
	}
	if len(request) > 16<<20 {
		return nil, errors.New("final provider request exceeds transport limit")
	}
	var body struct {
		MaxTokens int       `json:"max_tokens"`
		Messages  []Message `json:"messages"`
	}
	if json.Unmarshal(request, &body) != nil || body.MaxTokens != maxTokens || len(body.Messages) == 0 {
		return nil, errors.New("invalid final provider request")
	}
	characters := 0
	for _, r := range string(request) {
		characters += utf16.RuneLen(r)
	}
	estimated := (characters+3)/4 + 512
	if estimated+maxTokens <= contextTokens {
		return nil, nil
	}
	return &ModelContextRejection{1, "context_rejected", "CONTEXT_WINDOW_EXCEEDED", true, Hash(request), estimated, maxTokens, contextTokens}, nil
}

func budgetExchangeSeq(r Run, id string) (int, error) {
	prefix := r.ID + "/dsh/"
	if !strings.HasPrefix(id, prefix) {
		return 0, errors.New("budget model operation has no source exchange")
	}
	seq, err := strconv.Atoi(strings.TrimPrefix(id, prefix))
	if err != nil || seq < 1 || id != fmt.Sprintf("%s%d", prefix, seq) {
		return 0, errors.New("invalid budget model operation")
	}
	return seq, nil
}

// Both BeginModel and the response writer hold the same run row lock. Therefore
// an attempt and a not-sent rejection cannot be committed for one operation,
// even when a late callback races a retry from the same owner/fence.
func checkBudgetModelIntent(ctx context.Context, tx pgx.Tx, r Run, id string, request []byte, allowRejection bool) error {
	seq, err := budgetExchangeSeq(r, id)
	if err != nil {
		return err
	}
	var response, frozen, snapshot []byte
	var method, manifest string
	if err = tx.QueryRow(ctx, `SELECT e.method,e.request,e.response,a.manifest_hash,r.snapshot FROM harness_exchanges e
 JOIN harness_archives a USING(run_id,seq) JOIN runs r ON r.id=e.run_id WHERE e.run_id=$1 AND e.seq=$2`, r.ID, seq).Scan(&method, &frozen, &response, &manifest, &snapshot); err != nil {
		return err
	}
	if method != "model" || manifest == "" {
		return errors.New("budget model intent or archive not confirmed")
	}
	if response != nil && !allowRejection {
		var envelope struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(response, &envelope) != nil || envelope.Kind != "completion" {
			return errors.New("model operation already rejected before transmission")
		}
	}
	var input ModelInput
	if json.Unmarshal(frozen, &input) != nil {
		return errors.New("invalid frozen model input")
	}
	var snap Snapshot
	if json.Unmarshal(snapshot, &snap) != nil {
		return errors.New("invalid immutable model snapshot")
	}
	cap, err := budgetOutputLimit(snap, input)
	if err != nil {
		return err
	}
	snap.Agent.Model.MaxTokens = cap
	if snap.Agent.ContextTokens != r.Snapshot.Agent.ContextTokens {
		return errors.New("model context window differs from immutable snapshot")
	}
	// Bind every provider-visible field, including Runtime's late messages,
	// model, tools, thinking policy and output limit. No caller-added field is
	// accepted. JSON object key ordering may differ after PostgreSQL retention.
	if !json.Valid(request) || canonicalHash(request) != canonicalHash(BudgetModelRequest(snap.Agent.Model, input.Messages, input.Tools)) {
		return errors.New("final model request differs from the frozen intent and snapshot")
	}
	return nil
}

func freezeBudgetModel(ctx context.Context, tx pgx.Tx, r Run, id string, request []byte, maxTokens int) (any, error) {
	if err := checkBudgetModelIntent(ctx, tx, r, id, request, true); err != nil {
		return nil, err
	}
	rejected, err := InspectBudgetRequest(request, r.Snapshot.Agent.ContextTokens, maxTokens)
	if err != nil {
		return nil, err
	}
	var status, callRun, kind string
	var retainedRequest, raw []byte
	err = tx.QueryRow(ctx, "SELECT run_id,kind,status,request,response FROM calls WHERE id=$1", id).Scan(&callRun, &kind, &status, &retainedRequest, &raw)
	if rejected != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			if err != nil {
				return nil, err
			}
			return nil, errors.New("cannot reject a model operation with a registered attempt")
		}
		return rejected, nil
	}
	if err != nil {
		return nil, err
	}
	if callRun != r.ID || kind != "model" {
		return nil, errors.New("model operation collides with another receipt")
	}
	if status != "completed" || canonicalHash(retainedRequest) != canonicalHash(request) {
		return nil, errors.New("model result lacks a matching committed receipt")
	}
	var completion struct {
		Choices []struct {
			Message Message `json:"message"`
			Finish  string  `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(raw, &completion) != nil || len(completion.Choices) != 1 {
		return nil, errors.New("invalid committed model completion")
	}
	choice := completion.Choices[0]
	if choice.Message.Role != "assistant" || choice.Finish != "stop" && choice.Finish != "tool_calls" {
		return nil, errors.New("committed model output incomplete or truncated")
	}
	usage := providerUsage(raw)
	// Node only accepts exact integer counters. The int64 family ledger still
	// counts large valid values and fails closed; never round them for DSH.
	if usage != nil && usage.PromptTokens+usage.CompletionTokens > 9007199254740991 {
		usage = nil
	}
	return &ModelCompletion{Format: 1, Kind: "completion", Message: choice.Message, Usage: usage}, nil
}
