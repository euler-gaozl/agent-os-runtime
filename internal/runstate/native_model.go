package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

const NativeRequestBytes = 32 << 20
const NativeResponseBytes = 32 << 20

type NativeModelEffects interface {
	CompleteNativeModel(context.Context, Run, string, []Message, []Tool) (json.RawMessage, error)
}

// The HTTP status and complete raw provider JSON travel together in the durable
// attempt. A network/partial-body failure never becomes a context rejection.
type NativeProviderReceipt struct {
	Protocol   string          `json:"protocol"`
	HTTPStatus int             `json:"http_status"`
	Body       json.RawMessage `json:"body"`
}

func NativeReceipt(status int, body []byte) ([]byte, error) {
	if status < 100 || status > 599 || len(body) > NativeResponseBytes || !json.Valid(body) {
		return nil, errors.New("invalid native provider receipt")
	}
	return JSON(NativeProviderReceipt{"native-provider-v1", status, body}), nil
}

func ReadNativeReceipt(raw []byte) (NativeProviderReceipt, error) {
	var receipt NativeProviderReceipt
	if json.Unmarshal(raw, &receipt) != nil || receipt.Protocol != "native-provider-v1" || receipt.HTTPStatus < 100 || receipt.HTTPStatus > 599 || len(receipt.Body) > NativeResponseBytes || !json.Valid(receipt.Body) {
		return receipt, errors.New("invalid persisted native provider receipt")
	}
	return receipt, nil
}

var explicitContextMessage = regexp.MustCompile(`(?i)^this model(?:'s)? maximum context length is [0-9]+ tokens\b`)

// Only known structured codes or the specific provider context-limit sentence
// qualify. Generic HTTP 400, auth, rate limit, size limits and I/O are not this.
func NativeContextRejected(status int, body []byte) bool {
	if status != 400 || len(body) > NativeResponseBytes {
		return false
	}
	var reply struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &reply) != nil {
		return false
	}
	code := strings.ToLower(reply.Error.Code)
	return code == "context_length_exceeded" || code == "context_window_exceeded" || explicitContextMessage.MatchString(reply.Error.Message)
}

func ValidateNativeSnapshot(snap Snapshot) error {
	a := snap.Agent
	if a.Model.Limits == nil || a.Model.Limits.Validate(a.ContextTokens, a.Model) != nil || a.ModelTokenBudget != 0 || a.MaxModelAttempts <= 0 {
		return errors.New("native engine requires resolved model capabilities, no family token cap and a finite attempt policy")
	}
	for _, child := range snap.Children {
		if err := ValidateNativeSnapshot(child); err != nil {
			return err
		}
	}
	return nil
}

func hasNativeCapabilities(snap Snapshot) bool {
	if snap.Agent.Model.Limits != nil {
		return true
	}
	for _, child := range snap.Children {
		if hasNativeCapabilities(child) {
			return true
		}
	}
	return false
}

func NativeModelRequest(config ModelConfig, messages []Message, tools []Tool) []byte {
	return BudgetModelRequest(config, messages, tools)
}

func InspectNativeRequest(request []byte, snap Snapshot) error {
	if err := ValidateNativeSnapshot(snap); err != nil {
		return err
	}
	var body struct {
		Model     string    `json:"model"`
		MaxTokens int       `json:"max_tokens"`
		Messages  []Message `json:"messages"`
	}
	if len(request) > NativeRequestBytes || json.Unmarshal(request, &body) != nil || len(body.Messages) == 0 || body.Model != snap.Agent.Model.Model || body.MaxTokens != snap.Agent.Model.MaxTokens {
		return errors.New("invalid or oversized native model transport request")
	}
	// No chars/4 pre-send rejection: it is not the supplier's tokenizer. DSH's
	// pressure estimate can compact history, while the supplier decides actual fit.
	return nil
}

func checkNativeModelIntent(ctx context.Context, tx pgx.Tx, r Run, id string, request []byte, allowFrozen bool) error {
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
		return errors.New("native model intent or archive not confirmed")
	}
	var snap Snapshot
	var input ModelInput
	if json.Unmarshal(snapshot, &snap) != nil || json.Unmarshal(frozen, &input) != nil || ValidateNativeSnapshot(snap) != nil {
		return errors.New("invalid immutable native model intent")
	}
	if input.Purpose != "generation" && input.Purpose != "compaction" || input.MaxTokens != snap.Agent.Model.MaxTokens ||
		snap.Agent.ContextTokens != r.Snapshot.Agent.ContextTokens || canonicalHash(JSON(snap.Agent.Model)) != canonicalHash(JSON(r.Snapshot.Agent.Model)) {
		return errors.New("native request configuration differs from immutable snapshot")
	}
	if response != nil && !allowFrozen {
		var outcome struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(response, &outcome) != nil || outcome.Kind != "completion" {
			return errors.New("native model outcome is already frozen")
		}
	}
	if err := InspectNativeRequest(request, snap); err != nil {
		return err
	}
	if canonicalHash(request) != canonicalHash(NativeModelRequest(snap.Agent.Model, input.Messages, input.Tools)) {
		return errors.New("native final request differs from frozen input and snapshot")
	}
	return nil
}

type NativeContextRejection struct {
	Format         int    `json:"format"`
	Kind           string `json:"kind"`
	Code           string `json:"code"`
	NotSent        bool   `json:"not_sent"`
	HTTPStatus     int    `json:"http_status"`
	RequestSHA256  string `json:"request_sha256"`
	ResponseSHA256 string `json:"response_sha256"`
	MaxTokens      int    `json:"max_tokens"`
	ContextTokens  int    `json:"context_tokens"`
}

func freezeNativeModel(ctx context.Context, tx pgx.Tx, r Run, id string, request []byte) (any, error) {
	if err := checkNativeModelIntent(ctx, tx, r, id, request, true); err != nil {
		return nil, err
	}
	var status, owner, kind string
	var retained, raw []byte
	if err := tx.QueryRow(ctx, "SELECT run_id,kind,status,request,response FROM calls WHERE id=$1", id).Scan(&owner, &kind, &status, &retained, &raw); err != nil {
		return nil, err
	}
	if owner != r.ID || kind != "model" || status != "completed" || canonicalHash(retained) != canonicalHash(request) {
		return nil, errors.New("native outcome lacks a matching completed attempt")
	}
	receipt, err := ReadNativeReceipt(raw)
	if err != nil {
		return nil, err
	}
	if NativeContextRejected(receipt.HTTPStatus, receipt.Body) {
		return &NativeContextRejection{2, "provider_context_rejected", "CONTEXT_WINDOW_EXCEEDED", false, receipt.HTTPStatus,
			Hash(retained), Hash(receipt.Body), r.Snapshot.Agent.Model.MaxTokens, r.Snapshot.Agent.ContextTokens}, nil
	}
	if receipt.HTTPStatus < 200 || receipt.HTTPStatus >= 300 {
		return nil, errors.New("native provider response is not a successful completion")
	}
	var completion struct {
		Choices []struct {
			Message Message `json:"message"`
			Finish  string  `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal(receipt.Body, &completion) != nil || len(completion.Choices) != 1 {
		return nil, errors.New("invalid committed native completion")
	}
	choice := completion.Choices[0]
	if choice.Message.Role != "assistant" || choice.Finish != "stop" && choice.Finish != "tool_calls" {
		return nil, errors.New("committed native output incomplete or truncated")
	}
	usage := providerUsage(receipt.Body)
	if usage != nil && usage.PromptTokens+usage.CompletionTokens > 9007199254740991 {
		usage = nil
	}
	return &ModelCompletion{Format: 2, Kind: "completion", Message: choice.Message, Usage: usage}, nil
}
