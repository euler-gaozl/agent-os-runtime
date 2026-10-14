package runstate

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestBudgetPreflightFinalSerializationAndConfiguration(t *testing.T) {
	request := func(text string, max int) []byte {
		return JSON(map[string]any{"messages": []Message{{Role: "user", Content: text}}, "max_tokens": max})
	}
	for _, text := range []string{"small", strings.Repeat("中😀\n\"<>", 8000)} {
		raw := request(text, 6144)
		rejected, err := InspectBudgetRequest(raw, 32000, 6144)
		if err != nil {
			t.Fatal(err)
		}
		if len(text) > 100 && (rejected == nil || rejected.RequestSHA256 != Hash(raw) || rejected.EstimatedInputTokens+rejected.MaxTokens <= 32000) {
			t.Fatal("final escaped body not rejected")
		}
		if len(text) < 100 && rejected != nil {
			t.Fatal("small request rejected")
		}
	}
	for _, in := range []struct {
		window, output int
		raw            []byte
	}{
		{8192, 7800, request("x", 7800)}, {32000, 0, request("x", 0)}, {32000, 32769, request("x", 32769)},
		{32000, 6144, request("x", 4096)}, {32000, 6144, []byte(`{}`)},
	} {
		if rejection, err := InspectBudgetRequest(in.raw, in.window, in.output); err == nil || rejection != nil {
			t.Fatal("invalid input disguised as overflow")
		}
	}
}

func TestProviderUsageRequiresPresentIntegralConsistentTotals(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"usage":null}`, `{"usage":{}}`,
		`{"usage":{"prompt_tokens":null,"completion_tokens":1}}`,
		`{"usage":{"prompt_tokens":20}}`, `{"usage":{"prompt_tokens":1.5,"completion_tokens":1}}`,
		`{"usage":{"prompt_tokens":-1,"completion_tokens":1}}`,
		`{"usage":{"prompt_tokens":2,"completion_tokens":1,"prompt_cache_hit_tokens":3}}`,
		`{"usage":{"prompt_tokens":2,"completion_tokens":1,"prompt_cache_hit_tokens":null}}`,
		`{"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":4}}`,
		`{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":1}}`,
	} {
		if providerUsage([]byte(raw)) != nil {
			t.Fatal("invalid usage accepted", raw)
		}
	}
	for _, u := range []ProviderUsage{{0, 0, 0}, {20, 10, 0}, {20, 10, 15}} {
		raw, _ := json.Marshal(map[string]any{"usage": map[string]int64{"prompt_tokens": u.PromptTokens, "completion_tokens": u.CompletionTokens, "prompt_cache_hit_tokens": u.CachedPromptTokens, "total_tokens": u.PromptTokens + u.CompletionTokens}})
		if got := providerUsage(raw); got == nil || *got != u {
			t.Fatal("valid usage changed", got)
		}
	}
	if u := providerUsage([]byte(`{"usage":{"prompt_tokens":20,"completion_tokens":10}}`)); u == nil || u.CachedPromptTokens != 0 {
		t.Fatal("optional cache field became required")
	}
	if saturatingTokens(math.MaxInt64-10, 20) != math.MaxInt64 {
		t.Fatal("aggregate wrapped")
	}
	if requestReservation([]byte(`{"max_tokens":9223372036854775807}`)) != math.MaxInt64 {
		t.Fatal("reservation wrapped")
	}
}
