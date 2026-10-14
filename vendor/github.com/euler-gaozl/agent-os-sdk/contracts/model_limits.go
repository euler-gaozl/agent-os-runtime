package contracts

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ModelLimits is an immutable provider capability observation, not a task token
// allowance. The resource owner resolves it before publishing an Agent revision.
// A nil pointer preserves legacy configuration and replay semantics. Empty
// observations are not executable; readers must never substitute defaults.
type ModelLimits struct {
	Model           string `json:"model"`
	BaseURL         string `json:"base_url"`
	Protocol        string `json:"protocol"`
	ContextWindow   int    `json:"context_window"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	Source          string `json:"source"`
	ObservedAt      string `json:"observed_at"`
	MetadataSHA256  string `json:"metadata_sha256"`
}

var metadataDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Validate checks the wire contract only. Provider discovery, host authorization
// and credential resolution belong to the resource owner, never this library.
func (m ModelLimits) Validate(contextWindow int, model ModelConfig) error {
	// The fixed Node bridge must represent these integers without rounding.
	const maxExactJSInteger int64 = 9007199254740991
	if m.Protocol != "openai-chat-completions" || m.ContextWindow <= 0 || m.MaxOutputTokens <= 0 ||
		int64(m.ContextWindow) > maxExactJSInteger/2 || m.MaxOutputTokens > m.ContextWindow ||
		m.ContextWindow != contextWindow || m.MaxOutputTokens != model.MaxTokens ||
		m.Model == "" || m.Model != model.Model || m.BaseURL != strings.TrimRight(model.BaseURL, "/") {
		return errors.New("model limits must match resolved provider capabilities")
	}
	u, err := url.Parse(m.Source)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("model capability source must be a credential-free HTTPS URL")
	}
	base, err := url.Parse(m.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		base.Host != u.Host || strings.TrimRight(base.Path, "/")+"/models" != u.Path {
		return errors.New("model capability source must belong to the configured provider")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.ObservedAt); err != nil || !metadataDigest.MatchString(m.MetadataSHA256) {
		return errors.New("model capability observation requires time and metadata digest")
	}
	return nil
}
