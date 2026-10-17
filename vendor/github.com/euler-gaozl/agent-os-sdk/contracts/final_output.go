package contracts

import (
	"errors"
	"regexp"
)

// FinalOutputPolicy is part of the immutable Agent snapshot, never supplied by
// a model call. It names one tool input string and an explicit boolean readiness
// field in that tool's committed response. Only JSON boolean true is ready;
// absent, null and string values are not. The input is the original persisted
// string, without trimming or reserialization. Readiness is not factual correctness.
// Engine owners must also verify the bound tool and reject unsupported engines.
type FinalOutputPolicy struct {
	Protocol   string   `json:"protocol"`
	Tool       string   `json:"tool"`
	InputField string   `json:"input_field"`
	ReadyPath  []string `json:"ready_path"`
}

const FinalOutputProtocol = "receipt-field-v1"
const FinalOutputToolName = "final_output_commit"

var finalOutputName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`)

func (p FinalOutputPolicy) Validate() error {
	if p.Protocol != FinalOutputProtocol || !finalOutputName.MatchString(p.Tool) ||
		p.Tool == FinalOutputToolName || p.Tool == "working_state_update" ||
		!finalOutputName.MatchString(p.InputField) || len(p.ReadyPath) < 1 || len(p.ReadyPath) > 8 {
		return errors.New("invalid receipt final output policy")
	}
	for _, key := range p.ReadyPath {
		if !finalOutputName.MatchString(key) {
			return errors.New("final output readiness path must contain bounded object keys")
		}
	}
	return nil
}
