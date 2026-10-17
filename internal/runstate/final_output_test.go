package runstate

import (
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"testing"
)

func TestFinalPolicyBindsToolAndEngine(t *testing.T) {
	p := &contracts.FinalOutputPolicy{Protocol: contracts.FinalOutputProtocol, Tool: "mcp_fixture_check", InputField: "draft", ReadyPath: []string{"ready"}}
	snap := Snapshot{Agent: Agent{FinalOutput: p}, Connectors: []Connector{{Name: "fixture", Tools: []Tool{{Name: "check", InputSchema: []byte(`{"type":"object"}`)}}}}}
	if e := validateFinalSnapshot(snap, EngineDSHFinal); e != nil {
		t.Fatal(e)
	}
	for _, engine := range []string{EngineDSHNative, EngineDSHBudget, EngineDSHSource, EngineDSHEvidence} {
		if validateFinalSnapshot(snap, engine) == nil {
			t.Fatal("old engine accepted final policy", engine)
		}
	}
	missing := snap
	missing.Connectors = nil
	if validateFinalSnapshot(missing, EngineDSHFinal) == nil {
		t.Fatal("unbound candidate accepted")
	}
	tree := Snapshot{Children: map[string]Snapshot{"child": snap}}
	if !hasFinalOutput(tree) || validateFinalSnapshot(tree, EngineDSHNative) == nil {
		t.Fatal("child policy bypassed engine check")
	}
}

func TestFinalCandidateRequiresStringAndExactBoolean(t *testing.T) {
	p := &contracts.FinalOutputPolicy{InputField: "draft", ReadyPath: []string{"structuredContent", "ready"}}
	original := "中🙂\r\n{\"x\": 1}\n  "
	args := JSON(map[string]string{"draft": original})
	for _, response := range []string{`{}`, `null`, `{"structuredContent":{"ready":null}}`, `{"structuredContent":{"ready":"true"}}`, `{"structuredContent":{"ready":1}}`, `{"structuredContent":[true]}`, `{"isError":true,"structuredContent":{"ready":true}}`} {
		text, ready, valid := finalCandidateText(p, args, []byte(response))
		if !valid || ready || text != original {
			t.Fatal("unsafe readiness or changed original", response)
		}
	}
	text, ready, valid := finalCandidateText(p, args, []byte(`{"structuredContent":{"ready": true}}`))
	if !valid || !ready || text != original {
		t.Fatal("valid candidate changed")
	}
	for _, args := range []string{`{}`, `{"draft":null}`, `{"draft":123}`, `{"draft":{}}`} {
		if _, _, valid := finalCandidateText(p, []byte(args), []byte(`{}`)); valid {
			t.Fatal("non-string candidate", args)
		}
	}
}
