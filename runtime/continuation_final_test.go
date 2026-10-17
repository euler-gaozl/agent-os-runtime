package runtime

import (
	"strings"
	"testing"
)

// Real Owner HTTP/PG tests. Corrupted snapshot cases are explicit negative
// fixtures; accepted policies always enter through normal Submit and Claim.
func TestFinalContinuationPolicyAndRecheck(t *testing.T) {
	for _, mode := range []string{"same-policy", "child-policy-only", "cancel", "constraints", "attempt-cap", "not-503", "missing-policy", "invalid-path", "unbound-tool", "duplicate-tool"} {
		t.Run(mode, func(t *testing.T) {
			attempts, status := 3, 503
			if mode == "attempt-cap" {
				attempts = 1
			}
			if mode == "not-503" {
				status = 429
			}
			f := continuationSetupVersion(t, EngineDSHFinal, 2, attempts, func(s *Snapshot) {
				if mode == "child-policy-only" {
					child := *s
					s.Agent.FinalOutput = nil
					s.Children = map[string]Snapshot{"child": child}
				}
			})
			f.failModel(t, 1, status)
			version := strings.Repeat("0", 64)
			if mode != "attempt-cap" && mode != "not-503" {
				version = actionVersion(t, f)
			}
			ctx, s := f.n.fixture.ctx, f.n.fixture.store
			assessment, reason := "same_run_candidate", "retained_transient_model_failure"
			switch mode {
			case "cancel":
				actionRequest(t, f, "POST", "/v1/runs/"+f.n.run.ID+"/cancel", nil, 202)
				assessment, reason = "blocked", "cancelled"
			case "constraints":
				actionRequest(t, f, "POST", "/v1/runs/"+f.n.run.ID+"/constraints", JSON(map[string]any{"values": []string{"a newly required reporting period"}}), 200)
				assessment, reason = "manual_review", "constraints_changed"
			case "attempt-cap":
				assessment, reason = "blocked", "attempt_budget_exhausted"
			case "not-503":
				assessment, reason = "manual_review", "failure_not_assessed"
			case "missing-policy", "invalid-path", "unbound-tool", "duplicate-tool":
				snap := f.n.run.Snapshot
				switch mode {
				case "missing-policy":
					snap.Agent.FinalOutput = nil
				case "invalid-path":
					snap.Agent.FinalOutput.ReadyPath = []string{"ready[0]"}
				case "unbound-tool":
					snap.Agent.FinalOutput.Tool = "mcp_unknown_check"
				case "duplicate-tool":
					snap.Connectors[0].Tools = append(snap.Connectors[0].Tools, snap.Connectors[0].Tools[0]) // duplicate binding is not a valid frozen tool set
				}
				_, err := f.n.fixture.db.Exec(ctx, "UPDATE runs SET snapshot=$2 WHERE id=$1", f.n.run.ID, JSON(snap))
				nativeNotesNeed(t, err)
				assessment, reason = "manual_review", "invalid_frozen_configuration"
			}
			before := actionDigest(t, f)
			f.preview(t, assessment, reason)
			if actionDigest(t, f) != before {
				t.Fatal("preview changed failed task or policy")
			}
			frozen, invariant := actionFrozenEvidence(t, f), actionRunInvariant(t, f)
			want, outcome, receiptReason := 409, "rejected", reason
			if mode == "same-policy" || mode == "child-policy-only" {
				want, outcome, receiptReason = 202, "queued", ""
			}
			body := actionPost(t, f, "final-"+mode, version, want)
			actionReceipt(t, body, "final-"+mode, f.n.run.ID, version, outcome, receiptReason)
			if actionFrozenEvidence(t, f) != frozen || actionRunInvariant(t, f) != invariant {
				t.Fatal("decision rewrote task, frozen final policy or original evidence")
			}
			u, err := s.Usage(ctx, f.n.run.ID)
			nativeNotesNeed(t, err)
			if u.Attempts != 1 || u.UnknownUsageAttempts != 1 {
				t.Fatal("management action spent an attempt or forgave unknown usage", u)
			}
			queued := 0
			if mode == "same-policy" || mode == "child-policy-only" {
				queued = 1
				snapshot := canonicalHash(JSON(f.n.run.Snapshot))
				actionClaim(t, f)
				if f.n.run.Engine != EngineDSHFinal || canonicalHash(JSON(f.n.run.Snapshot)) != snapshot {
					t.Fatal("claim changed frozen engine or tool")
				}
				f.successModel(t, 1)
			}
			actionCounts(t, f, 1, queued)
		})
	}
}
