package runstate

import (
	"strings"
	"testing"
)

func TestContinuationActionIdentityBounds(t *testing.T) {
	for _, id := range []string{"a", "A_0-z", strings.Repeat("a", 128)} {
		if !ValidContinuationRequestID(id) {
			t.Fatal("valid request identity rejected")
		}
	}
	for _, id := range []string{"", "a/b", "a b", "é", "a\x00", strings.Repeat("a", 129)} {
		if ValidContinuationRequestID(id) {
			t.Fatal("invalid request identity accepted")
		}
	}
	if !ValidContinuationRunID(strings.Repeat("a", 512)) || ValidContinuationRunID(strings.Repeat("a", 513)) ||
		ValidContinuationRunID("bad\x00") || ValidContinuationRunID("bad\xff") {
		t.Fatal("run identity does not enforce byte bounds and encoding")
	}
	for _, version := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		if ValidContinuationFailureVersion(version) {
			t.Fatal("invalid failure version accepted")
		}
	}
}

func TestContinuationActionFailureABABinding(t *testing.T) {
	base := continuationFailureVersion("run", EngineDSHNative, 1, 2, 3, "call", "attempt")
	if !ValidContinuationFailureVersion(base) || base != continuationFailureVersion("run", EngineDSHNative, 1, 2, 3, "call", "attempt") {
		t.Fatal("failure version is not deterministic lowercase hash")
	}
	for name, changed := range map[string]string{
		"run":      continuationFailureVersion("other", EngineDSHNative, 1, 2, 3, "call", "attempt"),
		"engine":   continuationFailureVersion("run", EngineDSHBudget, 1, 2, 3, "call", "attempt"),
		"revision": continuationFailureVersion("run", EngineDSHNative, 2, 2, 3, "call", "attempt"),
		"fence":    continuationFailureVersion("run", EngineDSHNative, 1, 3, 3, "call", "attempt"),
		"frame":    continuationFailureVersion("run", EngineDSHNative, 1, 2, 4, "call", "attempt"),
		"call":     continuationFailureVersion("run", EngineDSHNative, 1, 2, 3, "other", "attempt"),
		"attempt":  continuationFailureVersion("run", EngineDSHNative, 1, 2, 3, "call", "other"),
	} {
		if changed == base {
			t.Fatalf("version missed changed %s", name)
		}
	}
	in := ContinuationRequest{RequestID: "first", ExpectedFailureVersion: base}
	hash := continuationRequestHash("run", in)
	in.RequestID = "second"
	if hash != continuationRequestHash("run", in) {
		t.Fatal("separate request primary key changed payload fingerprint")
	}
	if hash == continuationRequestHash("other", in) {
		t.Fatal("request payload fingerprint missed target")
	}
	in.ExpectedFailureVersion = continuationFailureVersion("run", EngineDSHNative, 1, 3, 3, "call", "other")
	if hash == continuationRequestHash("run", in) {
		t.Fatal("request payload fingerprint missed new failure")
	}
}

func TestContinuationActionStoredReceiptClosedDTO(t *testing.T) {
	valid := `{"protocol":"failed-run-continuation-receipt-v1","request_id":"r","run_id":"run","outcome":"queued","failure_version":"` + strings.Repeat("a", 64) + `","committed":true,"status_at_acceptance":"queued","accepted_at":"2026-10-15T00:00:00Z"}`
	if receipt, err := decodeContinuationReceipt([]byte(valid)); err != nil || receipt.RequestID != "r" {
		t.Fatal("valid persisted receipt rejected")
	}
	for name, raw := range map[string]string{
		"unknown_field":  strings.TrimSuffix(valid, "}") + `,"private_request":"must-not-escape"}`,
		"trailing_value": valid + `null`,
		"trailing_junk":  valid + `!`,
		"null":           `null`,
		"array":          `[` + valid + `]`,
		"oversize":       valid + strings.Repeat(" ", continuationActionReceiptBytes),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeContinuationReceipt([]byte(raw)); err == nil {
				t.Fatal("invalid stored response was allowed to replay")
			}
		})
	}
}
