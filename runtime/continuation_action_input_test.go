package runtime

import (
	"strings"
	"testing"
)

func TestContinuationActionExactJSON(t *testing.T) {
	v := strings.Repeat("a", 64)
	good := `{"request_id":"request_1-A","expected_failure_version":"` + v + `"}`
	in, err := decodeContinuationRequest([]byte(good))
	if err != nil || in.RequestID != "request_1-A" || in.ExpectedFailureVersion != v {
		t.Fatal("valid request did not survive decoding")
	}
	for name, raw := range map[string]string{
		"duplicate":         `{"request_id":"first","request_id":"last","expected_failure_version":"` + v + `"}`,
		"escaped_duplicate": `{"request_id":"first","request_\u0069d":"last","expected_failure_version":"` + v + `"}`,
		"case_alias":        `{"Request_ID":"request","expected_failure_version":"` + v + `"}`,
		"unknown":           strings.TrimSuffix(good, "}") + `,"other":true}`,
		"missing":           `{"request_id":"request"}`,
		"null_field":        `{"request_id":null,"expected_failure_version":"` + v + `"}`,
		"number":            `{"request_id":1,"expected_failure_version":"` + v + `"}`,
		"array":             `[` + good + `]`, "null": `null`, "extra": good + good,
		"invalid_utf8":  strings.Replace(good, "request_1-A", "request\xff", 1),
		"oversize":      good + strings.Repeat(" ", 1025),
		"upper_version": strings.Replace(good, v, strings.ToUpper(v), 1),
		"oversize_id":   strings.Replace(good, "request_1-A", strings.Repeat("r", 129), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeContinuationRequest([]byte(raw)); err == nil {
				t.Fatal("ambiguous or out-of-bounds request accepted")
			}
		})
	}
}

func TestContinuationActionPeerBoundary(t *testing.T) {
	base := "/v1/runs/run/continuation"
	for _, caller := range []string{"", "worker", "toolhub", "knowledge", "futures", "runtime", "control"} {
		for _, path := range []string{base, base + "/request-context", base + "/requests/request"} {
			for _, method := range []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"} {
				want := caller == "control" && (method == "GET" || (path == base && method == "POST"))
				if Authorize(caller, method, path) != want {
					t.Fatalf("boundary mismatch: %s %s %s", caller, method, path)
				}
			}
		}
	}
	for _, path := range []string{base + "/", base + "/unknown", base + "/requests/", base + "/requests/request/extra"} {
		if Authorize("control", "GET", path) || Authorize("knowledge", "GET", path) {
			t.Fatal("invalid management subtree fell through generic run grants")
		}
	}
	// servicehttp passes decoded URL.Path, whereas ServeMux can match an
	// escaped slash within a single {id}. Peer authorization must still deny.
	for _, suffix := range []string{"", "/request-context", "/requests/request"} {
		for _, caller := range []string{"toolhub", "knowledge", "worker", "runtime"} {
			if Authorize(caller, "GET", "/v1/runs/a/b/continuation"+suffix) {
				t.Fatal("escaped run identity bypassed management authorization")
			}
		}
	}
	if !Authorize("knowledge", "GET", "/v1/runs/run") || !Authorize("control", "POST", "/v1/runs/run/resume") {
		t.Fatal("unrelated historical route boundary changed")
	}
}
