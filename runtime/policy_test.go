package runtime

import "testing"

func TestRuntimePeerBoundaries(t *testing.T) {
	for _, test := range []struct {
		who, method, path string
		allow             bool
	}{
		{"worker", "POST", "/invoke", false}, {"control", "POST", "/invoke", false}, {"toolhub", "POST", "/invoke", true},
		{"knowledge", "GET", "/v1/runs/fixture", true}, {"knowledge", "POST", "/v1/runs", false},
		{"sandbox", "POST", "/v1/sandbox/authorize", true}, {"sandbox", "POST", "/v1/runs", false},
		{"telemetry", "POST", "/v1/telemetry/claim", true}, {"telemetry", "GET", "/v1/runs/fixture", false},
		{"futures", "POST", "/v1/apps/futures/runs", true}, {"futures", "GET", "/v1/apps/other/runs", false},
		{"futures", "POST", "/v1/runs", false}, {"futures", "POST", "/v1/notes/revise", false},
		{"control", "POST", "/v1/notes/revise", true}, {"worker", "GET", "/v1/notes", false},
		{"worker", "POST", "/v1/instances", true}, {"", "POST", "/v1/instances", false},
		{"runtime", "GET", "/readyz", true}, {"futures", "GET", "/readyz", false},
	} {
		if got := Authorize(test.who, test.method, test.path); got != test.allow {
			t.Errorf("%s %s %s authorization=%v", test.who, test.method, test.path, got)
		}
	}
}
