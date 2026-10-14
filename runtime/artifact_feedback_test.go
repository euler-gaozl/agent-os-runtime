package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
)

func TestArtifactReadRejectionIsReplayableAndCanBeCorrected(t *testing.T) {
	for _, code := range []int{404, 416} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			s := isolatedDomainDatabase(t, "runtime")
			snap := basicSnapshot("https://fixture.invalid")
			snap.Agent.Tools = append(snap.Agent.Tools, "artifact_read")
			submit(t, s, snap)
			r := claim(t, s, "artifact-feedback")
			reject, requests := true, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests++
				if req.URL.Path != "/v1/artifacts/page" || req.Header.Get("Authorization") != "Bearer "+testToken {
					t.Error("artifact request crossed transport boundary")
				}
				if !reject {
					_, _ = w.Write([]byte(`{"content":"retained fixture","has_more":false}`))
				} else {
					name := "artifact_unavailable"
					if code == 416 {
						name = "artifact_window_invalid"
					}
					jsonResponse(w, 200, map[string]any{"isError": true, "code": name, "message": "Correct the source identity or read offset zero."})
				}
			}))
			defer server.Close()
			s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, server.URL
			inv := noteInv(r, "artifact_read", "wrong-source-hash", map[string]any{"hash": strings.Repeat("a", 64), "offset": 30})
			body, err := s.LocalTool(context.Background(), inv)
			var feedback struct {
				IsError       bool `json:"isError"`
				Code, Message string
			}
			if err != nil || json.Unmarshal(body, &feedback) != nil || !feedback.IsError || feedback.Code == "" || feedback.Message == "" {
				t.Fatal("missing actionable read feedback", string(body), err)
			}
			reject = false
			replay, err := s.LocalTool(context.Background(), inv)
			if err != nil || !jsonEqual(replay, body) || requests != 1 {
				t.Fatal("committed rejection was retried or changed", requests, err)
			}
			inv.OperationID += "-corrected"
			inv.Arguments = JSON(map[string]any{"hash": strings.Repeat("b", 64), "offset": 0})
			corrected, err := s.LocalTool(context.Background(), inv)
			if err != nil || !strings.Contains(string(corrected), "retained fixture") || requests != 2 {
				t.Fatal("new logical read could not proceed", string(corrected), err)
			}
			inv.Fence--
			if _, err = s.LocalTool(context.Background(), inv); !errors.Is(err, ErrFence) || requests != 2 {
				t.Fatal("feedback bypassed the fence", err)
			}
		})
	}
}

func TestArtifactReadDoesNotConvertInfrastructureOrAuthorityFailures(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	snap := basicSnapshot("https://fixture.invalid")
	snap.Agent.Tools = append(snap.Agent.Tools, "artifact_read")
	submit(t, s, snap)
	r := claim(t, s, "artifact-hard-failure")
	status := 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	defer server.Close()
	s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, server.URL
	for _, code := range []int{400, 401, 403, 404, 409, 412, 416, 429, 500, 503} {
		status = code
		inv := noteInv(r, "artifact_read", fmt.Sprint(code), map[string]any{"hash": strings.Repeat("a", 64)})
		if _, err := s.LocalTool(context.Background(), inv); err == nil {
			t.Fatalf("status %d was mistaken for a correctable read", code)
		}
	}
	var count int
	if err := s.DB.QueryRow(context.Background(), "SELECT count(*) FROM calls WHERE run_id=$1", r.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("hard failure produced a successful read receipt", count, err)
	}
}
