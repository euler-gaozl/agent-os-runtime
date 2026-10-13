package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func TestSubmissionConfigurationRejectsUnknownEngine(t *testing.T) {
	s := NewStore(nil, nil, "")
	for _, invalid := range []string{"legacy-go", "dsh-0.2.0-rc.2", "source", " dsh-source-639ed015-state-v1", "future-engine"} {
		if err := s.ConfigureSubmissionEngine(invalid); err == nil {
			t.Fatal("unknown deployment engine accepted", invalid)
		}
	}
	if s.defaultRunEngine() != EngineDSHEvidence {
		t.Fatal("invalid configuration changed the default")
	}
}

func TestSubmissionHTTPPreservesEngineAcrossDeploymentAndChildren(t *testing.T) {
	s, _ := testStore(t)
	snap := basicSnapshot("https://fixture.invalid")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/resources/agent/test/1" && r.URL.Path != "/v1/published/futures/test" {
			t.Error("unexpected catalog request", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		jsonResponse(w, 200, Resource{Kind: "agent", ID: "test", Revision: 1, Spec: JSON(snap.Agent)})
	}))
	defer peer.Close()
	s.Remote = &serviceclient.Client{RegistryURL: peer.URL, Token: testToken}
	h := (&Server{Store: s, RegistryURL: peer.URL}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{
		"control": servicehttp.TokenHash(testToken + "control"), "futures": servicehttp.TokenHash(testToken + "futures"),
	}})
	ctx := context.Background()
	for _, app := range []bool{false, true} {
		path, caller := "/v1/runs", "control"
		if app {
			path, caller = "/v1/apps/futures/runs", "futures"
		}
		post := func(key, payload string) *httptest.ResponseRecorder {
			req := httptest.NewRequest("POST", path, strings.NewReader(payload))
			req.Header.Set("Authorization", "Bearer "+testToken+caller)
			req.Header.Set("Idempotency-Key", key)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			return w
		}
		body := `{"agent":{"id":"test","revision":1},"task":"explicit submission fixture"}`
		for _, firstEngine := range []string{EngineDSHEvidence, EngineDSHSource} {
			if err := s.ConfigureSubmissionEngine(firstEngine); err != nil {
				t.Fatal(err)
			}
			key := ID()
			w := post(key, body)
			if w.Code != 201 {
				t.Fatal(path, w.Code, w.Body.String())
			}
			var accepted struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &accepted); err != nil || accepted.ID == "" {
				t.Fatal("missing acceptance", err)
			}
			r, err := s.Run(ctx, accepted.ID)
			if err != nil || r.Engine != firstEngine {
				t.Fatal("wrong accepted engine", r.Engine, err)
			}
			next := EngineDSHSource
			if firstEngine == next {
				next = EngineDSHEvidence
			}
			if err = s.ConfigureSubmissionEngine(next); err != nil {
				t.Fatal(err)
			}
			retry := post(key, body)
			if retry.Code != 201 || retry.Body.String() != w.Body.String() {
				t.Fatal("deployment change broke idempotency", retry.Code, retry.Body.String())
			}
			r, err = s.Run(ctx, accepted.ID)
			if err != nil || r.Engine != firstEngine {
				t.Fatal("retry rewrote original engine", err)
			}
			if got := post(key, strings.Replace(body, "explicit submission fixture", "changed input", 1)); got.Code == 201 {
				t.Fatal("changed input accepted")
			}
			if got := post(ID(), strings.TrimSuffix(body, "}")+`,"engine":"`+next+`"}`); got.Code == 201 {
				t.Fatal("client selected execution engine")
			}
			// Child insertion is the same transactional path as spawn_agent. The
			// deployment switched, but the parent's immutable engine still wins.
			tx, err := s.DB.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			child, err := insertRun(ctx, tx, ID(), "child fixture", r.Snapshot, &r.ID, r.Constraints)
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			c, err := s.Run(ctx, child)
			if err != nil || c.Engine != firstEngine {
				t.Fatal("child changed engine", err)
			}
			var count int
			if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id=$1 AND kind='accepted'", r.ID).Scan(&count); err != nil || count != 1 {
				t.Fatal("repeated acceptance", count, err)
			}
		}
	}
}
