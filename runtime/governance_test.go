package runtime

import (
	"context"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRuntimeRechecksWithdrawnAssetsAndRejectsDirectWorker(t *testing.T) {
	s, _ := testStore(t)
	var enabled atomic.Bool
	enabled.Store(true)
	var checks atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checks.Add(1)
		if r.URL.Path != "/v1/resolve/skill/pinned/1" {
			t.Error("unexpected resource path", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if !enabled.Load() {
			w.WriteHeader(403)
			return
		}
		jsonResponse(w, 200, Resource{Kind: "skill", ID: "pinned", Revision: 1, Spec: JSON(Skill{Name: "fixture", Content: "retain source"})})
	}))
	defer peer.Close()
	s.Remote = &serviceclient.Client{CatalogURL: peer.URL, Token: testToken}
	s.CatalogURL = peer.URL
	snap := basicSnapshot("https://fixture.invalid")
	snap.Agent.Skills = []Ref{{ID: "pinned", Revision: 1}}
	submit(t, s, snap)
	run := claim(t, s, "governed")
	handler := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"toolhub": servicehttp.TokenHash(testToken), "worker": servicehttp.TokenHash(testToken + "worker")}})
	invoke := func(inv Invocation, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/invoke", strings.NewReader(string(JSON(inv))))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	inv := invocation(run)
	if w := invoke(inv, testToken); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	enabled.Store(false)
	inv.OperationID += "-after-withdrawal"
	if w := invoke(inv, testToken); w.Code != 403 {
		t.Fatal("withdrawn asset executed", w.Code, w.Body.String())
	}
	before := checks.Load()
	if w := invoke(inv, testToken+"worker"); w.Code != 403 {
		t.Fatal("direct Worker bypass accepted", w.Code)
	}
	if checks.Load() != before {
		t.Fatal("unauthorized request reached resource resolver")
	}
	var docs, calls int
	if e := s.DB.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM documents),(SELECT count(*) FROM calls)").Scan(&docs, &calls); e != nil || docs != 1 || calls != 1 {
		t.Fatal("denied request produced effects", docs, calls, e)
	}
}
