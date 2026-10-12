package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func TestNoteManagementRoutesScopeAuditAndKnownConflict(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	peers := map[string]string{}
	for _, role := range []string{"control", "worker", "toolhub", "futures", "storage", "knowledge"} {
		peers[role] = servicehttp.TokenHash(testToken + role)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize})
	request := func(role, method, path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(string(JSON(body))))
		r.Header.Set("Authorization", "Bearer "+testToken+role)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := request("control", "POST", "/v1/documents", Document{Kind: "memory", Title: "global preference", Content: "keep evidence", Source: "fixture://notes"})
	var d Document
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &d) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, role := range []string{"worker", "toolhub", "futures", "storage", "knowledge"} {
		for _, path := range []string{"/v1/notes", "/v1/notes/" + d.ID + "/history", "/v1/notes/" + d.ID + "/audit"} {
			if w := request(role, "GET", path, nil); w.Code != 403 {
				t.Fatal("unauthorized management read", role, path, w.Code)
			}
		}
		for _, path := range []string{"/v1/notes/revise", "/v1/notes/retire"} {
			if w := request(role, "POST", path, map[string]any{}); w.Code != 403 {
				t.Fatal("unauthorized management write", role, w.Code)
			}
		}
	}
	in := map[string]any{"request_id": "correction-1", "expected_current_id": d.ID, "title": d.Title, "content": "keep immutable evidence", "source": d.Source, "reason": "clarify preference"}
	w = request("control", "POST", "/v1/notes/revise", in)
	first := append([]byte(nil), w.Body.Bytes()...)
	var revised struct{ Document Document }
	if w.Code != 200 || json.Unmarshal(first, &revised) != nil {
		t.Fatal(w.Code, string(first))
	}
	if w = request("control", "POST", "/v1/notes/revise", in); w.Code != 200 || !jsonEqual(w.Body.Bytes(), first) {
		t.Fatal("lost admin reply retry", w.Code, w.Body.String())
	}
	in["request_id"] = "correction-stale"
	if w = request("control", "POST", "/v1/notes/revise", in); w.Code != 409 {
		t.Fatal("stale head is not conflict", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"note_rejected_request_id":"correction-stale"`) {
		t.Fatal("known mutation rejection lacks request identity", w.Body.String())
	}
	if unauth := request("unknown", "POST", "/v1/notes/revise", in); unauth.Code != 401 || strings.Contains(unauth.Body.String(), "note_rejected_request_id") {
		t.Fatal("authentication incorrectly classified as mutation result", unauth.Code, unauth.Body.String())
	}
	in["request_id"] = "forged-actor"
	in["actor_id"] = "somebody-else"
	if w = request("control", "POST", "/v1/notes/revise", in); w.Code != 400 {
		t.Fatal("accepted client actor", w.Code)
	}
	if w = request("control", "POST", "/v1/notes/retire", map[string]any{"request_id": "withdraw-1", "expected_current_id": revised.Document.ID, "reason": "obsolete preference"}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request("control", "GET", "/v1/documents/"+d.ID, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"retired"`) || !strings.Contains(w.Body.String(), "keep evidence") {
		t.Fatal("original lost or lifecycle hidden", w.Body.String())
	}
	for _, path := range []string{"/v1/notes?state=retired", "/v1/notes/" + d.ID + "/history", "/v1/notes/" + d.ID + "/audit"} {
		w = request("control", "GET", path, nil)
		if w.Code != 200 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	w = request("control", "GET", "/v1/notes/"+d.ID+"/audit", nil)
	var audit struct {
		Items []struct {
			ActorID   string `json:"actor_id"`
			RequestID string `json:"request_id"`
		}
	}
	if json.Unmarshal(w.Body.Bytes(), &audit) != nil || len(audit.Items) != 3 || audit.Items[2].ActorID != "control" || audit.Items[2].RequestID != "withdraw-1" {
		t.Fatal("audit identity or idempotency link missing", w.Body.String())
	}
	w = request("control", "GET", "/v1/documents", nil)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("retired note in effective search", w.Body.String())
	}
}

func TestNoteManagementPaginationAndLegacyKnowledgeBoundary(t *testing.T) {
	s := isolatedDomainDatabase(t, "runtime")
	ctx := context.Background()
	put := func(d Document) (Document, error) {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			return d, e
		}
		defer tx.Rollback(ctx)
		d, e = putDocument(ctx, tx, d)
		if e != nil {
			return d, e
		}
		return d, tx.Commit(ctx)
	}
	h := (&Server{Store: s}).Handler(servicehttp.Config{Role: "runtime", Peers: map[string]string{"control": servicehttp.TokenHash(testToken)}, Authorize: Authorize})
	request := func(method, path string, body any) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(string(JSON(body))))
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for i := 0; i < 32; i++ {
		if _, e := put(Document{Kind: "memory", Title: fmt.Sprintf("分页 %02d", i), Content: "durable note", Source: "fixture://pagination"}); e != nil {
			t.Fatal(e)
		}
	}
	legacy, e := put(Document{Kind: "evidence", Scope: "knowledge:old-import", Title: "legacy copy", Content: "unaltered original", Source: "fixture://legacy"})
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	after := ""
	for page := 0; page < 2; page++ {
		w := request("GET", "/v1/notes?after="+after, nil)
		var result struct {
			Items []struct{ ID string }
			Next  string `json:"next_cursor"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		want := 25
		if page == 1 {
			want = 7
		}
		if len(result.Items) != want {
			t.Fatal("wrong page size or Knowledge copy visible", len(result.Items))
		}
		for _, d := range result.Items {
			if seen[d.ID] || d.ID == legacy.ID {
				t.Fatal("duplicate or legacy Knowledge copy", d.ID)
			}
			seen[d.ID] = true
		}
		after = result.Next
	}
	if len(seen) != 32 || after != "" {
		t.Fatal("incomplete pagination", len(seen), after)
	}
	for _, kind := range []string{"revise", "retire"} {
		input := map[string]any{"request_id": "legacy-" + kind, "expected_current_id": legacy.ID, "reason": "must manage source in Knowledge"}
		if kind == "revise" {
			input["title"] = legacy.Title
			input["content"] = "changed"
			input["source"] = legacy.Source
		}
		if w := request("POST", "/v1/notes/"+kind, input); w.Code != 403 {
			t.Fatal("legacy mutation accepted", kind, w.Code, w.Body.String())
		}
	}
	changed := legacy
	changed.ID = ""
	changed.Supersedes = &legacy.ID
	changed.Content = "not allowed"
	if _, e := put(changed); e == nil {
		t.Fatal("legacy insert bypassed Knowledge boundary")
	}
	if w := request("GET", "/v1/documents/"+legacy.ID, nil); w.Code != 200 || !strings.Contains(w.Body.String(), "unaltered original") {
		t.Fatal("historical source no longer readable", w.Code, w.Body.String())
	}
}
