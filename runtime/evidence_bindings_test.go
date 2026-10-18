package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func TestEvidenceBindingFrozenScopeAndVersions(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	bindEvidenceApp(t, s, root.ID, "futures")
	when := time.Now().Add(-time.Hour)
	if _, err := s.DB.Exec(ctx, `UPDATE runs SET status='completed',created_at=$2,state='{"result":"original report"}' WHERE id=$1`, root.ID, when); err != nil {
		t.Fatal(err)
	}
	makeRun := func(app string, parent any, at time.Time) string {
		t.Helper()
		id := ID()
		_, err := s.DB.Exec(ctx, `INSERT INTO runs(id,submission_key,input_hash,parent_id,snapshot,task,status,created_at,state) SELECT $1,$1,'fixture',$2,snapshot,'fixture','completed',$3,'{"result":"revision"}' FROM runs WHERE id=$4`, id, parent, at, root.ID)
		if err != nil {
			t.Fatal(err)
		}
		if parent == nil {
			bindEvidenceApp(t, s, id, app)
		}
		return id
	}
	child := makeRun("", root.ID, when.Add(time.Second))
	target := makeRun("futures", nil, when.Add(time.Minute))
	foreign := makeRun("other", nil, when.Add(-time.Minute))
	op := root.ID + "/original"
	childOp := child + "/original"
	appEvidenceFixture(t, s, root.ID, op, "document-one", "completed", "tool", "document_read", "保留的原文🙂", when)
	appEvidenceFixture(t, s, child, childOp, "document-child", "completed", "tool", "document_read", "子任务原文", when)
	appEvidenceFixture(t, s, root.ID, root.ID+"/private", "private", "completed", "model", "document_read", "PRIVATE_MODEL_FIXTURE", when)
	appEvidenceFixture(t, s, foreign, foreign+"/read", "foreign", "completed", "tool", "document_read", "FOREIGN_FIXTURE", when)
	bind := func(run string, in bindEvidenceRequest) (*evidenceBinding, error) {
		tx, err := s.DB.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)
		b, err := bindAppEvidence(ctx, tx, "futures", run, "retrospective", in)
		if err != nil {
			return nil, err
		}
		return b, tx.Commit(ctx)
	}
	in := bindEvidenceRequest{SourceRunID: root.ID, ExpectedSourceResultSHA256: resultDigest("original report"), Reason: "explicit correction source"}
	_, err := bind(target, bindEvidenceRequest{SourceRunID: foreign, Reason: "wrong application"})
	requireStateStatus(t, err, 404)
	_, err = bind(target, bindEvidenceRequest{SourceRunID: target, Reason: "self"})
	requireStateStatus(t, err, 400)
	_, err = bind(root.ID, bindEvidenceRequest{SourceRunID: target, Reason: "future/cycle"})
	requireStateStatus(t, err, 409)
	wrong := in
	wrong.ExpectedSourceResultSHA256 = strings.Repeat("0", 64)
	_, err = bind(target, wrong)
	requireStateStatus(t, err, 409)
	if _, err = s.DB.Exec(ctx, `UPDATE runs SET status='running' WHERE id=$1`, child); err != nil {
		t.Fatal(err)
	}
	_, err = bind(target, in)
	requireStateStatus(t, err, 409)
	if _, err = s.DB.Exec(ctx, `UPDATE runs SET status='completed' WHERE id=$1`, child); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	out := make(chan *evidenceBinding, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); b, e := bind(target, in); out <- b; errs <- e }()
	}
	wg.Wait()
	close(out)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var manifest string
	for b := range out {
		if b == nil || b.ReceiptCount != 2 || b.Mode != "retrospective" {
			t.Fatal(b)
		}
		if manifest != "" && manifest != b.ManifestSHA256 {
			t.Fatal("duplicate binding changed manifest")
		}
		manifest = b.ManifestSHA256
	}
	changed := in
	changed.Reason = "rewriting association"
	_, err = bind(target, changed)
	requireStateStatus(t, err, 409)
	for _, query := range []string{`UPDATE app_evidence_bindings SET reason='edited'`, `DELETE FROM app_evidence_bindings`} {
		_, e := s.DB.Exec(ctx, query)
		permissionDenied(t, e)
	}
	page, err := s.readAppEvidence(ctx, "futures", target, "", "", 30)
	if err != nil || len(page.Items) != 2 {
		t.Fatal(page, err)
	}
	for _, item := range page.Items {
		if item.RunID != root.ID && item.RunID != child {
			t.Fatal("wrong original run")
		}
	}
	part, err := s.readAppEvidenceText(ctx, "futures", target, op, 0, 8192, "")
	if err != nil || part.Text != "保留的原文🙂" {
		t.Fatal(part, err)
	}
	_, err = s.readAppEvidenceText(ctx, "other", target, op, 0, 100, "")
	requireStateStatus(t, err, 404)
	_, err = s.readAppEvidenceText(ctx, "futures", target, root.ID+"/private", 0, 100, "")
	requireStateStatus(t, err, 404)
	ids, err := s.appEvidenceRuns(ctx, "futures", target)
	if err != nil || len(ids) != 1 || ids[0] != target {
		t.Fatal("binding widened generic receipt access", ids, err)
	}
	// Later source reads are not pulled into the frozen manifest.
	appEvidenceFixture(t, s, root.ID, root.ID+"/later", "later", "completed", "tool", "document_read", "NOT_IN_MANIFEST", time.Now())
	page, err = s.readAppEvidence(ctx, "futures", target, "", "", 30)
	if err != nil || len(page.Items) != 2 {
		t.Fatal(page, err)
	}
	second := makeRun("futures", nil, when.Add(2*time.Minute))
	b, err := bind(second, bindEvidenceRequest{SourceRunID: target, Reason: "second correction"})
	if err != nil || b.ReceiptCount != 2 {
		t.Fatal(b, err)
	}
	part, err = s.readAppEvidenceText(ctx, "futures", second, childOp, 0, 100, "")
	if err != nil || part.RunID != child {
		t.Fatal(part, err)
	}
	// Changing the response OR its read-offset request invalidates inherited data.
	for _, query := range []string{`UPDATE calls SET request=jsonb_set(request,'{arguments,offset}','1') WHERE id=$1`, `UPDATE calls SET response=jsonb_set(response,'{document,content}','"changed"') WHERE id=$1`} {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		// Restore through a saved JSON snapshot after testing (the service role may
		// change calls, but never the immutable association).
		var request, response []byte
		if e = tx.QueryRow(ctx, `SELECT request,response FROM calls WHERE id=$1`, op).Scan(&request, &response); e != nil {
			t.Fatal(e)
		}
		_ = tx.Rollback(ctx)
		if _, e = s.DB.Exec(ctx, query, op); e != nil {
			t.Fatal(e)
		}
		_, e = s.readAppEvidence(ctx, "futures", second, "", "", 20)
		requireStateStatus(t, e, 409)
		_, e = s.readAppEvidenceText(ctx, "futures", second, op, 0, 100, "")
		requireStateStatus(t, e, 409)
		if _, e = s.DB.Exec(ctx, `UPDATE calls SET request=$2,response=$3 WHERE id=$1`, op, request, response); e != nil {
			t.Fatal(e)
		}
	}
	var retained string
	if err = s.DB.QueryRow(ctx, `SELECT state->>'result' FROM runs WHERE id=$1`, target).Scan(&retained); err != nil || retained != "revision" {
		t.Fatal("historical result changed", retained, err)
	}
}

func TestEvidenceBindingSubmissionAtomicityAndHTTPBoundary(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("https://fixture.invalid")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, Resource{Kind: "agent", ID: "test", Revision: 1, Spec: JSON(snap.Agent)})
	}))
	defer peer.Close()
	s.Remote = &serviceclient.Client{RegistryURL: peer.URL, Token: testToken}
	h := (&Server{Store: s, RegistryURL: peer.URL}).Handler(servicehttp.Config{Role: "runtime", Authorize: Authorize, Peers: map[string]string{"futures": servicehttp.TokenHash(testToken)}})
	source := submit(t, s, snap)
	bindEvidenceApp(t, s, source, "futures")
	if _, err := s.DB.Exec(ctx, `UPDATE runs SET status='completed',created_at=now()-interval '1 hour',state='{"result":"source"}' WHERE id=$1`, source); err != nil {
		t.Fatal(err)
	}
	appEvidenceFixture(t, s, source, source+"/read", "doc", "completed", "tool", "document_read", "source text", time.Now())
	post := func(path, key string, body any) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(string(JSON(body))))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	payload := map[string]any{"agent": Ref{ID: "test", Revision: 1}, "task": "revise", "evidence_source_run": source, "expected_source_result_sha256": resultDigest("source")}
	w := post("/v1/apps/futures/runs", "revision", payload)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var accepted map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &accepted)
	b, _, err := readEvidenceBinding(ctx, s.DB, accepted["id"])
	if err != nil || b.Mode != "submission" || b.ReceiptCount != 1 {
		t.Fatal(b, err)
	}
	retry := post("/v1/apps/futures/runs", "revision", payload)
	if retry.Code != 201 || retry.Body.String() != w.Body.String() {
		t.Fatal("idempotency", retry.Code, retry.Body.String())
	}
	delete(payload, "evidence_source_run")
	if got := post("/v1/apps/futures/runs", "revision", payload); got.Code == 201 {
		t.Fatal("changed binding accepted")
	}
	payload["evidence_source_run"] = "missing-source"
	if got := post("/v1/apps/futures/runs", "invalid-source", payload); got.Code != 404 {
		t.Fatal(got.Code, got.Body.String())
	}
	var count int
	if err = s.DB.QueryRow(ctx, `SELECT count(*) FROM runs WHERE submission_key='futures:invalid-source'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial submission committed", count, err)
	}
	if got := post("/v1/apps/other/runs/"+source+"/evidence-binding", "", bindEvidenceRequest{SourceRunID: source, ExpectedSourceResultSHA256: resultDigest("source"), Reason: "cross app"}); got.Code != 403 {
		t.Fatal(got.Code, got.Body.String())
	}
}

func TestEvidenceBindingRejectsActiveEmptyAndOversizedSource(t *testing.T) {
	s, root, _ := workingFixture(t)
	ctx := context.Background()
	bindEvidenceApp(t, s, root.ID, "futures")
	target := ID()
	_, err := s.DB.Exec(ctx, `UPDATE runs SET created_at=now()-interval '1 hour',status='completed' WHERE id=$1`, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB.Exec(ctx, `INSERT INTO runs(id,submission_key,input_hash,snapshot,task,status) SELECT $1,$1,'fixture',snapshot,'fixture','completed' FROM runs WHERE id=$2`, target, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	bindEvidenceApp(t, s, target, "futures")
	bind := func() error {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(ctx)
		_, e = bindAppEvidence(ctx, tx, "futures", target, "retrospective", bindEvidenceRequest{SourceRunID: root.ID, Reason: "bound check"})
		if e != nil {
			return e
		}
		return tx.Commit(ctx)
	}
	requireStateStatus(t, bind(), 409)
	appEvidenceFixture(t, s, root.ID, root.ID+"/read", "doc", "completed", "tool", "document_read", "original", time.Now())
	for _, id := range []string{root.ID, target} {
		if _, err = s.DB.Exec(ctx, `UPDATE runs SET status='running' WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
		requireStateStatus(t, bind(), 409)
		if _, err = s.DB.Exec(ctx, `UPDATE runs SET status='completed' WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.DB.Exec(ctx, `INSERT INTO calls(id,run_id,kind,request_hash,request,status,attempt,fence,response) SELECT c.id||'/'||g.n,c.run_id,c.kind,c.request_hash,c.request,c.status,c.attempt,c.fence,c.response FROM calls c CROSS JOIN generate_series(1,256) AS g(n) WHERE c.id=$1`, root.ID+"/read")
	if err != nil {
		t.Fatal(err)
	}
	requireStateStatus(t, bind(), 409)
	if _, err = s.DB.Exec(ctx, `DELETE FROM calls WHERE id=$1`, root.ID+"/read/256"); err != nil {
		t.Fatal(err)
	}
	if err = bind(); err != nil {
		t.Fatal(err)
	}
	page, err := s.readAppEvidence(ctx, "futures", target, "", "", 5)
	if err != nil || len(page.Items) != 5 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	next, err := s.readAppEvidence(ctx, "futures", target, "", page.NextCursor, 5)
	if err != nil || len(next.Items) != 5 || next.Items[0].OperationID == page.Items[0].OperationID {
		t.Fatal(next, err)
	}
}
