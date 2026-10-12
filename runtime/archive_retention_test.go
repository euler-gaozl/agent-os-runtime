package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-runtime/execution"
	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This HTTP fixture retains immutable bytes in memory. It exercises the actual
// SDK transport and Runtime transaction domain without importing Storage's SQL
// or implementation. It is not a durability test of the Storage service itself.
type archiveHTTPFixture struct {
	mu     sync.Mutex
	bodies map[string][]byte
	server *httptest.Server
}

func newArchiveHTTPFixture(t *testing.T, runID string, afterPut func(*http.Request) bool) *archiveHTTPFixture {
	t.Helper()
	a := &archiveHTTPFixture{bodies: map[string][]byte{}}
	a.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" || req.Header.Get("Authorization") != "Bearer "+testToken || req.Header.Get("X-Artifact-Root") != "run:"+runID {
			t.Error("archive identity or ownership envelope changed")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, (4<<20)+1))
		hash := Hash(body)
		if err != nil || len(body) > 4<<20 || req.URL.Path != "/v1/artifacts/"+hash {
			t.Error("invalid content-addressed archive upload")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.bodies[hash] = append([]byte(nil), body...)
		a.mu.Unlock()
		if afterPut != nil && afterPut(req) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]string{"hash": hash})
	}))
	t.Cleanup(a.server.Close)
	return a
}

func (a *archiveHTTPFixture) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.bodies)
}

func (a *archiveHTTPFixture) read(t *testing.T, hash string) ([]byte, runstate.HarnessArchive) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	manifest := a.bodies[hash]
	var archive runstate.HarnessArchive
	if Hash(manifest) != hash || json.Unmarshal(manifest, &archive) != nil || archive.Version != 1 || len(archive.Parts) == 0 || len(archive.Parts) > 18 {
		t.Fatal("retained manifest invalid")
	}
	var body []byte
	for _, part := range archive.Parts {
		chunk := a.bodies[part.Hash]
		if len(chunk) != part.Bytes || part.Offset != len(body) || len(chunk) > 1<<20 || !utf8.Valid(chunk) || Hash(chunk) != part.Hash {
			t.Fatal("retained Unicode segment invalid")
		}
		body = append(body, chunk...)
	}
	if len(body) != archive.Bytes || Hash(body) != archive.SHA256 || !json.Valid(body) {
		t.Fatal("retained original batch changed")
	}
	return body, archive
}

func archiveOwnerFixture(t *testing.T) (*Store, *pgxpool.Pool, *pgxpool.Pool, string) {
	t.Helper()
	db, role, worker := fixtureDatabase(t)
	if err := Migrate(context.Background(), db, role, worker); err != nil {
		t.Fatal(err)
	}
	s := NewStore(rolePool(t, db, role), nil, "")
	id := submit(t, s, basicSnapshot("https://fixture.invalid"))
	return s, db, rolePool(t, db, worker), id
}

func archiveModelFrame(seq, start int, events []byte) execution.Frame {
	return execution.Frame{ID: seq, Method: "model", Params: JSON(map[string]any{"messages": []Message{{Role: "user", Content: "controlled archive test"}}}), Events: events, EventStart: start, EventEnd: start + 1}
}

func TestArchiveHTTPManifestLostReplyPreservesFirstBatchAndProvenance(t *testing.T) {
	s, db, work, id := archiveOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var lost atomic.Bool
	lost.Store(true)
	a := newArchiveHTTPFixture(t, id, func(req *http.Request) bool {
		return req.Header.Get("X-Artifact-Media-Type") == "application/vnd.agentos.event-manifest+json" && lost.CompareAndSwap(true, false)
	})
	s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, a.server.URL
	engine := execution.New(work, artifactWriter{s})
	r, err := engine.Claim(ctx, "original", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	original := JSON([]any{map[string]string{"type": "fixture", "text": strings.Repeat("跨分段中文原文", 100000)}})
	frame := archiveModelFrame(1, 0, original)
	modelCalls := 0
	effects := exchangeEffects{model: func(context.Context, Run, string, []Message, []Tool) (Message, error) {
		modelCalls++
		return Message{Role: "assistant", Content: "fixture"}, nil
	}, tool: func(context.Context, Invocation) ([]byte, error) {
		t.Error("unexpected tool effect")
		return nil, errors.New("unexpected tool")
	}}
	if _, _, err = engine.Exchange(ctx, &r, frame, effects); err == nil || lost.Load() || modelCalls != 0 {
		t.Fatal("manifest reply loss did not block the model", err, modelCalls)
	}
	var staged []byte
	var manifestHash string
	if err = db.QueryRow(ctx, "SELECT batch,manifest_hash FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&staged, &manifestHash); err != nil || string(staged) != string(original) || manifestHash != "" {
		t.Fatal("first committed batch was not retained", err)
	}
	before := a.count()
	if before < 3 {
		t.Fatal("test did not retain multiple segments and the manifest")
	}
	if _, err = db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	next, err := engine.Claim(ctx, "replacement", 15*time.Second)
	if err != nil || next.Fence <= r.Fence {
		t.Fatal("replacement claim failed", err)
	}
	if _, _, err = engine.Exchange(ctx, &r, frame, effects); !errors.Is(err, ErrFence) {
		t.Fatal("old uploader was not fenced", err)
	}
	frame.Events = JSON([]any{map[string]string{"type": "regenerated", "text": "must not replace first evidence"}})
	if _, _, err = engine.Exchange(ctx, &next, frame, effects); err != nil {
		t.Fatal(err)
	}
	retained, archive := a.read(t, next.State.HarnessArtifact)
	if string(retained) != string(original) || archive.SourceOwner != r.Owner || archive.SourceFence != r.Fence || archive.SourceStart != 0 || archive.SourceEnd != 1 || a.count() != before || modelCalls != 1 {
		t.Fatal("replay duplicated/replaced original evidence or provenance")
	}
	if err = db.QueryRow(ctx, "SELECT batch FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&staged); err != nil || staged != nil {
		t.Fatal("acknowledged staging not retired", err)
	}
	firstHash := next.State.HarnessArtifact
	// Rebuilt DSH sessions can have different native offsets. The persisted
	// directory must stay contiguous while recording the new source offsets.
	second := archiveModelFrame(2, 37, JSON([]any{map[string]string{"type": "after-replay"}}))
	if _, _, err = engine.Exchange(ctx, &next, second, effects); err != nil {
		t.Fatal(err)
	}
	_, follow := a.read(t, next.State.HarnessArtifact)
	if follow.Previous != firstHash || follow.EventStart != archive.EventEnd || follow.EventEnd != archive.EventEnd+1 || follow.SourceStart != 37 || follow.SourceEnd != 38 || follow.SourceFence != next.Fence || follow.SourceOwner != next.Owner {
		t.Fatal("cross-attempt archive continuity/provenance lost")
	}
}

func TestArchiveHTTPCancellationKeepsStagedBytesBeforeAcknowledgement(t *testing.T) {
	s, db, work, id := archiveOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var cancelled atomic.Bool
	a := newArchiveHTTPFixture(t, id, func(req *http.Request) bool {
		if req.Header.Get("X-Artifact-Media-Type") == "application/vnd.agentos.event-manifest+json" && cancelled.CompareAndSwap(false, true) {
			if err := s.Cancel(ctx, id); err != nil {
				t.Error(err)
			}
		}
		return false
	})
	s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, a.server.URL
	engine := execution.New(work, artifactWriter{s})
	r, err := engine.Claim(ctx, "cancel-during-upload", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := JSON([]any{map[string]string{"type": "before-cancel", "text": "保留原始资料"}})
	calls := 0
	effects := exchangeEffects{model: func(context.Context, Run, string, []Message, []Tool) (Message, error) { calls++; return Message{}, nil }}
	if _, _, err = engine.Exchange(ctx, &r, archiveModelFrame(1, 0, body), effects); !errors.Is(err, ErrFence) || calls != 0 || !cancelled.Load() {
		t.Fatal("cancellation crossed the archive barrier", err)
	}
	var raw []byte
	var hash string
	if err = db.QueryRow(ctx, "SELECT batch,manifest_hash FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&raw, &hash); err != nil || string(raw) != string(body) || hash != "" {
		t.Fatal("cancelled archive lost pending bytes", err)
	}
	if a.count() != 2 {
		t.Fatal("test did not cancel after real HTTP byte acceptance")
	}
}

func TestArchiveHTTPLeaseTakeoverDuringManifestUpload(t *testing.T) {
	s, db, work, id := archiveOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var takeover atomic.Bool
	replacement := make(chan Run, 1)
	a := newArchiveHTTPFixture(t, id, func(req *http.Request) bool {
		if req.Header.Get("X-Artifact-Media-Type") != "application/vnd.agentos.event-manifest+json" || !takeover.CompareAndSwap(false, true) {
			return false
		}
		// The old uploader passed Exchange's entrance check and Storage accepted
		// every byte. Ownership changes before its final acknowledgement lock.
		if _, err := db.Exec(ctx, "UPDATE runs SET lease_until=now()-interval '1 second' WHERE id=$1", id); err != nil {
			t.Error(err)
			return true
		}
		next, err := execution.New(work, artifactWriter{s}).Claim(ctx, "during-upload-replacement", 10*time.Second)
		if err != nil {
			t.Error(err)
			return true
		}
		replacement <- next
		return false
	})
	s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, a.server.URL
	engine := execution.New(work, artifactWriter{s})
	r, err := engine.Claim(ctx, "uploading-original", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := JSON([]any{map[string]string{"type": "inflight-takeover", "text": "first original bytes"}})
	frame := archiveModelFrame(1, 0, body)
	models := 0
	effects := exchangeEffects{model: func(context.Context, Run, string, []Message, []Tool) (Message, error) {
		models++
		return Message{Role: "assistant", Content: "fixture"}, nil
	}}
	if _, _, err = engine.Exchange(ctx, &r, frame, effects); !errors.Is(err, ErrFence) || models != 0 || !takeover.Load() {
		t.Fatal("in-flight old upload crossed the final fence", err, models)
	}
	var next Run
	select {
	case next = <-replacement:
	case <-ctx.Done():
		t.Fatal("replacement claim not observed")
	}
	if next.Fence <= r.Fence || next.Owner == r.Owner {
		t.Fatal("ownership did not advance")
	}
	var staged []byte
	var hash string
	if err = db.QueryRow(ctx, "SELECT batch,manifest_hash FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&staged, &hash); err != nil || string(staged) != string(body) || hash != "" {
		t.Fatal("old owner acknowledged or discarded the staged original", err)
	}
	before := a.count()
	frame.Events = JSON([]any{map[string]string{"type": "regenerated"}})
	if _, _, err = engine.Exchange(ctx, &next, frame, effects); err != nil {
		t.Fatal(err)
	}
	retained, archive := a.read(t, next.State.HarnessArtifact)
	if models != 1 || a.count() != before || string(retained) != string(body) || archive.SourceOwner != r.Owner || archive.SourceFence != r.Fence {
		t.Fatal("replacement lost the first uploader's evidence/provenance")
	}
}

func TestArchiveHTTPFailedTurnRetainsFinalEventsBeforeFailureCheckpoint(t *testing.T) {
	s, db, work, id := archiveOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a := newArchiveHTTPFixture(t, id, nil)
	s.Remote, s.StorageURL = &serviceclient.Client{Token: testToken}, a.server.URL
	engine := execution.New(work, artifactWriter{s})
	r, err := engine.Claim(ctx, "failure-retention", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := JSON([]any{map[string]string{"type": "turn/end", "reason": "fixture-failure"}})
	frame := execution.Frame{ID: 1, Method: "failed", Params: JSON(map[string]string{"error": "fixture-failure"}), Events: body, EventEnd: 1}
	_, _, err = engine.Exchange(ctx, &r, frame, exchangeEffects{})
	if err == nil || !strings.Contains(err.Error(), "fixture-failure") || r.State.HarnessArtifact == "" {
		t.Fatal("failure discarded final evidence", err)
	}
	if checkpointErr := engine.CheckpointFailure(ctx, r, err); checkpointErr != nil {
		t.Fatal(checkpointErr)
	}
	fresh, err := s.Run(ctx, id)
	if err != nil || fresh.Status != "failed" || fresh.State.HarnessArtifact != r.State.HarnessArtifact {
		t.Fatal("failed run lost its retained artifact pointer", err)
	}
	retained, _ := a.read(t, fresh.State.HarnessArtifact)
	if string(retained) != string(body) {
		t.Fatal("final failure evidence changed")
	}
	var acknowledged bool
	if err = db.QueryRow(ctx, "SELECT batch IS NULL AND manifest_hash<>'' FROM harness_archives WHERE run_id=$1 AND seq=1", id).Scan(&acknowledged); err != nil || !acknowledged {
		t.Fatal("final batch not acknowledged", err)
	}
}

func TestFutureEngineRemainsQueuedWhenCurrentWorkerClaims(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	future := submit(t, s, basicSnapshot("https://fixture.invalid"))
	if _, err := s.DB.Exec(ctx, "UPDATE runs SET engine='future-unsupported' WHERE id=$1", future); err != nil {
		t.Fatal(err)
	}
	current := submit(t, s, basicSnapshot("https://fixture.invalid"))
	r := claim(t, s, "supported-only")
	if r.ID != current {
		t.Fatal("unsupported engine claimed")
	}
	fresh, err := s.Run(ctx, future)
	if err != nil || fresh.Status != "queued" || fresh.Fence != 0 {
		t.Fatal("unsupported run mutated", err)
	}
}
