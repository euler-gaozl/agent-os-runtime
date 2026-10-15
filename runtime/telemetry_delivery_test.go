package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// Runtime owns the outbox and its leased acknowledgements. This fixture has
// no Telemetry implementation, inbox, exporter, model or external service.
// NOLOGIN/SET ROLE exercises database permissions, not separate login secrets.
type telemetryOwnerFixture struct {
	store  *Store
	server *httptest.Server
	client *http.Client
	tokens map[string]string
}

func newTelemetryOwnerFixture(t *testing.T, ctx context.Context) *telemetryOwnerFixture {
	t.Helper()
	db, role, workerRole := fixtureDatabase(t)
	if err := Migrate(ctx, db, role, workerRole); err != nil {
		t.Fatal(err)
	}
	app := rolePool(t, db, role)
	var currentRole string
	var superuser bool
	if err := app.QueryRow(ctx, "SELECT current_user,rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&currentRole, &superuser); err != nil || currentRole != role || superuser {
		t.Fatal("fixture is not using the restricted Runtime role", err)
	}
	f := &telemetryOwnerFixture{store: NewStore(app, nil, ""), tokens: map[string]string{}}
	peers := map[string]string{}
	for _, name := range []string{"telemetry", "control", "worker", "toolhub", "futures"} {
		f.tokens[name] = testToken + "-" + name + "-" + ID()
		peers[name] = servicehttp.TokenHash(f.tokens[name])
	}
	f.server = httptest.NewServer((&Server{Store: f.store}).Handler(servicehttp.Config{Role: "runtime", Peers: peers, Authorize: Authorize}))
	t.Cleanup(f.server.Close)
	host, _, err := net.SplitHostPort(f.server.Listener.Addr().String())
	if err != nil || host != "127.0.0.1" {
		t.Fatal("fixture HTTP listener is not IPv4 loopback")
	}
	transport := &http.Transport{Proxy: nil}
	f.client = &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("fixture redirects are forbidden")
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return f
}

func (f *telemetryOwnerFixture) post(t *testing.T, ctx context.Context, role, endpoint string, input any) (int, []byte) {
	t.Helper()
	if endpoint != "/v1/telemetry/claim" && endpoint != "/v1/telemetry/ack" {
		t.Fatal("unexpected fixture endpoint")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+endpoint, bytes.NewReader(JSON(input)))
	if err != nil {
		t.Fatal(err)
	}
	token := f.tokens[role]
	if role == "unknown" {
		token = testToken + "-unrecognized-synthetic-peer"
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := f.client.Do(req)
	if err != nil {
		t.Fatal("fixture HTTP request failed", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		t.Fatal("fixture HTTP response is invalid or oversized", err)
	}
	return response.StatusCode, body
}

func (f *telemetryOwnerFixture) claim(t *testing.T, ctx context.Context, owner string) []telemetryDelivery {
	t.Helper()
	status, body := f.post(t, ctx, "telemetry", "/v1/telemetry/claim", map[string]string{"owner": owner})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d", status)
	}
	// Check the actual wire shape, not only a struct that ignores extra fields.
	var wire []map[string]json.RawMessage
	if err := json.Unmarshal(body, &wire); err != nil || wire == nil {
		t.Fatal("claim did not return a JSON array", err)
	}
	for _, item := range wire {
		if len(item) != 5 {
			t.Fatal("outbox export includes unexpected fields")
		}
		for _, key := range []string{"sequence", "run_id", "kind", "created_at", "fence"} {
			if _, ok := item[key]; !ok {
				t.Fatal("outbox export omitted identity field", key)
			}
		}
	}
	var batch []telemetryDelivery
	if err := json.Unmarshal(body, &batch); err != nil {
		t.Fatal(err)
	}
	return batch
}

func (f *telemetryOwnerFixture) ack(t *testing.T, ctx context.Context, owner string, receipts []telemetryReceipt, want int) {
	t.Helper()
	status, body := f.post(t, ctx, "telemetry", "/v1/telemetry/ack", map[string]any{"owner": owner, "receipts": receipts})
	if status != want {
		t.Fatalf("ack status = %d, want %d", status, want)
	}
	if want == http.StatusOK {
		var reply struct {
			Acknowledged bool `json:"acknowledged"`
		}
		if err := json.Unmarshal(body, &reply); err != nil || !reply.Acknowledged {
			t.Fatal("successful ack lacks acknowledgement", err)
		}
	}
}

func telemetryOwnerReceipts(batch []telemetryDelivery) []telemetryReceipt {
	receipts := make([]telemetryReceipt, 0, len(batch))
	for _, d := range batch {
		receipts = append(receipts, telemetryReceipt{Sequence: d.Sequence, Fence: d.Fence})
	}
	return receipts
}

func telemetryOwnerBatch(t *testing.T, batch []telemetryDelivery, run string, want map[int64]string) {
	t.Helper()
	if len(batch) != len(want) {
		t.Fatalf("claim size = %d, want %d", len(batch), len(want))
	}
	seen := map[int64]bool{}
	for _, d := range batch {
		kind, ok := want[d.Sequence]
		if !ok || seen[d.Sequence] || d.RunID != run || d.Kind != kind || d.Fence < 1 || d.CreatedAt.IsZero() {
			t.Fatal("claim lost, duplicated or changed event identity")
		}
		seen[d.Sequence] = true
	}
}

func telemetryOwnerRows(t *testing.T, ctx context.Context, s *Store, contentOnly bool) []byte {
	t.Helper()
	query := "SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY seq),'[]'::jsonb) FROM events e"
	if contentOnly {
		query = "SELECT COALESCE(jsonb_agg(to_jsonb(e)-'telemetry_owner'-'telemetry_fence'-'telemetry_until'-'telemetry_delivered_at' ORDER BY seq),'[]'::jsonb) FROM events e"
	}
	var rows []byte
	if err := s.DB.QueryRow(ctx, query).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestTelemetryOwnerHTTPLateCommitAndServiceIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newTelemetryOwnerFixture(t, ctx)
	id, err := f.store.Submit(ctx, ID(), "Synthetic outbox fixture; no model execution", basicSnapshot("https://fixture.invalid"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var accepted int64
	if err = f.store.DB.QueryRow(ctx, "SELECT seq FROM events WHERE run_id=$1 AND kind='accepted'", id).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	owner := ID()
	beforeIdentity := telemetryOwnerRows(t, ctx, f.store, false)
	for _, role := range []string{"missing", "unknown", "control", "worker", "toolhub", "futures"} {
		want := http.StatusForbidden
		if role == "missing" || role == "unknown" {
			want = http.StatusUnauthorized
		}
		for _, endpoint := range []string{"/v1/telemetry/claim", "/v1/telemetry/ack"} {
			var input any = map[string]string{"owner": owner}
			if endpoint == "/v1/telemetry/ack" {
				input = map[string]any{"owner": owner, "receipts": []telemetryReceipt{{Sequence: accepted, Fence: 1}}}
			}
			status, _ := f.post(t, ctx, role, endpoint, input)
			if status != want {
				t.Fatalf("%s %s status = %d, want %d", role, endpoint, status, want)
			}
			if !bytes.Equal(beforeIdentity, telemetryOwnerRows(t, ctx, f.store, false)) {
				t.Fatal("rejected service identity changed the outbox")
			}
		}
	}

	// Sequence allocation is not commit order: keep the smaller sequence in a
	// real open transaction while a second connection commits the larger one.
	late, err := f.store.DB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		rollbackContext, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = late.Rollback(rollbackContext)
	}()
	var low, high int64
	if err = late.QueryRow(ctx, "INSERT INTO events(run_id,kind,payload) VALUES($1,'late',$2) RETURNING seq", id, JSON(map[string]string{"private_fixture_payload": "late payload stays in Runtime"})).Scan(&low); err != nil {
		t.Fatal(err)
	}
	if err = f.store.DB.QueryRow(ctx, "INSERT INTO events(run_id,kind,payload) VALUES($1,'early',$2) RETURNING seq", id, JSON(map[string]string{"private_fixture_payload": "early payload stays in Runtime"})).Scan(&high); err != nil || high <= low {
		t.Fatal("fixture sequence allocation order is invalid", err)
	}
	earlyContent := telemetryOwnerRows(t, ctx, f.store, true)
	first := f.claim(t, ctx, owner)
	telemetryOwnerBatch(t, first, id, map[int64]string{accepted: "accepted", high: "early"})
	f.ack(t, ctx, owner, telemetryOwnerReceipts(first), http.StatusOK)
	if !bytes.Equal(earlyContent, telemetryOwnerRows(t, ctx, f.store, true)) {
		t.Fatal("first delivery changed already committed event content")
	}
	if err = late.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	content := telemetryOwnerRows(t, ctx, f.store, true)
	second := f.claim(t, ctx, owner)
	telemetryOwnerBatch(t, second, id, map[int64]string{low: "late"})
	f.ack(t, ctx, owner, telemetryOwnerReceipts(second), http.StatusOK)
	acknowledged := telemetryOwnerRows(t, ctx, f.store, false)
	f.ack(t, ctx, owner, telemetryOwnerReceipts(second), http.StatusOK)
	if !bytes.Equal(acknowledged, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("duplicate acknowledgement changed committed delivery metadata")
	}
	if batch := f.claim(t, ctx, ID()); len(batch) != 0 {
		t.Fatal("acknowledged event was delivered again")
	}
	var total, delivered int
	if err = f.store.DB.QueryRow(ctx, "SELECT count(*),count(telemetry_delivered_at) FROM events").Scan(&total, &delivered); err != nil || total != 3 || delivered != 3 {
		t.Fatal("late commit was lost or an event was removed", total, delivered, err)
	}
	if !bytes.Equal(content, telemetryOwnerRows(t, ctx, f.store, true)) {
		t.Fatal("delivery changed original event identity, payload or timestamp")
	}
	t.Logf("late sequence %d retained after higher sequence %d was acknowledged; all 3 events retained; 12 identity rejections left rows unchanged", low, high)
}

func TestTelemetryOwnerHTTPNaturalLeaseFenceAndAtomicAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newTelemetryOwnerFixture(t, ctx)
	id, err := f.store.Submit(ctx, ID(), "Synthetic delivery lease fixture; no model execution", basicSnapshot("https://fixture.invalid"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var accepted, extra int64
	if err = f.store.DB.QueryRow(ctx, "SELECT seq FROM events WHERE run_id=$1 AND kind='accepted'", id).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	if err = f.store.DB.QueryRow(ctx, "INSERT INTO events(run_id,kind,payload) VALUES($1,'lease-fixture',$2) RETURNING seq", id, JSON(map[string]string{"private_fixture_payload": "retained source body"})).Scan(&extra); err != nil {
		t.Fatal(err)
	}
	content := telemetryOwnerRows(t, ctx, f.store, true)
	oldOwner, nextOwner := ID(), ID()
	first := f.claim(t, ctx, oldOwner)
	want := map[int64]string{accepted: "accepted", extra: "lease-fixture"}
	telemetryOwnerBatch(t, first, id, want)
	leased := telemetryOwnerRows(t, ctx, f.store, false)
	if batch := f.claim(t, ctx, nextOwner); len(batch) != 0 {
		t.Fatal("active delivery lease was taken by a new owner")
	}
	if !bytes.Equal(leased, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("unsuccessful competing claim changed the active lease")
	}
	f.ack(t, ctx, nextOwner, telemetryOwnerReceipts(first), http.StatusPreconditionFailed)
	if !bytes.Equal(leased, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("wrong delivery owner acknowledged active events")
	}
	var leaseUntil, databaseNow time.Time
	if err = f.store.DB.QueryRow(ctx, "SELECT max(telemetry_until),clock_timestamp() FROM events WHERE run_id=$1", id).Scan(&leaseUntil, &databaseNow); err != nil || !leaseUntil.After(databaseNow) {
		t.Fatal("claim did not establish a future lease", err)
	}
	t.Logf("waiting for the unmodified 30-second lease; observed remaining %s", leaseUntil.Sub(databaseNow))
	// Wait on the database clock. No UPDATE of owner, fence, delivery or lease
	// fields is used to accelerate takeover or manufacture an acknowledgement.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err = f.store.DB.QueryRow(ctx, "SELECT bool_and(telemetry_until<clock_timestamp()) FROM events WHERE run_id=$1", id).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("natural delivery lease did not expire within the test bound")
		case <-ticker.C:
		}
	}
	f.ack(t, ctx, oldOwner, telemetryOwnerReceipts(first), http.StatusPreconditionFailed)
	if !bytes.Equal(leased, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("expired-owner acknowledgement changed an event")
	}
	retry := f.claim(t, ctx, nextOwner)
	telemetryOwnerBatch(t, retry, id, want)
	old := map[int64]telemetryDelivery{}
	for _, d := range first {
		old[d.Sequence] = d
	}
	for _, d := range retry {
		prior := old[d.Sequence]
		if d.Fence != prior.Fence+1 || !d.CreatedAt.Equal(prior.CreatedAt) || d.Kind != prior.Kind || d.RunID != prior.RunID {
			t.Fatal("natural takeover did not preserve identity and advance the fence once")
		}
	}
	beforeAck := telemetryOwnerRows(t, ctx, f.store, false)
	f.ack(t, ctx, oldOwner, telemetryOwnerReceipts(first), http.StatusPreconditionFailed)
	f.ack(t, ctx, nextOwner, telemetryOwnerReceipts(first), http.StatusPreconditionFailed)
	if !bytes.Equal(beforeAck, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("old owner or old fence changed reclaimed events")
	}
	// The first row would succeed, but a stale second receipt must roll back
	// the entire acknowledgement transaction, not leave a partially delivered batch.
	mixed := telemetryOwnerReceipts(retry)
	mixed[1].Fence = old[mixed[1].Sequence].Fence
	f.ack(t, ctx, nextOwner, mixed, http.StatusPreconditionFailed)
	if !bytes.Equal(beforeAck, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("failed receipt batch partially acknowledged an earlier row")
	}
	f.ack(t, ctx, nextOwner, telemetryOwnerReceipts(retry), http.StatusOK)
	completed := telemetryOwnerRows(t, ctx, f.store, false)
	f.ack(t, ctx, nextOwner, telemetryOwnerReceipts(retry), http.StatusOK)
	if !bytes.Equal(completed, telemetryOwnerRows(t, ctx, f.store, false)) {
		t.Fatal("new-owner acknowledgement is not idempotent")
	}
	if batch := f.claim(t, ctx, ID()); len(batch) != 0 {
		t.Fatal("completed delivery was reclaimed")
	}
	var total, delivered int
	if err = f.store.DB.QueryRow(ctx, "SELECT count(*),count(telemetry_delivered_at) FROM events").Scan(&total, &delivered); err != nil || total != 2 || delivered != 2 {
		t.Fatal("acknowledgement lost or deleted an event", total, delivered, err)
	}
	if !bytes.Equal(content, telemetryOwnerRows(t, ctx, f.store, true)) {
		t.Fatal("lease or acknowledgement changed original event content")
	}
	t.Log("natural lease expiry advanced both fences once; stale and mixed acknowledgements rolled back; repeated new-owner ack preserved the complete rows")
}
