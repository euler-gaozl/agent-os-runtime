package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the production DNS/endpoint policy and JSON-RPC transport, not a
// replacement execute callback returning a fabricated successful receipt.
func mcpExecutionPeer(t *testing.T, call http.HandlerFunc) Connector {
	t.Helper()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Method string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case "initialize":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25"}}`))
		case "notifications/initialized":
			w.WriteHeader(202)
		case "tools/call":
			call(w, r)
		default:
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(peer.Close)
	t.Setenv("MCP_ALLOWED_HOSTS", strings.TrimPrefix(peer.URL, "http://"))
	return Connector{Name: "slow", Endpoint: peer.URL, ReadOnly: true, Tools: []Tool{tool("fetch", "fixture fetch", `{"type":"object","additionalProperties":false}`)}}
}

func TestCheckedMCPDelayedErrorIsRetainedAndReplayed(t *testing.T) {
	s, _ := testStore(t)
	var requests atomic.Int32
	c := mcpExecutionPeer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		// Sources must be able to return its own timeout error after a slow
		// upstream. Previously Runtime abandoned response headers at 8s.
		select {
		case <-time.After(8500 * time.Millisecond):
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[{"type":"text","text":"source connection failed; no capture committed"}]}}`))
		case <-r.Context().Done():
		}
	})
	snap := basicSnapshot("https://fixture.invalid")
	snap.Connectors = []Connector{c}
	submit(t, s, snap)
	r, err := s.Claim(context.Background(), "mcp-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inv := invocation(r)
	inv.Name, inv.Arguments = "mcp_slow_fetch", json.RawMessage(`{}`)
	execute := func(ctx context.Context, _ Run) ([]byte, error) {
		return executeCheckedMCP(ctx, c, "fetch", inv.Arguments)
	}
	first, err := s.ExternalTool(context.Background(), inv, execute)
	if err != nil || !strings.Contains(string(first), `"isError":true`) {
		t.Fatal("tool result was converted into a fatal transport failure", string(first), err)
	}
	again, err := s.ExternalTool(context.Background(), inv, execute)
	if err != nil || canonicalHash(first) != canonicalHash(again) || requests.Load() != 1 {
		t.Fatal("recorded error was not replayed", err, requests.Load())
	}
	var status string
	if err = s.DB.QueryRow(context.Background(), "SELECT status FROM calls WHERE id=$1", inv.OperationID).Scan(&status); err != nil || status != "completed" {
		t.Fatal("response was not durably acknowledged", status, err)
	}
}

func TestCheckedMCPTransportFailureKeepsEffectUncertainty(t *testing.T) {
	for _, readonly := range []bool{true, false} {
		t.Run(map[bool]string{true: "read-only", false: "effectful"}[readonly], func(t *testing.T) {
			s, _ := testStore(t)
			var requests atomic.Int32
			c := mcpExecutionPeer(t, func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(502)
			})
			c.ReadOnly = readonly
			snap := basicSnapshot("https://fixture.invalid")
			snap.Connectors = []Connector{c}
			submit(t, s, snap)
			r := claim(t, s, "mcp-test")
			inv := invocation(r)
			inv.Name, inv.Arguments = "mcp_slow_fetch", json.RawMessage(`{}`)
			execute := func(ctx context.Context, _ Run) ([]byte, error) {
				return executeCheckedMCP(ctx, c, "fetch", inv.Arguments)
			}
			for i := 0; i < 2; i++ {
				if _, err := s.ExternalTool(context.Background(), inv, execute); err == nil || errors.Is(err, ErrUnknown) == readonly {
					t.Fatal("transport failure changed uncertainty semantics", err)
				}
			}
			want, count := "unknown", int32(1)
			if readonly {
				want, count = "retryable", 2
			}
			var status string
			if err := s.DB.QueryRow(context.Background(), "SELECT status FROM calls WHERE id=$1", inv.OperationID).Scan(&status); err != nil || status != want || requests.Load() != count {
				t.Fatal(status, requests.Load(), err)
			}
		})
	}
}

func TestCheckedMCPExecutionRespectsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := mcpExecutionPeer(t, func(_ http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	})
	start := time.Now()
	if _, err := executeCheckedMCP(ctx, c, "fetch", json.RawMessage(`{}`)); err == nil {
		t.Fatal("cancelled call succeeded")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("execution timeout overrode caller cancellation")
	}
	t.Setenv("MCP_ALLOWED_HOSTS", "")
	if _, err := executeCheckedMCP(context.Background(), c, "fetch", json.RawMessage(`{}`)); err == nil {
		t.Fatal("private endpoint accepted without operator allowlist")
	}
}
