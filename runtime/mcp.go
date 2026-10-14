package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/mcp"
	"github.com/euler-gaozl/agent-os-sdk/mcpsecurity"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"net/http"
	"os"
	"strings"
	"time"
)

type MCPClient = mcp.MCPClient

var DiscoverMCP = mcp.DiscoverMCP
var ExecuteMCP = mcp.ExecuteMCP
var validateEndpoint = contracts.ValidateEndpoint
var externalHTTP = servicehttp.Client

func executeCheckedMCP(ctx context.Context, c Connector, name string, args json.RawMessage) ([]byte, error) {
	policy := mcpsecurity.Policy{AllowedHosts: strings.Split(os.Getenv("MCP_ALLOWED_HOSTS"), ",")}
	if e := policy.Endpoint(c.Endpoint); e != nil {
		return nil, e
	}
	client := policy.Client()
	defer client.CloseIdleConnections()
	rpc := mcp.MCPClient{Connector: c, HTTP: client}
	if e := rpc.Initialize(ctx); e != nil {
		return nil, errors.New("MCP handshake failed")
	}
	// Discovery/handshake use the short policy timeout. Execution must also
	// accommodate a bounded upstream fetch (Sources allows 30s) and its error
	// response; abandoning headers at 8s hid that result from the Agent.
	// Create a separate transport rather than mutate one already in use.
	executionClient := policy.Client()
	executionClient.Timeout = 40 * time.Second
	executionClient.Transport.(*http.Transport).ResponseHeaderTimeout = 35 * time.Second
	defer executionClient.CloseIdleConnections()
	rpc.HTTP = executionClient
	result, e := rpc.RPC(ctx, "tools/call", map[string]any{"name": name, "arguments": args}, false)
	if e != nil {
		return nil, errors.New("MCP execution failed; receipt requires reconciliation for effectful tools")
	}
	return result, nil
}
