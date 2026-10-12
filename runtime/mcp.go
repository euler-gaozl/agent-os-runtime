package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/mcp"
	"github.com/euler-gaozl/agent-os-sdk/mcpsecurity"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"os"
	"strings"
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
	result, e := rpc.RPC(ctx, "tools/call", map[string]any{"name": name, "arguments": args}, false)
	if e != nil {
		return nil, errors.New("MCP execution failed; receipt requires reconciliation for effectful tools")
	}
	return result, nil
}
