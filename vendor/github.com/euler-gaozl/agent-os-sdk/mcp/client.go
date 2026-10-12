package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var externalHTTP = &http.Client{Timeout: 90 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}

type MCPClient struct {
	Connector Connector
	Session   string
	HTTP      *http.Client
}

func validateEndpoint(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("explicit HTTP(S) endpoint without embedded credentials required")
	}
	return nil
}
func (m *MCPClient) RPC(ctx context.Context, method string, params any, notify bool) (json.RawMessage, error) {
	if e := validateEndpoint(m.Connector.Endpoint); e != nil {
		return nil, e
	}
	payload := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	if !notify {
		payload["id"] = 1
	}
	req, e := http.NewRequestWithContext(ctx, "POST", m.Connector.Endpoint, bytes.NewReader(JSON(payload)))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-11-25")
	if m.Session != "" {
		req.Header.Set("MCP-Session-Id", m.Session)
	}
	if m.Connector.KeyEnv != "" {
		if m.Connector.AuthToken == "" {
			return nil, errors.New("connector credential is not configured")
		}
		req.Header.Set("Authorization", "Bearer "+m.Connector.AuthToken)
	}
	client := m.HTTP
	if client == nil {
		client = externalHTTP
	}
	resp, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("MCP HTTP %d", resp.StatusCode)
	}
	if s := resp.Header.Get("MCP-Session-Id"); s != "" {
		m.Session = s
	}
	if notify {
		return nil, nil
	}
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > 4<<20 {
		return nil, errors.New("MCP response too large")
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		var found []byte
		scan := bufio.NewScanner(bytes.NewReader(raw))
		scan.Buffer(make([]byte, 4096), 4<<20)
		var data []string
		consume := func() {
			if len(data) == 0 {
				return
			}
			b := []byte(strings.Join(data, "\n"))
			var obj map[string]json.RawMessage
			if json.Unmarshal(b, &obj) == nil && string(obj["id"]) == "1" {
				found = b
			}
			data = nil
		}
		for scan.Scan() {
			line := scan.Text()
			if line == "" {
				consume()
			} else if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		consume()
		if e = scan.Err(); e != nil {
			return nil, e
		}
		if found == nil {
			return nil, errors.New("MCP stream ended without response")
		}
		raw = found
	}
	var msg struct {
		Version string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if e = json.Unmarshal(raw, &msg); e != nil {
		return nil, e
	}
	if msg.Version != "2.0" || string(msg.ID) != "1" {
		return nil, errors.New("MCP response version or request id mismatch")
	}
	if len(msg.Error) > 0 && string(msg.Error) != "null" {
		return nil, fmt.Errorf("MCP protocol error: %s", msg.Error)
	}
	if len(msg.Result) == 0 {
		return nil, errors.New("MCP missing result")
	}
	return msg.Result, nil
}
func (m *MCPClient) Initialize(ctx context.Context) error {
	if m.Connector.Protocol != "" && m.Connector.Protocol != "2025-11-25" {
		return errors.New("supported MCP profile is 2025-11-25 Streamable HTTP")
	}
	b, e := m.RPC(ctx, "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "agent-os", "version": "0.3.0"}}, false)
	if e != nil {
		return e
	}
	var reply struct {
		Version string `json:"protocolVersion"`
	}
	if e = json.Unmarshal(b, &reply); e != nil {
		return e
	}
	if reply.Version != "2025-11-25" {
		return errors.New("MCP server selected an unsupported version")
	}
	_, e = m.RPC(ctx, "notifications/initialized", map[string]any{}, true)
	return e
}
func DiscoverMCP(ctx context.Context, c Connector) ([]Tool, error) {
	m := MCPClient{Connector: c}
	if e := m.Initialize(ctx); e != nil {
		return nil, e
	}
	out := []Tool{}
	cursor := ""
	for page := 0; page < 20; page++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		b, e := m.RPC(ctx, "tools/list", params, false)
		if e != nil {
			return nil, e
		}
		var reply struct {
			Tools  []Tool `json:"tools"`
			Cursor string `json:"nextCursor"`
		}
		if e = json.Unmarshal(b, &reply); e != nil {
			return nil, e
		}
		out = append(out, reply.Tools...)
		cursor = reply.Cursor
		if cursor == "" {
			return out, nil
		}
	}
	return nil, errors.New("MCP tool pagination limit exceeded")
}
func ExecuteMCP(ctx context.Context, c Connector, name string, args json.RawMessage) ([]byte, error) {
	m := MCPClient{Connector: c}
	if e := m.Initialize(ctx); e != nil {
		return nil, e
	}
	return m.RPC(ctx, "tools/call", map[string]any{"name": name, "arguments": args}, false)
}
