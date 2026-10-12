package serviceclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

type Client struct{ RegistryURL, RuntimeURL, CatalogURL, Token string }

func (c Client) Request(ctx context.Context, method, endpoint string, in any, out any) error {
	var body io.Reader
	if in != nil {
		body = bytes.NewReader(contracts.JSON(in))
	}
	req, e := http.NewRequestWithContext(ctx, method, endpoint, body)
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := servicehttp.Client.Do(req)
	if e != nil {
		return &servicehttp.StatusError{Status: 503, Message: "upstream service unavailable"}
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if e != nil {
		return e
	}
	if len(raw) > 4<<20 {
		return errors.New("upstream response exceeds limit")
	}
	if resp.StatusCode == 409 {
		return contracts.ErrUnknown
	}
	if resp.StatusCode == 412 {
		return contracts.ErrFence
	}
	if resp.StatusCode >= 400 {
		return &servicehttp.StatusError{Status: resp.StatusCode, Message: "upstream request rejected"}
	}
	if out == nil {
		return nil
	}
	if p, ok := out.(*json.RawMessage); ok {
		*p = append((*p)[:0], raw...)
		return nil
	}
	return json.Unmarshal(raw, out)
}
func (c Client) Resource(ctx context.Context, kind string, ref contracts.Ref, out any) error {
	base := c.RegistryURL + "/v1/resources/"
	if kind == "skill" || kind == "connector" {
		base = c.CatalogURL + "/v1/resolve/"
	}
	var resource contracts.Resource
	if e := c.Request(ctx, "GET", fmt.Sprintf("%s%s/%s/%d", base, url.PathEscape(kind), url.PathEscape(ref.ID), ref.Revision), nil, &resource); e != nil {
		return e
	}
	return json.Unmarshal(resource.Spec, out)
}
func (c Client) Secret(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", nil
	}
	var result struct {
		Value string `json:"value"`
	}
	e := c.Request(ctx, "POST", c.RegistryURL+"/v1/credentials/resolve", map[string]string{"name": name}, &result)
	return result.Value, e
}
func (c Client) Run(ctx context.Context, id string) (contracts.Run, error) {
	var run contracts.Run
	e := c.Request(ctx, "GET", c.RuntimeURL+"/v1/runs/"+url.PathEscape(id), nil, &run)
	return run, e
}
func (c Client) Invoke(ctx context.Context, inv contracts.Invocation) ([]byte, error) {
	var raw json.RawMessage
	e := c.Request(ctx, "POST", c.RuntimeURL+"/invoke", inv, &raw)
	return raw, e
}
