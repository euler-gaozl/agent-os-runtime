package serviceclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

func (c Client) PutArtifact(ctx context.Context, base string, body []byte, media, root string) (string, error) {
	if len(body) > 4<<20 {
		return "", errors.New("artifact exceeds 4 MiB")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	hash := contracts.Hash(body)
	req, e := http.NewRequestWithContext(ctx, "PUT", strings.TrimRight(base, "/")+"/v1/artifacts/"+hash, bytes.NewReader(body))
	if e != nil {
		return "", e
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Artifact-Media-Type", media)
	req.Header.Set("X-Artifact-Root", root)
	resp, e := servicehttp.Client.Do(req)
	if e != nil {
		return "", &servicehttp.StatusError{Status: 503, Message: "artifact store unavailable"}
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if e != nil {
		return "", e
	}
	if resp.StatusCode != 200 {
		return "", &servicehttp.StatusError{Status: resp.StatusCode, Message: "artifact store rejected upload"}
	}
	var receipt struct {
		Hash string `json:"hash"`
	}
	if len(raw) > 4096 || json.Unmarshal(raw, &receipt) != nil || receipt.Hash != hash {
		return "", errors.New("invalid artifact store receipt")
	}
	return hash, nil
}
