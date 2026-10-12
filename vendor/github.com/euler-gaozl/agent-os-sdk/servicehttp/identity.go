package servicehttp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
)

type identityKey struct{}

func Caller(ctx context.Context) string { v, _ := ctx.Value(identityKey{}).(string); return v }
func TokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// Peers contain hashes only. A service cannot impersonate another service by
// reading its inbound authorization configuration.
func peer(c Config, r *http.Request) string {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if len(token) < 32 {
		return ""
	}
	hash := TokenHash(token)
	for role, expected := range c.Peers {
		if hash == expected {
			return role
		}
	}
	return ""
}

// Duplicate hashes make caller identity ambiguous and must fail at startup.
func ValidatePeers(peers map[string]string) error {
	seen := map[string]bool{}
	for name, hash := range peers {
		bytes, err := hex.DecodeString(hash)
		if name == "" || err != nil || len(bytes) != sha256.Size || hex.EncodeToString(bytes) != hash || seen[hash] {
			return errors.New("invalid or ambiguous service identity configuration")
		}
		seen[hash] = true
	}
	return nil
}
