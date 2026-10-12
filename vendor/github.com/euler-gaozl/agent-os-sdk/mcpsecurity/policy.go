package mcpsecurity

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type Policy struct{ AllowedHosts []string }

func (p Policy) allowed(host string) bool {
	for _, s := range p.AllowedHosts {
		if strings.EqualFold(strings.TrimSpace(s), host) {
			return true
		}
	}
	return false
}
func (p Policy) Endpoint(raw string) error {
	if e := contracts.ValidateEndpoint(raw); e != nil {
		return e
	}
	u, _ := url.Parse(raw)
	if u.RawQuery != "" {
		return errors.New("MCP URL query credentials are forbidden; use a credential reference")
	}
	if p.allowed(u.Host) {
		return nil
	}
	if u.Scheme != "https" {
		return errors.New("public MCP endpoints require HTTPS; private HTTP hosts require an explicit host:port allowlist")
	}
	if ip, e := netip.ParseAddr(u.Hostname()); e == nil && !publicIP(ip) {
		return errors.New("private, local and metadata addresses require an explicit allowlist")
	}
	return nil
}
func publicIP(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsValid() && a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() && !netip.MustParsePrefix("100.64.0.0/10").Contains(a) && !netip.MustParsePrefix("198.18.0.0/15").Contains(a)
}

// Resolve and validate at the actual dial, so a hostname cannot pass inspection
// with a public address then resolve to metadata/private networks on execution.
func (p Policy) Client() *http.Client {
	tr := &http.Transport{Proxy: nil, MaxResponseHeaderBytes: 32 << 10, MaxIdleConns: 16, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 8 * time.Second}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		d := net.Dialer{Timeout: 4 * time.Second}
		if p.allowed(address) {
			return d.DialContext(ctx, network, address)
		}
		ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil {
			return nil, e
		}
		if len(ips) == 0 {
			return nil, errors.New("host has no addresses")
		}
		for _, ip := range ips {
			if !publicIP(ip) {
				return nil, errors.New("MCP DNS resolved to a forbidden address")
			}
		}
		var last error
		for _, ip := range ips {
			c, e := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if e == nil {
				return c, nil
			}
			last = e
		}
		return nil, last
	}
	return &http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MCP redirects are forbidden") }}
}
