// Package servicehttp supplies bounded HTTP transport, admission control and
// operational endpoints. It contains no application repositories or SQL.
package servicehttp

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

var Client = &http.Client{Timeout: 90 * time.Second, Transport: &http.Transport{
	Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	MaxIdleConns: 32, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 8,
	IdleConnTimeout: 60 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 60 * time.Second,
}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirects are disabled") }}

type Config struct {
	Role, Token, AdminToken string
	Peers                   map[string]string
	DevLocal                bool
	MaxConcurrent           int
	RequestTimeout          time.Duration
	Readiness               func(context.Context) error
	// Policies belong to the service, not this transport package. Missing peer
	// policy denies every authenticated request, including operational endpoints.
	Authorize  func(caller, method, path string) bool
	UserFacing bool
	PublicPath func(*http.Request) bool
}
type StatusError struct {
	Status  int
	Message string
}

func (e *StatusError) Error() string { return e.Message }
func JSONResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func Decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return &StatusError{400, "invalid or oversized JSON body"}
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return &StatusError{400, "expected exactly one JSON value"}
	}
	return nil
}
func Wrap(h func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if e := h(w, r); e != nil {
			status := 400
			message := e.Error()
			var pg *pgconn.PgError
			var connect *pgconn.ConnectError
			var network net.Error
			if errors.As(e, &pg) {
				message = "database operation rejected"
				if strings.HasPrefix(pg.Code, "08") || strings.HasPrefix(pg.Code, "53") || pg.Code == "57P01" || pg.Code == "57P02" || pg.Code == "57P03" {
					status, message = 503, "database temporarily unavailable"
				} else if pg.Code == "57014" {
					status, message = 504, "database operation timed out"
				} else if pg.Code == "P4090" {
					status, message = 409, "resource state conflict"
				}
			} else if errors.Is(e, io.ErrUnexpectedEOF) || errors.Is(e, pgconn.ErrConnClosed) || errors.As(e, &connect) || errors.As(e, &network) {
				status, message = 503, "upstream temporarily unavailable"
			}
			if errors.Is(e, contracts.ErrFence) {
				status = 412
			}
			if errors.Is(e, contracts.ErrUnknown) {
				status = 409
			}
			var se *StatusError
			if errors.As(e, &se) {
				status = se.Status
				message = se.Message
			}
			if errors.Is(e, context.DeadlineExceeded) {
				status = 504
				message = "operation timed out"
			}
			if status == 503 {
				w.Header().Set("Retry-After", "1")
			}
			JSONResponse(w, status, map[string]string{"error": message})
		}
	}
}
func Truncate(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}
func authorized(c Config, r *http.Request) bool {
	expected := c.Token
	if c.UserFacing {
		expected = c.AdminToken
	}
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(expected) >= 32 && subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) == 1 {
		return true
	}
	if !c.DevLocal || !c.UserFacing {
		return false
	}
	peer, _, _ := net.SplitHostPort(r.RemoteAddr)
	host, _, e := net.SplitHostPort(r.Host)
	if e != nil {
		host = r.Host
	}
	ip := net.ParseIP(peer)
	origin := r.Header.Get("Origin")
	ok := ip != nil && ip.IsLoopback() && (host == "127.0.0.1" || host == "localhost" || host == "::1" || host == "[::1]") && (origin == "" || origin == "http://"+r.Host)
	return ok && (r.Method == "GET" || r.Method == "HEAD" || r.Header.Get("X-AgentOS-Client") == "web")
}
func New(c Config, ready func(context.Context) error, routes func(*http.ServeMux)) http.Handler {
	if c.Readiness != nil {
		ready = c.Readiness
	}
	if c.MaxConcurrent < 1 {
		c.MaxConcurrent = 16
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 95 * time.Second
	}
	mux := http.NewServeMux()
	routes(mux)
	slots := make(chan struct{}, c.MaxConcurrent)
	var requests, rejected, failures, inflight atomic.Int64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/healthz" {
			JSONResponse(w, 200, map[string]string{"role": c.Role, "status": "alive"})
			return
		}
		static := c.UserFacing && c.PublicPath != nil && r.URL.Path != "/metrics" && r.URL.Path != "/readyz" && c.PublicPath(r)
		if !static && len(c.Peers) > 0 && !c.UserFacing {
			caller := peer(c, r)
			if caller == "" {
				JSONResponse(w, 401, map[string]string{"error": "authentication required"})
				return
			}
			if c.Authorize == nil || !c.Authorize(caller, r.Method, r.URL.Path) {
				JSONResponse(w, 403, map[string]string{"error": "service identity is not authorized for this operation"})
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), identityKey{}, caller))
		} else if !static && !authorized(c, r) {
			JSONResponse(w, 401, map[string]string{"error": "authentication required"})
			return
		}
		if r.URL.Path == "/readyz" {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if ready != nil {
				if e := ready(ctx); e != nil {
					JSONResponse(w, 503, map[string]bool{"ready": false})
					return
				}
			}
			JSONResponse(w, 200, map[string]bool{"ready": true})
			return
		}
		if r.URL.Path == "/metrics" {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			fmt.Fprintf(w, "agentos_http_requests_total %d\nagentos_http_rejected_total %d\nagentos_http_panics_total %d\nagentos_http_inflight %d\nagentos_go_heap_bytes %d\nagentos_go_goroutines %d\n", requests.Load(), rejected.Load(), failures.Load(), inflight.Load(), m.HeapAlloc, runtime.NumGoroutine())
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			rejected.Add(1)
			w.Header().Set("Retry-After", "1")
			JSONResponse(w, 429, map[string]string{"error": "service at capacity"})
			return
		}
		requests.Add(1)
		inflight.Add(1)
		defer inflight.Add(-1)
		var bytes [12]byte
		_, _ = rand.Read(bytes[:])
		id := hex.EncodeToString(bytes[:])
		w.Header().Set("X-Request-ID", id)
		defer func() {
			if v := recover(); v != nil {
				failures.Add(1)
				slog.Error("HTTP handler panic", "service", c.Role, "request_id", id)
				JSONResponse(w, 500, map[string]string{"error": "internal service error", "request_id": id})
			}
		}()
		ctx, cancel := context.WithTimeout(r.Context(), c.RequestTimeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
