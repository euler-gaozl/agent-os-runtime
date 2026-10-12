package servicehost

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
func Load() error {
	config := flag.String("config", "", "service configuration JSON file (never committed)")
	flag.Parse()
	if *config == "" {
		return nil
	}
	raw, e := os.ReadFile(*config)
	if e != nil {
		return e
	}
	var values map[string]string
	if e = json.Unmarshal(raw, &values); e != nil {
		return e
	}
	for k, v := range values {
		if os.Getenv(k) == "" {
			if e = os.Setenv(k, v); e != nil {
				return e
			}
		}
	}
	return nil
}
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil || dsn == "" {
		return nil, errors.New("database connection configuration required")
	}
	n, e := strconv.Atoi(Env("DB_MAX_CONNS", "3"))
	if e != nil || n < 2 || n > 16 {
		return nil, errors.New("DB_MAX_CONNS must be 2..16")
	}
	cfg.MaxConns = int32(n)
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 2 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "15000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "15000"
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, errors.New("cannot configure database pool")
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, errors.New("database connection unavailable")
	}
	return p, nil
}
func Client() serviceclient.Client {
	return serviceclient.Client{RegistryURL: os.Getenv("REGISTRY_URL"), RuntimeURL: os.Getenv("RUNTIME_URL"), CatalogURL: os.Getenv("TOOLHUB_URL"), Token: os.Getenv("SERVICE_TOKEN")}
}

type Build func(context.Context, *pgxpool.Pool, servicehttp.Config) (http.Handler, func(context.Context) error, error)

// Options are owned by the entrypoint. The SDK has no role-to-schema, route
// permission, default address or migration map.
type Options struct {
	Role, Port    string
	Database      bool
	Concurrency   int
	UserFacing    bool
	AdminTokenEnv string
	Readiness     func(context.Context, *pgxpool.Pool) error
	Authorize     func(caller, method, path string) bool
	PublicPath    func(*http.Request) bool
}

func (o Options) validate() error {
	if o.Role == "" || o.Port == "" || o.Concurrency < 1 || o.Concurrency > 64 {
		return errors.New("role, port and bounded default concurrency required")
	}
	if o.Database && o.Readiness == nil {
		return errors.New("database service requires its own schema readiness check")
	}
	if !o.UserFacing && o.Authorize == nil {
		return errors.New("internal service requires its own authorization policy")
	}
	return nil
}

func Main(options Options, build Build) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if e := run(options, build); e != nil && !errors.Is(e, context.Canceled) {
		slog.Error("service stopped", "role", options.Role, "error", e)
		os.Exit(1)
	}
}
func run(options Options, build Build) error {
	if e := options.validate(); e != nil {
		return e
	}
	role, port := options.Role, options.Port
	if e := Load(); e != nil {
		return e
	}
	limit, e := memoryLimit(Env("GOMEMLIMIT", "96MiB"))
	if e != nil {
		return e
	}
	debug.SetMemoryLimit(limit)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if os.Getenv("INSTANCE_ID") == "" {
		host, _ := os.Hostname()
		_ = os.Setenv("INSTANCE_ID", role+"-"+host+"-"+contracts.ID()[:8])
	}
	token := os.Getenv("SERVICE_TOKEN")
	if len(token) < 32 {
		return errors.New("generated SERVICE_TOKEN required")
	}
	var db *pgxpool.Pool
	if options.Database {
		db, e = Open(ctx, os.Getenv("DATABASE_URL"))
		if e != nil {
			return e
		}
		defer db.Close()
	}
	cfg := servicehttp.Config{Role: role, Token: token, AdminToken: Env("ADMIN_TOKEN", token), DevLocal: os.Getenv("DEV_LOCAL") == "1", Authorize: options.Authorize, UserFacing: options.UserFacing, PublicPath: options.PublicPath}
	if options.Readiness != nil {
		cfg.Readiness = func(ctx context.Context) error { return options.Readiness(ctx, db) }
	}
	if cfg.Readiness != nil {
		probeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		err := cfg.Readiness(probeCtx)
		stop()
		if err != nil {
			return errors.New("service schema or database permissions incompatible; run the selected service migration before starting")
		}
	}
	defaultConcurrency := strconv.Itoa(options.Concurrency)
	concurrency, err := strconv.Atoi(Env("HTTP_MAX_CONCURRENT", defaultConcurrency))
	if err != nil || concurrency < 1 || concurrency > 64 {
		return errors.New("HTTP_MAX_CONCURRENT must be 1..64")
	}
	cfg.MaxConcurrent = concurrency
	if raw := os.Getenv("SERVICE_PEERS"); raw != "" {
		if e = json.Unmarshal([]byte(raw), &cfg.Peers); e != nil {
			return errors.New("invalid service identity policy")
		}
	}
	if cfg.Peers[role] != servicehttp.TokenHash(token) {
		return errors.New("service identity configuration missing or inconsistent")
	}
	if err := servicehttp.ValidatePeers(cfg.Peers); err != nil {
		return err
	}
	if options.AdminTokenEnv != "" {
		cfg.AdminToken = Env(options.AdminTokenEnv, cfg.AdminToken)
	}
	h, bg, e := build(ctx, db, cfg)
	if e != nil {
		return e
	}
	address := Env("LISTEN", "127.0.0.1:"+port)
	if cfg.DevLocal && cfg.UserFacing && address != "127.0.0.1:"+port {
		return errors.New("local unauthenticated UI must bind exact loopback address")
	}
	server := &http.Server{Addr: address, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 100 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 16384}
	background := make(chan error, 2)
	if bg != nil {
		go func() { background <- bg(ctx) }()
	}
	go heartbeat(ctx, role)
	go func() { background <- server.ListenAndServe() }()
	slog.Info("service started", "role", role, "listen", address)
	select {
	case <-ctx.Done():
	case e = <-background:
		cancel()
	}
	shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
	}
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func memoryLimit(value string) (int64, error) {
	for _, unit := range []struct {
		suffix string
		factor int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}} {
		if strings.HasSuffix(value, unit.suffix) {
			n, e := strconv.ParseInt(strings.TrimSuffix(value, unit.suffix), 10, 64)
			if e == nil && n > 0 && n <= (1<<62)/unit.factor {
				return n * unit.factor, nil
			}
			break
		}
	}
	return 0, errors.New("GOMEMLIMIT must be a positive binary size, e.g. 96MiB")
}
func heartbeat(ctx context.Context, role string) {
	client := Client()
	if client.RuntimeURL == "" {
		return
	}
	host, _ := os.Hostname()
	id := Env("INSTANCE_ID", role+"-"+contracts.ID()[:8])
	body := map[string]any{"id": id, "role": role, "hostname": host, "pid": os.Getpid()}
	tick := time.NewTicker(4 * time.Second)
	defer tick.Stop()
	for {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = client.Request(call, "POST", client.RuntimeURL+"/v1/instances", body, nil)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func CheckSchema(ctx context.Context, p *pgxpool.Pool, table string) error {
	var exists bool
	if e := p.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); e != nil {
		return e
	}
	if !exists {
		return fmt.Errorf("service schema missing: run its migration before starting")
	}
	return nil
}
