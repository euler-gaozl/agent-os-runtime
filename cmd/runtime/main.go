package main

import (
	"context"
	runtimeapi "github.com/euler-gaozl/agent-os-runtime/runtime"
	"github.com/euler-gaozl/agent-os-sdk/servicehost"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"os"
)

func main() {
	servicehost.Main(servicehost.Options{Role: "runtime", Port: "8094", Database: true, Concurrency: 16, Authorize: runtimeapi.Authorize, Readiness: runtimeapi.Readiness}, func(ctx context.Context, db *pgxpool.Pool, c servicehttp.Config) (http.Handler, func(context.Context) error, error) {
		if e := applyLocalDefaults(); e != nil {
			return nil, nil, e
		}
		remote := servicehost.Client()
		s := runtimeapi.NewStore(db, &remote, os.Getenv("STORAGE_URL"))
		if err := s.ConfigureSubmissionEngine(os.Getenv("DEFAULT_RUN_ENGINE")); err != nil {
			return nil, nil, err
		}
		server := &runtimeapi.Server{Store: s, Token: os.Getenv("SERVICE_TOKEN"), RegistryURL: os.Getenv("REGISTRY_URL"), KnowledgeURL: os.Getenv("KNOWLEDGE_URL"), SandboxURL: os.Getenv("SANDBOX_URL")}
		if err := server.ConfigureReceiptExports(os.Getenv("APP_RECEIPT_EXPORTS")); err != nil {
			return nil, nil, err
		}
		return server.Handler(c), nil, nil
	})
}
