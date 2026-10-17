package runtime

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/euler-gaozl/agent-os-sdk/serviceclient"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	*runstate.Store
	CatalogURL, StorageURL string
	Remote                 *serviceclient.Client
	submissionEngine       string
}

func NewStore(db *pgxpool.Pool, remote *serviceclient.Client, storageURL string) *Store {
	s := &Store{Store: &runstate.Store{DB: db}, Remote: remote, StorageURL: storageURL}
	if remote != nil {
		s.CatalogURL = remote.CatalogURL
	}
	s.Store.Archives = artifactWriter{s}
	return s
}

type artifactWriter struct{ s *Store }

func (a artifactWriter) PutArtifact(ctx context.Context, b []byte, media, root string) (string, error) {
	if a.s.Remote == nil || a.s.StorageURL == "" {
		return "", errors.New("artifact service required")
	}
	return a.s.Remote.PutArtifact(ctx, a.s.StorageURL, b, media, root)
}
func (s *Store) Resource(ctx context.Context, kind string, ref Ref, dst any) error {
	if s.Remote == nil {
		return errors.New("registry/catalog client required")
	}
	return s.Remote.Resource(ctx, kind, ref, dst)
}
func (s *Store) Secret(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", nil
	}
	if s.Remote == nil {
		return "", errors.New("credential service required")
	}
	return s.Remote.Secret(ctx, name)
}

var event = runstate.EmitEvent
var lockRun = runstate.LockRun
var insertRun = runstate.InsertRun
var scanRun = runstate.ScanRun
var lockFamily = runstate.LockFamily

const runColumns = runstate.RunColumns
const EngineDSHEvidence = runstate.EngineDSHEvidence
const EngineDSHSource = runstate.EngineDSHSource
const EngineDSHResident = runstate.EngineDSHResident
const EngineDSHBudget = runstate.EngineDSHBudget
const EngineDSHNative = runstate.EngineDSHNative
const EngineDSHFinal = runstate.EngineDSHFinal
const EngineDSHGuarded = runstate.EngineDSHGuarded

func isSourceEngine(engine string) bool { return runstate.IsSourceEngine(engine) }
