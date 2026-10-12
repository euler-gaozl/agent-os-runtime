// Package execution is the versioned Worker persistence protocol. It contains
// no HTTP server, local tool execution, schema migration or decision loop.
package execution

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

const Protocol = "runtime-execution-v1"
const EngineDSHEvidence = runstate.EngineDSHEvidence

type Run = contracts.Run
type Message = contracts.Message
type Tool = contracts.Tool
type Frame = runstate.Frame
type Effects = runstate.Effects
type ArtifactWriter = runstate.ArtifactWriter
type Receipt = runstate.Receipt
type Child = runstate.Child
type Engine struct{ state *runstate.Store }

var ErrNoWork = errors.New("no eligible work")
var ErrWaitingChildren = runstate.ErrWaitingChildren

func New(db *pgxpool.Pool, archives ArtifactWriter) *Engine {
	return &Engine{state: &runstate.Store{DB: db, Archives: archives}}
}
func Readiness(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("database required")
	}
	return runstate.Readiness(ctx, db)
}
func (e *Engine) Claim(ctx context.Context, id string, lease time.Duration) (Run, error) {
	r, err := e.state.Claim(ctx, id, lease)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNoWork
	}
	return r, err
}
func (e *Engine) Run(ctx context.Context, id string) (Run, error) { return e.state.Run(ctx, id) }
func (e *Engine) Heartbeat(ctx context.Context, r Run, lease time.Duration) error {
	return e.state.Heartbeat(ctx, r, lease)
}
func (e *Engine) Save(ctx context.Context, r Run, status string) error {
	return e.state.Save(ctx, r, status)
}
func (e *Engine) CheckpointFailure(ctx context.Context, r Run, cause error) error {
	return e.state.CheckpointFailure(ctx, r, cause)
}
func (e *Engine) BeginModel(ctx context.Context, r Run, id string, request []byte) (string, []byte, error) {
	return e.state.BeginModel(ctx, r, id, request)
}
func (e *Engine) EndModel(ctx context.Context, r Run, id, attempt string, response []byte, cause error) error {
	return e.state.EndModel(ctx, r, id, attempt, response, cause)
}
func (e *Engine) ToolReceipt(ctx context.Context, runID, id string) (Receipt, error) {
	return e.state.ToolReceipt(ctx, runID, id)
}
func (e *Engine) Children(ctx context.Context, id string) ([]Child, error) {
	return e.state.Children(ctx, id)
}
func (e *Engine) Exchange(ctx context.Context, r *Run, f Frame, effects Effects) (any, bool, error) {
	if effects == nil {
		return nil, false, errors.New("execution effects required")
	}
	return e.state.Exchange(ctx, r, f, effects)
}
