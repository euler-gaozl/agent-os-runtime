package runstate

import "github.com/euler-gaozl/agent-os-sdk/contracts"

type Ref = contracts.Ref
type ModelConfig = contracts.ModelConfig
type Tool = contracts.Tool
type Connector = contracts.Connector
type Skill = contracts.Skill
type Agent = contracts.Agent
type Snapshot = contracts.Snapshot
type FunctionCall = contracts.FunctionCall
type ToolCall = contracts.ToolCall
type Message = contracts.Message
type RunState = contracts.RunState
type Run = contracts.Run
type Document = contracts.Document
type Resource = contracts.Resource
type Event = contracts.Event
type Invocation = contracts.Invocation
type Publication = contracts.Publication
type KnowledgeBase = contracts.KnowledgeBase
type KnowledgeFile = contracts.KnowledgeFile
type Retrieval = contracts.Retrieval

var (
	ErrFence   = contracts.ErrFence
	ErrUnknown = contracts.ErrUnknown
	ID         = contracts.ID
	JSON       = contracts.JSON
	Hash       = contracts.Hash
	Has        = contracts.Has
)
