package runtime

import (
	"context"
	"errors"
)

func (s *Store) Snapshot(ctx context.Context, ref Ref, depth int) (Snapshot, error) {
	out := Snapshot{AgentRef: ref, Children: map[string]Snapshot{}, Knowledge: map[string]KnowledgeBase{}}
	if depth > 2 {
		return out, errors.New("child agent binding depth exceeds 2")
	}
	if e := s.Resource(ctx, "agent", ref, &out.Agent); e != nil {
		return out, e
	}
	if e := out.Agent.Validate(); e != nil {
		return out, e
	}
	for _, r := range out.Agent.Prompts {
		var p Skill
		if e := s.Resource(ctx, "prompt", r, &p); e != nil {
			return out, e
		}
		out.Agent.Prompt += "\n\n" + p.Content
	}
	for _, r := range out.Agent.Skills {
		var v Skill
		if e := s.Resource(ctx, "skill", r, &v); e != nil {
			return out, e
		}
		out.Skills = append(out.Skills, v)
	}
	for _, r := range out.Agent.Connectors {
		var v Connector
		if e := s.Resource(ctx, "connector", r, &v); e != nil {
			return out, e
		}
		v.Name = r.ID
		out.Connectors = append(out.Connectors, v)
	}
	for _, r := range out.Agent.ChildAgents {
		v, e := s.Snapshot(ctx, r, depth+1)
		if e != nil {
			return out, e
		}
		out.Children[r.ID] = v
	}
	for _, r := range out.Agent.Knowledge {
		var kb KnowledgeBase
		if e := s.Resource(ctx, "knowledge", r, &kb); e != nil {
			return out, e
		}
		if e := kb.Validate(); e != nil {
			return out, e
		}
		out.Knowledge[r.ID] = kb
	}
	return out, nil
}
