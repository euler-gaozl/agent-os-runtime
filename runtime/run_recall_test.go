package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestRunRecallScopesPagesAndReplaysItsOriginalView(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	snap := basicSnapshot("https://fixture.invalid")
	snap.Agent.Tools = append(snap.Agent.Tools, "run_recall")
	snap.Children = map[string]Snapshot{"child": snap}
	submit(t, s, snap)
	parent := claim(t, s, "parent")
	invoke := func(r Run, id, name string, args any) ([]byte, error) {
		return s.LocalTool(ctx, Invocation{RunID: r.ID, Owner: r.Owner, Fence: r.Fence, RuleRevision: r.RuleRevision, OperationID: r.ID + "/" + id, Name: name, Arguments: JSON(args)})
	}
	for i := 0; i < 12; i++ {
		if _, e := invoke(parent, fmt.Sprintf("note/%02d", i), "note_save", map[string]any{"kind": "evidence", "title": "库存修订", "content": strings.Repeat("计划与实绩应区分；", 200), "source": "fixture://source"}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := invoke(parent, "spawn", "spawn_agent", map[string]string{"agent_id": "child", "task": "controlled child evidence"}); e != nil {
		t.Fatal(e)
	}
	child := claim(t, s, "child")
	if _, e := invoke(child, "child-note", "note_save", map[string]string{"kind": "fact", "title": "literal", "content": "100% literal value", "source": "fixture://child"}); e != nil {
		t.Fatal(e)
	}
	if _, e := invoke(child, "no-parent", "run_recall", map[string]string{"operation_id": parent.ID + "/note/00"}); e == nil {
		t.Fatal("child read parent receipt")
	}
	submit(t, s, snap)
	outsider := claim(t, s, "outsider")
	if _, e := invoke(outsider, "no-child", "run_recall", map[string]string{"operation_id": child.ID + "/child-note"}); e == nil {
		t.Fatal("unrelated run read child evidence")
	}
	first, e := invoke(parent, "list-1", "run_recall", map[string]string{})
	if e != nil {
		t.Fatal(e)
	}
	var page struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
		More  bool             `json:"has_more"`
	}
	if e = json.Unmarshal(first, &page); e != nil || len(page.Items) != 10 || !page.More || page.Next == "" {
		t.Fatal("unbounded or broken listing", e)
	}
	second, e := invoke(parent, "list-2", "run_recall", map[string]string{"after": page.Next})
	if e != nil {
		t.Fatal(e)
	}
	var tail struct {
		Items []map[string]any `json:"items"`
		More  bool             `json:"has_more"`
	}
	if e = json.Unmarshal(second, &tail); e != nil || len(tail.Items) != 4 || tail.More {
		t.Fatal("pagination missed or included recall calls", e, len(tail.Items))
	}
	literal, e := invoke(parent, "search-literal", "run_recall", map[string]string{"query": "%"})
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(literal, &tail); e != nil || len(tail.Items) != 1 || tail.Items[0]["id"] != child.ID+"/child-note" {
		t.Fatal("wildcard widened evidence search", e)
	}
	// Read the large JSON response in Unicode pages; no full-body transport is
	// needed merely to discover the receipt or locate its original document.
	var text strings.Builder
	for offset := 0; ; {
		b, e := invoke(parent, fmt.Sprintf("read/%d", offset), "run_recall", map[string]any{"operation_id": parent.ID + "/note/00", "offset": offset, "limit": 333})
		if e != nil {
			t.Fatal(e)
		}
		var p struct {
			Content string
			Next    int  `json:"next_offset"`
			More    bool `json:"has_more"`
		}
		if e = json.Unmarshal(b, &p); e != nil {
			t.Fatal(e)
		}
		if len([]rune(p.Content)) > 333 || p.Next <= offset {
			t.Fatal("invalid Unicode page")
		}
		text.WriteString(p.Content)
		if !p.More {
			break
		}
		offset = p.Next
	}
	var d Document
	if e = json.Unmarshal([]byte(text.String()), &d); e != nil || d.Content != strings.Repeat("计划与实绩应区分；", 200) {
		t.Fatal("retained receipt truncated/corrupted", e)
	}
	if _, e := invoke(parent, "fresh-note", "note_save", map[string]string{"kind": "memory", "title": "later", "content": "later committed evidence", "source": "fixture://later"}); e != nil {
		t.Fatal(e)
	}
	cached, e := invoke(parent, "list-1", "run_recall", map[string]string{})
	if e != nil || canonicalHash(cached) != canonicalHash(first) {
		t.Fatal("replay changed original evidence view", e)
	}
}
