package runtime

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"strings"
)

func putDocument(ctx context.Context, tx pgx.Tx, d Document) (Document, error) {
	if !Has([]string{"knowledge", "memory", "evidence", "fact"}, d.Kind) || strings.TrimSpace(d.Title) == "" || strings.TrimSpace(d.Content) == "" || strings.TrimSpace(d.Source) == "" {
		return d, errors.New("kind, title, content and provenance source required")
	}
	if len(d.Content) > 1000000 {
		return d, errors.New("document exceeds 1 MB")
	}
	if d.ID == "" {
		d.ID = ID()
	}
	if d.Metadata == nil {
		d.Metadata = map[string]any{}
	}
	if d.Supersedes != nil {
		var previousKind, previousScope string
		if e := tx.QueryRow(ctx, "SELECT kind,scope FROM documents WHERE id=$1", *d.Supersedes).Scan(&previousKind, &previousScope); e != nil {
			return d, e
		}
		if previousKind != d.Kind || previousScope != d.Scope {
			return d, errors.New("revisions must preserve document kind and scope")
		}
	}
	_, e := tx.Exec(ctx, "INSERT INTO documents(id,kind,title,content,source,metadata,supersedes,scope,valid_until) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)", d.ID, d.Kind, d.Title, d.Content, d.Source, JSON(d.Metadata), d.Supersedes, d.Scope, d.ValidUntil)
	return d, e
}

var searchDocuments = searchNotes

func (s *Store) Search(ctx context.Context, q, kind string) ([]Document, error) {
	return searchNotes(ctx, s.DB, q, kind)
}
func (s *Store) PutDocument(ctx context.Context, d Document) (Document, error) {
	return s.putAdminDocument(ctx, d, "runtime-internal")
}

type artifactPageRequest struct {
	Hash      string   `json:"hash"`
	Offset    int      `json:"offset"`
	Limit     int      `json:"limit"`
	Roots     []string `json:"roots"`
	AdoptRoot string   `json:"adopt_root"`
}
