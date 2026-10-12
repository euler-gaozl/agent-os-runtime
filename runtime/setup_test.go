package runtime

import (
	"context"
	"testing"
)

const testToken = "test-only-service-token-32-characters-long"

func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	db, role, worker := fixtureDatabase(t)
	if e := Migrate(context.Background(), db, role, worker); e != nil {
		t.Fatal(e)
	}
	return NewStore(db, nil, ""), ""
}
func isolatedDomainDatabase(t *testing.T, domain string) *Store {
	t.Helper()
	if domain != "runtime" {
		t.Fatal("foreign fixture requested")
	}
	s, _ := testStore(t)
	return s
}
