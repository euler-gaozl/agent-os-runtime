package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProtectedMigrationConfig(t *testing.T) {
	good := `{"MIGRATION_DATABASE_URL":"postgres://owner:fixture@127.0.0.1/db","ROLE":"runtime","WORKER_ROLE":"worker"}`
	path := filepath.Join(t.TempDir(), "migration.json")
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", good, true},
		{"missing role", `{"MIGRATION_DATABASE_URL":"fixture","ROLE":"runtime"}`, false},
		{"duplicate key", strings.Replace(good, `"ROLE":"runtime"`, `"ROLE":"one","ROLE":"runtime"`, 1), false},
		{"case alias", strings.Replace(good, `"ROLE"`, `"role"`, 1), false},
		{"null credential", strings.Replace(good, `"postgres://owner:fixture@127.0.0.1/db"`, `null`, 1), false},
		{"same identity", strings.Replace(good, `"worker"`, `"runtime"`, 1), false},
		{"unexpected key", strings.Replace(good, `"ROLE"`, `"UNEXPECTED"`, 1), false},
		{"trailing object", good + `{}`, false},
		{"array", `[` + good + `]`, false},
		{"oversize", good + strings.Repeat(" ", 65537), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := readMigrationConfig(path)
			if (err == nil) != tc.valid {
				t.Fatal("unexpected configuration acceptance", err)
			}
			if tc.valid && (values["ROLE"] != "runtime" || values["WORKER_ROLE"] != "worker") {
				t.Fatal("configuration identities changed")
			}
			if err != nil && strings.Contains(err.Error(), "postgres://") {
				t.Fatal("error exposed connection")
			}
		})
	}
}
