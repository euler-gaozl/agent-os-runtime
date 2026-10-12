package main

import (
	"os"
	"testing"
)

func TestEntrypointDefaultsBeforeClientConstruction(t *testing.T) {
	expected := map[string]string{"RUNTIME_URL": "http://127.0.0.1:8094", "REGISTRY_URL": "http://127.0.0.1:8096", "TOOLHUB_URL": "http://127.0.0.1:8091", "STORAGE_URL": "http://127.0.0.1:8092", "SANDBOX_URL": "http://127.0.0.1:8093", "KNOWLEDGE_URL": "http://127.0.0.1:8095"}
	for key := range expected {
		t.Setenv(key, "")
	}
	if e := applyLocalDefaults(); e != nil {
		t.Fatal(e)
	}
	for key, want := range expected {
		if os.Getenv(key) != want {
			t.Fatal("missing original local default", key)
		}
	}
	for key := range expected {
		t.Setenv(key, "https://explicit.invalid/"+key)
	}
	if e := applyLocalDefaults(); e != nil {
		t.Fatal(e)
	}
	for key := range expected {
		if os.Getenv(key) != "https://explicit.invalid/"+key {
			t.Fatal("explicit configuration overwritten", key)
		}
	}
}
