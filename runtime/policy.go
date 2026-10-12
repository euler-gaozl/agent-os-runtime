package runtime

import "strings"

// Authorize preserves the per-service HTTP boundary; owning a Worker token
// never permits bypassing ToolHub governance through direct Runtime invocation.
func Authorize(caller, method, path string) bool {
	if path == "/readyz" || path == "/metrics" {
		return caller == "runtime" || caller == "control"
	}
	if path == "/v1/instances" && method == "POST" {
		return caller != ""
	}
	get := method == "GET"
	switch caller {
	case "telemetry":
		return method == "POST" && (path == "/v1/telemetry/claim" || path == "/v1/telemetry/ack")
	case "sandbox":
		return method == "POST" && path == "/v1/sandbox/authorize"
	case "control":
		return strings.HasPrefix(path, "/v1/runs") || path == "/v1/status" ||
			(get && (path == "/v1/notes" || strings.HasPrefix(path, "/v1/notes/"))) ||
			(method == "POST" && (path == "/v1/notes/revise" || path == "/v1/notes/retire")) ||
			((get || method == "POST") && path == "/v1/documents") || (get && strings.HasPrefix(path, "/v1/documents/"))
	case "futures":
		return path == "/v1/apps/futures/runs" || strings.HasPrefix(path, "/v1/apps/futures/runs/")
	case "toolhub":
		return (get && strings.HasPrefix(path, "/v1/runs/")) || (method == "POST" && path == "/invoke")
	case "knowledge":
		return get && strings.HasPrefix(path, "/v1/runs/")
	}
	return false
}
