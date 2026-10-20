package runtime

import "strings"

// Authorize preserves the per-service HTTP boundary; owning a Worker token
// never permits bypassing ToolHub governance through direct Runtime invocation.
func Authorize(caller, method, path string) bool {
	// URL.Path is already decoded by the identity middleware. An escaped slash
	// inside ServeMux's {id} must not evade this guard and reach peer run grants.
	if strings.HasSuffix(path, "/continuation") || strings.Contains(path, "/continuation/") {
		if caller != "control" || !strings.HasPrefix(path, "/v1/runs/") {
			return false
		}
		if strings.HasSuffix(path, "/continuation") {
			return method == "GET" || method == "POST"
		}
		if strings.HasSuffix(path, "/continuation/request-context") {
			return method == "GET"
		}
		if at := strings.LastIndex(path, "/continuation/requests/"); at >= 0 {
			id := path[at+len("/continuation/requests/"):]
			return method == "GET" && id != "" && !strings.Contains(id, "/")
		}
		return false
	}
	if path == "/v1/receipt-projections" {
		return method == "GET" && (caller == "runtime" || caller == "control")
	}
	if path == "/v1/source-state" {
		return caller == "worker" && method == "POST"
	}
	if strings.HasSuffix(path, "/calls/page") || strings.HasSuffix(path, "/calls/record") {
		return caller == "control" && method == "GET" && strings.HasPrefix(path, "/v1/runs/")
	}
	// App failure records and diagnostics are read-only projections. A business
	// service may read only its own app prefix; control may read any app. The
	// handler additionally verifies app_runs(app, run_id) ownership.
	if strings.HasSuffix(path, "/diagnostics") && strings.HasPrefix(path, "/v1/apps/") {
		if method != "GET" {
			return false
		}
		return caller == "control" || (caller == "futures" && strings.HasPrefix(path, "/v1/apps/futures/runs/"))
	}
	if strings.HasSuffix(path, "/run-failures") && strings.HasPrefix(path, "/v1/apps/") {
		if method != "GET" {
			return false
		}
		return caller == "control" || (caller == "futures" && strings.HasPrefix(path, "/v1/apps/futures/"))
	}
	// This derived ledger is not exposed through generic run-prefix grants.
	// Worker state operations use the separate fenced source-engine route.
	if strings.HasSuffix(path, "/working-state") || strings.HasSuffix(path, "/source-recovery") {
		return caller == "control" && method == "GET" && strings.HasPrefix(path, "/v1/runs/")
	}
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
