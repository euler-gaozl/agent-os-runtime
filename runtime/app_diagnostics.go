package runtime

// App-scoped failure diagnostics are a bounded, read-only projection of the
// execution facts that already live in runs, calls and events. This file does
// not create an error store, does not change run state and never returns a
// model request/response, a provider URL, a credential name or arbitrary
// exception text. Business wording belongs to the calling application; the
// Runtime exposes only stable codes, stages and a bounded safe parameter.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-runtime/internal/runstate"
	"github.com/jackc/pgx/v5"
)

const diagnosticsProtocol = "app-run-diagnostics-v1"
const failureListProtocol = "app-run-failures-v1"

// The scan budget bounds one list request. Failure rows are rare because only
// failed, cancelled and effect-unknown app runs are candidates; category and
// stage filters are applied to the projected stream before a page is returned,
// and the cursor advances over every scanned candidate so pages never repeat or
// skip a row. Truncation is reported instead of hiding it.
const failurePageLimit = 50
const failureScanBatch = 25
const failureScanLimit = 200
const failureWindowDefault = 90 * 24 * time.Hour
const failureWindowMaximum = 366 * 24 * time.Hour

var failureStatuses = []string{"failed", "cancelled", "waiting_unknown"}

var diagnosticStages = map[string]bool{
	"tool": true, "model": true, "execution": true, "cancel": true, "unknown": true,
}

var diagnosticCategories = map[string]bool{
	"parameter_rejected": true, "configuration_or_auth": true, "downstream_unavailable": true,
	"effect_unknown": true, "protocol_error": true, "budget_exhausted": true,
	"persistence_or_lease": true, "cancelled": true, "unknown": true,
}

// Fixed messages contain no interpolated data. The application owns the
// user-facing explanation and maps these codes to it.
var diagnosticMessages = map[string]string{
	"parameter_rejected":     "A tool request was rejected as invalid.",
	"configuration_or_auth":  "A required configuration or credential is missing or was rejected.",
	"downstream_unavailable": "A downstream service was temporarily unavailable.",
	"effect_unknown":         "An external effect could not be confirmed.",
	"protocol_error":         "An execution protocol response was incomplete or invalid.",
	"budget_exhausted":       "An execution budget or context limit was reached.",
	"persistence_or_lease":   "Execution state persistence or ownership could not be confirmed.",
	"cancelled":              "The task was cancelled.",
	"unknown":                "The recorded failure does not identify a confirmed cause.",
}

type diagnosticAction struct {
	recommendedAction string
	retryPolicy       string
}

var diagnosticActions = map[string]diagnosticAction{
	"parameter_rejected":     {"correct_input", "manual_after_correction"},
	"configuration_or_auth":  {"contact_operator", "operator_after_fix"},
	"downstream_unavailable": {"retry_later", "manual_retry_later"},
	"effect_unknown":         {"operator_review", "operator_review"},
	"protocol_error":         {"contact_operator", "operator_review"},
	"budget_exhausted":       {"narrow_scope", "manual_after_correction"},
	"persistence_or_lease":   {"operator_review", "operator_review"},
	"cancelled":              {"none", "do_not_retry"},
	"unknown":                {"operator_review", "operator_review"},
}

type safeParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type runDiagnostics struct {
	Protocol          string              `json:"protocol"`
	RunID             string              `json:"run_id"`
	Status            string              `json:"status"`
	FailureVersion    string              `json:"failure_version"`
	ReportedAt        time.Time           `json:"reported_at"`
	Stage             string              `json:"stage"`
	Code              string              `json:"code"`
	SafeMessage       string              `json:"safe_message"`
	CauseConfirmed    bool                `json:"cause_confirmed"`
	RelatedEventIDs   []int64             `json:"related_event_ids"`
	OperationID       string              `json:"operation_id,omitempty"`
	RelatedRole       string              `json:"related_role,omitempty"`
	Tool              string              `json:"tool,omitempty"`
	SafeParameter     *safeParameter      `json:"safe_parameter,omitempty"`
	RecommendedAction string              `json:"recommended_action"`
	RetryPolicy       string              `json:"retry_policy"`
	Usage             runstate.ModelUsage `json:"usage"`
}

type runFailureItem struct {
	RunID          string    `json:"run_id"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	Commodity      string    `json:"commodity,omitempty"`
	Period         string    `json:"period,omitempty"`
	Stage          string    `json:"stage"`
	Code           string    `json:"code"`
	CauseConfirmed bool      `json:"cause_confirmed"`
	OperationID    string    `json:"operation_id,omitempty"`
	Tool           string    `json:"tool,omitempty"`
	FailureVersion string    `json:"failure_version"`
}

type failureCoverage struct {
	Since     time.Time `json:"since"`
	Until     time.Time `json:"until"`
	Scanned   int       `json:"scanned"`
	ScanLimit int       `json:"scan_limit"`
	Truncated bool      `json:"truncated"`
	Complete  bool      `json:"complete"`
}

type runFailurePage struct {
	Protocol   string           `json:"protocol"`
	App        string           `json:"app"`
	Items      []runFailureItem `json:"items"`
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
	Coverage   failureCoverage  `json:"coverage"`
}

type failureCursor struct {
	App       string `json:"app"`
	SinceUnix int64  `json:"since"`
	UntilUnix int64  `json:"until"`
	Status    string `json:"status"`
	Category  string `json:"category"`
	Stage     string `json:"stage"`
	Commodity string `json:"commodity"`
	AtUnix    int64  `json:"at"`
	ID        string `json:"id"`
}

type failureFilter struct {
	since     time.Time
	until     time.Time
	status    string
	category  string
	stage     string
	commodity string
}

type failureCandidate struct {
	ID        string
	Status    string
	CreatedAt time.Time
	Metadata  json.RawMessage
}

type anomalyEvent struct {
	Seq     int64
	Kind    string
	OpID    string
	Payload json.RawMessage
}

type linkedFailure struct {
	stage     string
	code      string
	confirmed bool
	opID      string
	role      string
	tool      string
	param     *safeParameter
	events    []int64
	version   string
}

func (s *Store) appOwnedRun(ctx context.Context, app, id string) (failureCandidate, error) {
	var out failureCandidate
	err := s.DB.QueryRow(ctx, `SELECT r.id,r.status,r.created_at,a.metadata
 FROM app_runs a JOIN runs r ON r.id=a.run_id WHERE a.app=$1 AND a.run_id=$2`, app, id).
		Scan(&out.ID, &out.Status, &out.CreatedAt, &out.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, stateError(404, "app task not found")
	}
	return out, err
}

func (s *Store) terminalCheckpoint(ctx context.Context, run, status string) (int64, error) {
	var seq int64
	err := s.DB.QueryRow(ctx, `SELECT seq FROM events WHERE run_id=$1 AND kind='checkpoint'
 AND payload->>'status'=$2 ORDER BY seq DESC LIMIT 1`, run, status).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

// A recorded failure is only linked when a tool/model anomaly is still the
// latest unrecovered anomaly and precedes the terminal checkpoint. A recovered
// tool error, or an anomaly recorded after termination, is historical noise.
func (s *Store) lastAnomalyBefore(ctx context.Context, run string, terminal int64) (*anomalyEvent, bool, error) {
	rows, err := s.DB.Query(ctx, `SELECT seq,kind,payload FROM events
 WHERE run_id=$1 AND kind IN ('tool_retryable','tool_unknown','model_failed')
 ORDER BY seq DESC LIMIT 16`, run)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	anomalies := []anomalyEvent{}
	for rows.Next() {
		var e anomalyEvent
		if err = rows.Scan(&e.Seq, &e.Kind, &e.Payload); err != nil {
			return nil, false, err
		}
		var p struct {
			OperationID string `json:"operation_id"`
			CallID      string `json:"call_id"`
		}
		if err = json.Unmarshal(e.Payload, &p); err != nil {
			return nil, false, err
		}
		e.OpID = p.OperationID
		if e.OpID == "" {
			e.OpID = p.CallID
		}
		anomalies = append(anomalies, e)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	for i := range anomalies {
		e := anomalies[i]
		if terminal > 0 && e.Seq >= terminal {
			continue
		}
		if e.OpID != "" {
			var recovered bool
			kind := "tool_completed"
			key := "operation_id"
			if strings.HasPrefix(e.Kind, "model_") {
				kind, key = "model_completed", "call_id"
			}
			if err = s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE run_id=$1 AND kind=$2
 AND seq>$3 AND payload->>$4=$5)`, run, kind, e.Seq, key, e.OpID).Scan(&recovered); err != nil {
				return nil, false, err
			}
			if recovered {
				continue
			}
		}
		return &e, true, nil
	}
	if len(anomalies) > 0 {
		// Every recorded anomaly was recovered (or happened after termination).
		return nil, false, nil
	}
	return nil, false, nil
}

var httpStatusError = regexp.MustCompile(`^service HTTP ([0-9]{3})(?::|$)`)
var toolNameSafe = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

func classifyToolFailure(status, text string) (string, bool) {
	if status == "unknown" {
		return "effect_unknown", true
	}
	lower := strings.ToLower(text)
	if m := httpStatusError.FindStringSubmatch(text); m != nil {
		code, _ := strconv.Atoi(m[1])
		switch {
		case code == 400 || code == 404 || code == 413 || code == 422:
			return "parameter_rejected", true
		case code == 401 || code == 403:
			return "configuration_or_auth", true
		case code == 409:
			return "effect_unknown", true
		case code == 412:
			return "persistence_or_lease", true
		case code == 429 || code >= 500:
			return "downstream_unavailable", true
		}
	}
	switch {
	case strings.Contains(lower, "not permitted"), strings.Contains(lower, "not authorized"),
		strings.Contains(lower, "not bound to this agent"), strings.Contains(lower, "credential"),
		strings.Contains(lower, "not configured"), strings.Contains(lower, "connector missing"):
		return "configuration_or_auth", true
	case strings.Contains(lower, "budget exhausted"), strings.Contains(lower, "exceeds supported"):
		return "budget_exhausted", false
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "deadline"),
		strings.Contains(lower, "connection refused"), strings.Contains(lower, "no such host"),
		strings.Contains(lower, "temporarily unavailable"):
		return "downstream_unavailable", false
	case strings.Contains(lower, "invalid"), strings.Contains(lower, "exceeds document length"),
		strings.Contains(lower, "offset exceeds"):
		return "parameter_rejected", false
	}
	return "unknown", false
}

func classifyModelFailure(text string) (string, bool) {
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "token budget exhausted"), strings.Contains(lower, "context budget exceeded"),
		strings.Contains(lower, "attempt budget exhausted"):
		return "budget_exhausted", true
	case strings.Contains(lower, "incomplete or truncated"):
		return "protocol_error", true
	case strings.Contains(lower, "credential"):
		return "configuration_or_auth", true
	}
	if m := httpStatusError.FindStringSubmatch(strings.TrimSpace(text)); m != nil {
		code, _ := strconv.Atoi(m[1])
		switch {
		case code == 401 || code == 403:
			return "configuration_or_auth", true
		case code == 429 || code >= 500:
			return "downstream_unavailable", true
		case code == 400 || code == 413 || code == 422:
			return "protocol_error", true
		}
	}
	if strings.HasPrefix(lower, "model http ") {
		switch {
		case strings.HasSuffix(lower, "503"), strings.HasSuffix(lower, "502"), strings.HasSuffix(lower, "504"), strings.HasSuffix(lower, "429"):
			return "downstream_unavailable", true
		case strings.HasSuffix(lower, "401"), strings.HasSuffix(lower, "403"):
			return "configuration_or_auth", true
		}
	}
	return "unknown", false
}

// Only short scalar business arguments are projected. Model requests, tool
// responses, headers and arbitrary error text never reach this function. The
// argument is the call's `request->arguments` JSON object, not the envelope.
func safeToolParameter(arguments json.RawMessage) *safeParameter {
	if len(arguments) == 0 || len(arguments) > 8192 {
		return nil
	}
	var in map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &in); err != nil {
		return nil
	}
	for _, name := range []string{"query", "keyword", "symbol", "commodity"} {
		raw, ok := in[name]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil {
			continue
		}
		if !safeParameterValue(value) {
			continue
		}
		return &safeParameter{Name: name, Value: value}
	}
	return nil
}

func safeParameterValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	n := utf8.RuneCountInString(value)
	if n < 1 || n > 80 {
		return false
	}
	lower := strings.ToLower(value)
	for _, blocked := range []string{"://", "bearer ", "token", "secret", "password", "api_key", "apikey"} {
		if strings.Contains(lower, blocked) {
			return false
		}
	}
	for _, r := range value {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return false
		}
	}
	return strings.TrimSpace(value) == value
}

func failureDigest(run, status, stage, code string, terminal, anomaly int64, op, errorText string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00", run, status, stage, code, terminal, anomaly, op)
	err := sha256.Sum256([]byte(errorText))
	h.Write(err[:])
	return "fvd1:" + hex.EncodeToString(h.Sum(nil))
}

func (s *Store) projectFailure(ctx context.Context, cand failureCandidate) (runFailureItem, bool, error) {
	item := runFailureItem{RunID: cand.ID, Status: cand.Status, CreatedAt: cand.CreatedAt}
	var meta struct {
		Commodity string `json:"commodity"`
		Period    string `json:"period"`
	}
	if len(cand.Metadata) > 0 {
		_ = json.Unmarshal(cand.Metadata, &meta)
	}
	item.Commodity = truncateRunes(meta.Commodity, 100)
	item.Period = truncateRunes(meta.Period, 300)

	terminal, err := s.terminalCheckpoint(ctx, cand.ID, cand.Status)
	if err != nil {
		return item, false, err
	}
	stage, code, confirmed, op, role, tool := "unknown", "unknown", false, "", "", ""
	errorText := ""
	version := failureDigest(cand.ID, cand.Status, stage, code, terminal, 0, "", "")
	if cand.Status == "cancelled" {
		stage, code, confirmed = "cancel", "cancelled", true
		version = failureDigest(cand.ID, cand.Status, stage, code, terminal, terminal, "", "")
	} else {
		anomaly, linked, e := s.lastAnomalyBefore(ctx, cand.ID, terminal)
		if e != nil {
			return item, false, e
		}
		if anomaly != nil && linked {
			role = "tool"
			if strings.HasPrefix(anomaly.Kind, "model_") {
				role = "model"
			}
			stage = role
			op = anomaly.OpID
			var callStatus, callError string
			var arguments []byte
			e := s.DB.QueryRow(ctx, `SELECT status,error,left(coalesce(request->>'name',''),64),
 CASE WHEN length(request::text)<=8192 THEN request->'arguments' ELSE NULL END
 FROM calls WHERE run_id=$1 AND id=$2`, cand.ID, anomaly.OpID).Scan(&callStatus, &callError, &tool, &arguments)
			if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return item, false, e
			}
			errorText = callError
			if role == "tool" {
				code, confirmed = classifyToolFailure(callStatus, callError)
				if callStatus == "unknown" {
					code = "effect_unknown"
				}
				if !toolNameSafe.MatchString(tool) {
					tool = ""
				}
			} else {
				code, confirmed = classifyModelFailure(callError)
			}
			if terminal == 0 {
				confirmed = false
			}
		}
		if anomaly != nil && linked && terminal > 0 {
			version = failureDigest(cand.ID, cand.Status, stage, code, terminal, anomaly.Seq, op, errorText)
		} else if anomaly != nil && !linked && terminal > 0 {
			// A recovered anomaly is historical; the digest still binds the
			// terminal checkpoint so a late response cannot be reused.
			version = failureDigest(cand.ID, cand.Status, stage, code, terminal, 0, "", "")
		}
	}
	item.Stage, item.Code, item.CauseConfirmed = stage, code, confirmed
	item.OperationID, item.Tool = op, tool
	item.FailureVersion = version
	return item, true, nil
}

func (s *Store) readRunDiagnostics(ctx context.Context, app, id string) (runDiagnostics, error) {
	out := runDiagnostics{Protocol: diagnosticsProtocol, RunID: id, RelatedEventIDs: []int64{}}
	cand, err := s.appOwnedRun(ctx, app, id)
	if err != nil {
		return out, err
	}
	out.Status = cand.Status
	terminal, err := s.terminalCheckpoint(ctx, id, cand.Status)
	if err != nil {
		return out, err
	}
	stage, code, confirmed, op, role, tool := "unknown", "unknown", false, "", "", ""
	var param *safeParameter
	var events []int64
	version := failureDigest(id, cand.Status, stage, code, terminal, 0, "", "")
	if cand.Status == "cancelled" {
		stage, code, confirmed = "cancel", "cancelled", true
		events = append(events, terminal)
	} else {
		anomaly, linked, e := s.lastAnomalyBefore(ctx, id, terminal)
		if e != nil {
			return out, e
		}
		if anomaly != nil && linked {
			role = "tool"
			if strings.HasPrefix(anomaly.Kind, "model_") {
				role = "model"
			}
			stage, op = role, anomaly.OpID
			var callStatus, callError string
			var arguments []byte
			e := s.DB.QueryRow(ctx, `SELECT status,error,left(coalesce(request->>'name',''),64),
 CASE WHEN length(request::text)<=8192 THEN request->'arguments' ELSE NULL END
 FROM calls WHERE run_id=$1 AND id=$2`, id, anomaly.OpID).Scan(&callStatus, &callError, &tool, &arguments)
			if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return out, e
			}
			if role == "tool" {
				code, confirmed = classifyToolFailure(callStatus, callError)
				if callStatus == "unknown" {
					code = "effect_unknown"
				}
				if !toolNameSafe.MatchString(tool) {
					tool = ""
				}
				if tool != "" {
					param = safeToolParameter(arguments)
				}
			} else {
				code, confirmed = classifyModelFailure(callError)
			}
			if terminal == 0 {
				confirmed = false
			}
			events = append(events, anomaly.Seq)
			version = failureDigest(id, cand.Status, stage, code, terminal, anomaly.Seq, op, callError)
		}
		if terminal > 0 && (len(events) == 0 || events[len(events)-1] != terminal) {
			events = append(events, terminal)
		}
	}
	if !diagnosticCategories[code] {
		code, stage, confirmed = "unknown", "unknown", false
	}
	out.Stage, out.Code, out.CauseConfirmed = stage, code, confirmed
	out.OperationID, out.RelatedRole, out.Tool, out.SafeParameter = op, role, tool, param
	out.RelatedEventIDs = events
	out.FailureVersion = version
	out.SafeMessage = diagnosticMessages[code]
	action := diagnosticActions[code]
	out.RecommendedAction, out.RetryPolicy = action.recommendedAction, action.retryPolicy
	usage, e := s.Store.Usage(ctx, id)
	if e != nil {
		return out, e
	}
	out.Usage = usage
	out.ReportedAt = time.Now().UTC()
	return out, nil
}

func parseFailureWindow(q string) (time.Time, error) {
	if q == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, q)
	if err != nil {
		return time.Time{}, stateError(400, "invalid failure window timestamp")
	}
	return t.UTC().Truncate(time.Second), nil
}

func firstValue(q map[string][]string, key string) string {
	if values := q[key]; len(values) == 1 {
		return values[0]
	}
	return ""
}

func failureQuery(q map[string][]string) (failureFilter, int, error) {
	var f failureFilter
	now := time.Now().UTC().Truncate(time.Second)
	until, err := parseFailureWindow(firstValue(q, "until"))
	if err != nil {
		return f, 0, err
	}
	if until.IsZero() {
		until = now
	}
	since, err := parseFailureWindow(firstValue(q, "since"))
	if err != nil {
		return f, 0, err
	}
	if since.IsZero() {
		since = until.Add(-failureWindowDefault)
	}
	if !since.Before(until) || until.Sub(since) > failureWindowMaximum {
		return f, 0, stateError(400, "failure window must be positive and at most 366 days")
	}
	f.since, f.until = since, until
	f.status = firstValue(q, "status")
	if f.status != "" && !contains(failureStatuses, f.status) {
		return f, 0, stateError(400, "invalid failure status")
	}
	f.category = firstValue(q, "category")
	if f.category != "" && !diagnosticCategories[f.category] {
		return f, 0, stateError(400, "invalid failure category")
	}
	f.stage = firstValue(q, "stage")
	if f.stage != "" && !diagnosticStages[f.stage] {
		return f, 0, stateError(400, "invalid failure stage")
	}
	f.commodity = strings.TrimSpace(firstValue(q, "commodity"))
	if utf8.RuneCountInString(f.commodity) > 100 {
		return f, 0, stateError(400, "commodity filter is too long")
	}
	limit := 25
	if raw := firstValue(q, "limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > failurePageLimit {
			return f, 0, stateError(400, "invalid failure page limit")
		}
		limit = n
	}
	return f, limit, nil
}

func contains(values []string, v string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

func (s *Store) scanFailureCandidates(ctx context.Context, app string, f failureFilter, after failureCursor, limit int) ([]failureCandidate, bool, error) {
	statuses := failureStatuses
	if f.status != "" {
		statuses = []string{f.status}
	}
	q := `SELECT a.run_id,r.status,a.created_at,a.metadata
 FROM app_runs a JOIN runs r ON r.id=a.run_id
 WHERE a.app=$1 AND r.status=ANY($2) AND a.created_at>=$3 AND a.created_at<$4
 AND ($5='' OR a.metadata->>'commodity'=$5)`
	args := []any{app, statuses, f.since, f.until, f.commodity}
	if after.ID != "" {
		q += ` AND (a.created_at,a.run_id)<($6,$7)`
		args = append(args, after.At(), after.ID)
	}
	q += fmt.Sprintf(` ORDER BY a.created_at DESC,a.run_id DESC LIMIT %d`, limit+1)
	rows, err := s.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := []failureCandidate{}
	for rows.Next() {
		var c failureCandidate
		if err = rows.Scan(&c.ID, &c.Status, &c.CreatedAt, &c.Metadata); err != nil {
			return nil, false, err
		}
		out = append(out, c)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}

func (c failureCursor) At() time.Time { return time.Unix(c.AtUnix, 0).UTC() }

func encodeFailureCursor(c failureCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func parseFailureCursor(encoded, app string, f failureFilter) (*failureCursor, error) {
	if encoded == "" {
		return nil, nil
	}
	if len(encoded) > 2048 {
		return nil, stateError(400, "invalid failure cursor")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, stateError(400, "invalid failure cursor")
	}
	var c failureCursor
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return nil, stateError(400, "invalid failure cursor")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, stateError(400, "invalid failure cursor")
	}
	at := c.At().UTC().Truncate(time.Second)
	if c.App != app || c.SinceUnix != f.since.Unix() || c.UntilUnix != f.until.Unix() ||
		c.Status != f.status || c.Category != f.category || c.Stage != f.stage || c.Commodity != f.commodity ||
		at.IsZero() || c.ID == "" || len(c.ID) > 200 {
		return nil, stateError(400, "failure cursor does not match this query")
	}
	return &c, nil
}

func (s *Store) readAppRunFailures(ctx context.Context, app string, f failureFilter, after string, limit int) (runFailurePage, error) {
	out := runFailurePage{Protocol: failureListProtocol, App: app, Items: []runFailureItem{},
		Coverage: failureCoverage{Since: f.since.UTC(), Until: f.until.UTC(), ScanLimit: failureScanLimit}}
	c, err := parseFailureCursor(after, app, f)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if limit < 1 || limit > failurePageLimit {
		return out, stateError(400, "invalid failure page")
	}
	if f.since.IsZero() || f.until.IsZero() || !f.since.Before(f.until) || f.until.Sub(f.since) > failureWindowMaximum {
		return out, stateError(400, "failure window must be positive and at most 366 days")
	}
	cursor := failureCursor{App: app, SinceUnix: f.since.Unix(), UntilUnix: f.until.Unix(),
		Status: f.status, Category: f.category, Stage: f.stage, Commodity: f.commodity}
	if c != nil {
		cursor.ID = c.ID
		cursor.AtUnix = c.AtUnix
	}
	scanned := 0
	for {
		batch, more, err := s.scanFailureCandidates(ctx, app, f, cursor, failureScanBatch)
		if err != nil {
			return out, err
		}
		for _, cand := range batch {
			if scanned >= failureScanLimit {
				out.HasMore = true
				out.Coverage.Truncated = true
				out.NextCursor = encodeFailureCursor(cursor)
				out.Coverage.Scanned = scanned
				return out, nil
			}
			cursor.AtUnix, cursor.ID = cand.CreatedAt.Unix(), cand.ID
			scanned++
			out.Coverage.Scanned = scanned
			item, ok, err := s.projectFailure(ctx, cand)
			if err != nil {
				return out, err
			}
			if !ok {
				continue
			}
			if f.category != "" && item.Code != f.category {
				continue
			}
			if f.stage != "" && item.Stage != f.stage {
				continue
			}
			out.Items = append(out.Items, item)
			if len(out.Items) == limit {
				hasMore := more
				if !hasMore {
					// A probe of one row: a returned row means another candidate
					// exists even though it is not part of a >limit batch.
					probe, _, err := s.scanFailureCandidates(ctx, app, f, cursor, 1)
					if err != nil {
						return out, err
					}
					hasMore = len(probe) > 0
				}
				out.HasMore = hasMore
				if hasMore {
					out.NextCursor = encodeFailureCursor(cursor)
				}
				return out, nil
			}
		}
		if !more {
			out.HasMore = false
			out.Coverage.Complete = true
			out.Coverage.Scanned = scanned
			return out, nil
		}
	}
}

func (s *Server) appDiagnosticsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/apps/{app}/runs/{id}/diagnostics", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.RawQuery != "" {
			return stateError(400, "unexpected diagnostics query")
		}
		v, err := s.Store.readRunDiagnostics(r.Context(), r.PathValue("app"), r.PathValue("id"))
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("GET /v1/apps/{app}/run-failures", wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := callQuery(r, "since", "until", "status", "category", "stage", "commodity", "limit", "after")
		if err != nil {
			return err
		}
		f, limit, err := failureQuery(q)
		if err != nil {
			return err
		}
		page, err := s.Store.readAppRunFailures(r.Context(), r.PathValue("app"), f, q.Get("after"), limit)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		jsonResponse(w, 200, page)
		return nil
	}))
}

func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}
