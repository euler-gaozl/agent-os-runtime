package runtime

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
)

// Only event identity/type/time cross the boundary. Prompts, model messages,
// tool arguments, results and credentials are never exported here.
type telemetryDelivery struct {
	Sequence  int64     `json:"sequence"`
	RunID     string    `json:"run_id"`
	Kind      string    `json:"kind"`
	CreatedAt time.Time `json:"created_at"`
	Fence     int64     `json:"fence"`
}
type telemetryReceipt struct {
	Sequence int64 `json:"sequence"`
	Fence    int64 `json:"fence"`
}

func (s *Store) claimTelemetry(ctx context.Context, owner string) ([]telemetryDelivery, error) {
	if len(owner) < 16 || len(owner) > 100 || !safeName.MatchString(owner) {
		return nil, errors.New("invalid telemetry delivery owner")
	}
	rows, e := s.DB.Query(ctx, `WITH batch AS (
 SELECT seq FROM events WHERE telemetry_delivered_at IS NULL
 AND (telemetry_until IS NULL OR telemetry_until<clock_timestamp()) ORDER BY seq FOR UPDATE SKIP LOCKED LIMIT 32
 ) UPDATE events e SET telemetry_owner=$1,telemetry_fence=telemetry_fence+1,telemetry_until=clock_timestamp()+interval '30 seconds'
 FROM batch WHERE e.seq=batch.seq RETURNING e.seq,e.run_id,e.kind,e.created_at,e.telemetry_fence`, owner)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []telemetryDelivery{}
	for rows.Next() {
		var d telemetryDelivery
		if e = rows.Scan(&d.Sequence, &d.RunID, &d.Kind, &d.CreatedAt, &d.Fence); e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) acknowledgeTelemetry(ctx context.Context, owner string, receipts []telemetryReceipt) error {
	if len(receipts) < 1 || len(receipts) > 32 {
		return errors.New("bounded telemetry receipts required")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for _, r := range receipts {
		tag, e := tx.Exec(ctx, `UPDATE events SET telemetry_delivered_at=coalesce(telemetry_delivered_at,clock_timestamp())
 WHERE seq=$1 AND telemetry_owner=$2 AND telemetry_fence=$3
 AND (telemetry_delivered_at IS NOT NULL OR telemetry_until>clock_timestamp())`, r.Sequence, owner, r.Fence)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return ErrFence
		}
	}
	return tx.Commit(ctx)
}
func (s *Server) telemetryDeliveryRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/telemetry/claim", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if servicehttp.Caller(r.Context()) != "telemetry" {
			return &servicehttp.StatusError{Status: 403, Message: "telemetry service identity required"}
		}
		var in struct {
			Owner string `json:"owner"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		out, e := s.Store.claimTelemetry(r.Context(), in.Owner)
		if e != nil {
			return e
		}
		jsonResponse(w, 200, out)
		return nil
	}))
	mux.HandleFunc("POST /v1/telemetry/ack", wrap(func(w http.ResponseWriter, r *http.Request) error {
		if servicehttp.Caller(r.Context()) != "telemetry" {
			return &servicehttp.StatusError{Status: 403, Message: "telemetry service identity required"}
		}
		var in struct {
			Owner    string             `json:"owner"`
			Receipts []telemetryReceipt `json:"receipts"`
		}
		if e := decode(w, r, &in); e != nil {
			return e
		}
		if e := s.Store.acknowledgeTelemetry(r.Context(), in.Owner, in.Receipts); e != nil {
			return e
		}
		jsonResponse(w, 200, map[string]bool{"acknowledged": true})
		return nil
	}))
}
