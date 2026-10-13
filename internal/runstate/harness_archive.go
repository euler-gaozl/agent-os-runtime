package runstate

import (
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// Same upstream kernel; different, pinned platform evidence protocol. The old
// engine retains its wire behavior for unfinished historical runs.
const EngineDSHEvidence = "dsh-0.2.0-rc.2-evidence-v1"
const EngineDSHSource = "dsh-source-639ed015-state-v1"
const EngineDSHResident = "dsh-source-639ed015-resident-v2"

func IsSourceEngine(engine string) bool {
	return engine == EngineDSHSource || engine == EngineDSHResident
}

func hasIncrementalEvidence(engine string) bool {
	return engine == EngineDSHEvidence || IsSourceEngine(engine)
}

const archiveSegmentBytes = 1 << 20
const archiveBatchBytes = 16 << 20

type ArchivePart struct {
	Hash   string `json:"hash"`
	Offset int    `json:"offset_bytes"`
	Bytes  int    `json:"bytes"`
}
type HarnessArchive struct {
	Version     int           `json:"version"`
	RunID       string        `json:"run_id"`
	Exchange    int           `json:"exchange"`
	EventStart  int           `json:"event_start"`
	EventEnd    int           `json:"event_end"`
	Bytes       int           `json:"bytes"`
	SHA256      string        `json:"sha256"`
	Previous    string        `json:"previous_manifest,omitempty"`
	SourceOwner string        `json:"source_owner"`
	SourceFence int64         `json:"source_fence"`
	SourceStart int           `json:"source_event_start"`
	SourceEnd   int           `json:"source_event_end"`
	Parts       []ArchivePart `json:"parts"`
}

// Called with the run fence locked, in the same transaction as the exchange
// intent. A replay must use this first committed batch, not regenerated event
// timestamps or identifiers. No external effect may precede archive commit.
func stageHarnessArchive(ctx context.Context, tx pgx.Tx, r Run, f Frame) error {
	if len(f.Events) > archiveBatchBytes || !utf8.Valid(f.Events) {
		return errors.New("DSH event batch exceeds retention budget or is not UTF-8")
	}
	var events []json.RawMessage
	if json.Unmarshal(f.Events, &events) != nil || events == nil || f.EventStart < 0 || f.EventEnd-f.EventStart != len(events) {
		return errors.New("invalid incremental DSH event range")
	}
	var previous, sourceEnd int
	var fence int64
	var hash string
	e := tx.QueryRow(ctx, "SELECT event_end,source_end,source_fence,manifest_hash FROM harness_archives WHERE run_id=$1 ORDER BY seq DESC LIMIT 1", r.ID).Scan(&previous, &sourceEnd, &fence, &hash)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if errors.Is(e, pgx.ErrNoRows) && f.EventStart != 0 {
		return errors.New("first DSH event batch must start at zero")
	}
	if e == nil && (hash == "" || (fence == r.Fence && f.EventStart != sourceEnd)) {
		return errors.New("DSH event batch is unacknowledged or has an in-attempt gap")
	}
	// Rebuilt native session indexes can differ across attempts. The durable
	// directory uses accepted batch counts and also records original provenance;
	// these batches are evidence, never a replacement DSH session/replay log.
	_, e = tx.Exec(ctx, "INSERT INTO harness_archives(run_id,seq,event_start,event_end,batch,source_owner,source_fence,source_start,source_end,previous_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", r.ID, f.ID, previous, previous+len(events), []byte(f.Events), r.Owner, r.Fence, f.EventStart, f.EventEnd, hash)
	return e
}

func (s *Store) commitHarnessArchive(ctx context.Context, r Run, seq int) (string, error) {
	var raw []byte
	var hash string
	a := HarnessArchive{Version: 1, RunID: r.ID, Exchange: seq, Parts: []ArchivePart{}}
	if e := s.DB.QueryRow(ctx, "SELECT event_start,event_end,batch,manifest_hash,previous_hash,source_owner,source_fence,source_start,source_end FROM harness_archives WHERE run_id=$1 AND seq=$2", r.ID, seq).Scan(&a.EventStart, &a.EventEnd, &raw, &hash, &a.Previous, &a.SourceOwner, &a.SourceFence, &a.SourceStart, &a.SourceEnd); e != nil {
		return "", e
	}
	if hash != "" {
		return hash, nil
	}
	if raw == nil {
		return "", errors.New("missing staged DSH event batch")
	}
	a.Bytes, a.SHA256 = len(raw), Hash(raw)
	for start := 0; start < len(raw); {
		end := min(start+archiveSegmentBytes, len(raw))
		for end < len(raw) && !utf8.RuneStart(raw[end]) {
			end--
		}
		part, e := s.PutArtifact(ctx, raw[start:end], "text/plain; charset=utf-8", "run:"+r.ID)
		if e != nil {
			return "", e
		}
		a.Parts = append(a.Parts, ArchivePart{Hash: part, Offset: start, Bytes: end - start})
		start = end
	}
	manifest := JSON(a)
	hash, e := s.PutArtifact(ctx, manifest, "application/vnd.agentos.event-manifest+json", "run:"+r.ID)
	if e != nil {
		return "", e
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	if e = LockRun(ctx, tx, r.ID, r.Owner, r.Fence, false); e != nil {
		return "", e
	}
	// Both writers of a retried batch upload identical content-addressed bytes.
	// Only the current owner can acknowledge them or retire the temporary copy.
	var existing string
	if e = tx.QueryRow(ctx, "SELECT manifest_hash FROM harness_archives WHERE run_id=$1 AND seq=$2 FOR UPDATE", r.ID, seq).Scan(&existing); e != nil {
		return "", e
	}
	if existing != "" && existing != hash {
		return "", errors.New("DSH archive identity changed")
	}
	if _, e = tx.Exec(ctx, "UPDATE harness_archives SET manifest_hash=$3,manifest=$4,batch=NULL WHERE run_id=$1 AND seq=$2", r.ID, seq, hash, manifest); e != nil {
		return "", e
	}
	return hash, tx.Commit(ctx)
}
