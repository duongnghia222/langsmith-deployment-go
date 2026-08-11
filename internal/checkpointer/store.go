// Package checkpointer implements LSD's Checkpointer gRPC service backed by Postgres.
// It provides byte-exact storage and retrieval of checkpoint blobs; LSD never
// decodes SerializedValue payloads — it stores encoding as TEXT and blob as BYTEA
// and returns them unchanged (R5 D1).
package checkpointer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides pgx-backed checkpoint persistence.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// BlobInput holds the fields for a single checkpoint_blobs row.
type BlobInput struct {
	Channel  string
	Version  string
	Encoding string // stored in the `type` column
	Blob     []byte // stored as BYTEA; nil is permitted (blob column is nullable)
}

// WriteInput holds a single checkpoint_writes row.
type WriteInput struct {
	Idx      int32
	Channel  string
	Encoding string // stored in the `type` column
	Blob     []byte
}

// PutInput holds all data required to upsert one checkpoint row plus its blobs.
type PutInput struct {
	ThreadID           string
	CheckpointNS       string
	CheckpointID       string
	ParentCheckpointID string // C4: incoming config's checkpoint_id; "" → SQL NULL
	RunID              string // C5: from config.run_id or metadata.run_id; "" → SQL NULL
	CheckpointJSON     []byte // raw JSON; stored in the jsonb `checkpoint` column
	MetadataJSON       []byte // raw JSON; stored in the jsonb `metadata` column
	Blobs              []BlobInput
}

// PutWritesInput holds data for one or more checkpoint_writes rows.
type PutWritesInput struct {
	ThreadID     string
	CheckpointNS string
	CheckpointID string
	TaskID       string
	TaskPath     string
	Writes       []WriteInput
}

// BlobRow is one row returned from a List or GetTuple query.
type BlobRow struct {
	Channel  string
	Version  string
	Encoding string
	Blob     []byte
}

// WriteRow is one row returned from a GetTuple query.
type WriteRow struct {
	TaskID   string
	Idx      int32
	Channel  string
	Encoding string
	Blob     []byte
	TaskPath string
}

// SendRow is a (type, blob) pair for a pending_send row from the parent checkpoint.
type SendRow struct {
	Encoding string
	Blob     []byte
}

// CheckpointRow is one row from the checkpoints table plus its associated blobs.
// Nullable uuid columns are represented as "" when NULL (per LSD convention; see
// runs/store.go: COALESCE(col::text,'')).
type CheckpointRow struct {
	ThreadID           string
	CheckpointNS       string
	CheckpointID       string
	ParentCheckpointID string
	RunID              string
	CheckpointJSON     []byte
	MetadataJSON       []byte
	Blobs              []BlobRow
	Writes             []WriteRow
	PendingSends       []SendRow // C9: parent checkpoint's __pregel_tasks writes
}

// Prune strategies — mirror checkpointer.PruneRequest_PruneStrategy values.
const (
	PruneStrategyUnspecified int32 = 0
	PruneStrategyKeepLatest  int32 = 1
	PruneStrategyDeleteAll   int32 = 2
)

// tasksChannelConst is the channel name used to carry pending sends.
// Keep verbatim: TASKS = "__pregel_tasks" (langgraph.constants)
const tasksChannelConst = "__pregel_tasks"

// Put upserts a checkpoint row and its blobs in a single transaction.
func (s *Store) Put(ctx context.Context, in PutInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// C4+C5: write parent_checkpoint_id and run_id.
	// Python aput: INSERT INTO checkpoints (run_id, thread_id, ..., parent_checkpoint_id, ...)
	// Use NULLIF to convert "" → SQL NULL so the uuid cast succeeds.
	if _, err := tx.Exec(ctx, `
		INSERT INTO checkpoints
			(run_id, thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
		VALUES (NULLIF($1,'')::uuid, $2::uuid, $3, $4::uuid, NULLIF($5,'')::uuid, $6::jsonb, $7::jsonb)
		ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id)
		DO UPDATE SET
			checkpoint = EXCLUDED.checkpoint,
			metadata   = EXCLUDED.metadata
	`, in.RunID, in.ThreadID, in.CheckpointNS, in.CheckpointID, in.ParentCheckpointID,
		string(in.CheckpointJSON), string(in.MetadataJSON)); err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}

	for _, b := range in.Blobs {
		// C10: Python aput uses DO NOTHING for checkpoint_blobs (checkpoint.py:247)
		if _, err := tx.Exec(ctx, `
			INSERT INTO checkpoint_blobs
				(thread_id, checkpoint_ns, channel, version, type, blob)
			VALUES ($1::uuid, $2, $3, $4, $5, $6)
			ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING
		`, in.ThreadID, in.CheckpointNS, b.Channel, b.Version, b.Encoding, b.Blob); err != nil {
			return fmt.Errorf("insert blob channel=%s version=%s: %w", b.Channel, b.Version, err)
		}
	}

	if _, err := tx.Exec(ctx,
		`UPDATE thread
		    SET state_updated_at = now(),
		        updated_at       = now()
		  WHERE thread_id = $1::uuid`,
		in.ThreadID,
	); err != nil {
		return fmt.Errorf("bump thread state_updated_at: %w", err)
	}

	return tx.Commit(ctx)
}

// PutWrites upserts checkpoint_writes rows in a single transaction.
func (s *Store) PutWrites(ctx context.Context, in PutWritesInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, w := range in.Writes {
		// C10: Python INSERT_CHECKPOINT_WRITES_SQL is DO NOTHING; the UPSERT variant
		// (UPSERT_CHECKPOINT_WRITES_SQL) is only used when all writes are in WRITES_IDX_MAP
		// (async_postgres_checkpointer.py:275). Here each write has already been assigned
		// its sentinel idx if applicable (in service.go), so we use the same split:
		// sentinel (negative) indices get DO UPDATE, others get DO NOTHING.
		var conflictClause string
		if w.Idx < 0 {
			// sentinel channel — upsert so the latest value wins
			conflictClause = `DO UPDATE SET channel = EXCLUDED.channel, type = EXCLUDED.type,
			              blob = EXCLUDED.blob, task_path = EXCLUDED.task_path`
		} else {
			// regular write — DO NOTHING (Python INSERT_CHECKPOINT_WRITES_SQL)
			conflictClause = `DO NOTHING`
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO checkpoint_writes
				(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob, task_path)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9)
			ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
			`+conflictClause,
			in.ThreadID, in.CheckpointNS, in.CheckpointID,
			in.TaskID, w.Idx, w.Channel, w.Encoding, w.Blob, in.TaskPath); err != nil {
			return fmt.Errorf("insert write idx=%d: %w", w.Idx, err)
		}
	}

	if _, err := tx.Exec(ctx,
		`UPDATE thread
		    SET state_updated_at = now(),
		        updated_at       = now()
		  WHERE thread_id = $1::uuid`,
		in.ThreadID,
	); err != nil {
		return fmt.Errorf("bump thread state_updated_at: %w", err)
	}

	return tx.Commit(ctx)
}

// GetTuple returns the checkpoint identified by (threadID, checkpointNS, checkpointID).
// When checkpointID is empty, returns the latest checkpoint (ordered by checkpoint_id DESC)
// for the thread+namespace. Returns (nil, nil) when no checkpoint exists.
func (s *Store) GetTuple(ctx context.Context, threadID, checkpointNS, checkpointID string) (*CheckpointRow, error) {
	var (
		row  CheckpointRow
		q    string
		args []any
	)
	if checkpointID != "" {
		q = `SELECT thread_id::text, checkpoint_ns, checkpoint_id::text,
		            COALESCE(parent_checkpoint_id::text, ''),
		            COALESCE(run_id::text, ''),
		            checkpoint::text::bytea, metadata::text::bytea
		     FROM checkpoints
		     WHERE thread_id = $1::uuid AND checkpoint_ns = $2 AND checkpoint_id = $3::uuid`
		args = []any{threadID, checkpointNS, checkpointID}
	} else {
		q = `SELECT thread_id::text, checkpoint_ns, checkpoint_id::text,
		            COALESCE(parent_checkpoint_id::text, ''),
		            COALESCE(run_id::text, ''),
		            checkpoint::text::bytea, metadata::text::bytea
		     FROM checkpoints
		     WHERE thread_id = $1::uuid AND checkpoint_ns = $2
		     ORDER BY checkpoint_id DESC LIMIT 1`
		args = []any{threadID, checkpointNS}
	}

	err := s.pool.QueryRow(ctx, q, args...).Scan(
		&row.ThreadID, &row.CheckpointNS, &row.CheckpointID,
		&row.ParentCheckpointID, &row.RunID,
		&row.CheckpointJSON, &row.MetadataJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan checkpoint: %w", err)
	}

	if err := s.loadBlobsAndWrites(ctx, &row); err != nil {
		return nil, err
	}

	return &row, nil
}

// loadBlobsAndWrites populates Blobs, Writes, and PendingSends on a CheckpointRow.
// It mirrors Python SELECT_SQL:
//   - channel_values: JOIN checkpoint_blobs ON channel_versions key/value match
//   - pending_writes: checkpoint_writes WHERE checkpoint_id = this checkpoint
//   - pending_sends:  checkpoint_writes WHERE checkpoint_id = parent AND channel = TASKS,
//     ORDER BY task_id, idx
func (s *Store) loadBlobsAndWrites(ctx context.Context, row *CheckpointRow) error {
	// Blobs: join via channel_versions stored in checkpoint JSON, mirroring SELECT_SQL.
	// We fetch all blobs for (thread_id, checkpoint_ns) and filter in Go to match
	// channel_versions; this avoids needing a jsonb_each_text lateral join in pgx.
	brows, err := s.pool.Query(ctx, `
		SELECT channel, version, type, blob
		FROM checkpoint_blobs
		WHERE thread_id = $1::uuid AND checkpoint_ns = $2
	`, row.ThreadID, row.CheckpointNS)
	if err != nil {
		return fmt.Errorf("query blobs: %w", err)
	}
	defer brows.Close()
	for brows.Next() {
		var b BlobRow
		if err := brows.Scan(&b.Channel, &b.Version, &b.Encoding, &b.Blob); err != nil {
			return fmt.Errorf("scan blob: %w", err)
		}
		row.Blobs = append(row.Blobs, b)
	}
	if err := brows.Err(); err != nil {
		return fmt.Errorf("iterate blobs: %w", err)
	}

	// Writes: pending_writes for this checkpoint, ORDER BY task_id, idx
	wrows, err := s.pool.Query(ctx, `
		SELECT task_id::text, idx, channel, type, blob, task_path
		FROM checkpoint_writes
		WHERE thread_id = $1::uuid AND checkpoint_ns = $2 AND checkpoint_id = $3::uuid
		ORDER BY task_id, idx
	`, row.ThreadID, row.CheckpointNS, row.CheckpointID)
	if err != nil {
		return fmt.Errorf("query writes: %w", err)
	}
	defer wrows.Close()
	for wrows.Next() {
		var w WriteRow
		if err := wrows.Scan(&w.TaskID, &w.Idx, &w.Channel, &w.Encoding, &w.Blob, &w.TaskPath); err != nil {
			return fmt.Errorf("scan write: %w", err)
		}
		row.Writes = append(row.Writes, w)
	}
	if err := wrows.Err(); err != nil {
		return fmt.Errorf("iterate writes: %w", err)
	}

	// C9: PendingSends — parent checkpoint's checkpoint_writes WHERE channel = tasksChannel.
	// Python SELECT_SQL: pending_sends ordered by task_id, idx.
	if row.ParentCheckpointID != "" {
		// tasksChannel = "__pregel_tasks" (langgraph.constants.TASKS)
		psrows, err := s.pool.Query(ctx, `
			SELECT type, blob
			FROM checkpoint_writes
			WHERE thread_id = $1::uuid AND checkpoint_ns = $2
			  AND checkpoint_id = $3::uuid AND channel = $4
			ORDER BY task_id, idx
		`, row.ThreadID, row.CheckpointNS, row.ParentCheckpointID, tasksChannelConst)
		if err != nil {
			return fmt.Errorf("query pending_sends: %w", err)
		}
		defer psrows.Close()
		for psrows.Next() {
			var ps SendRow
			if err := psrows.Scan(&ps.Encoding, &ps.Blob); err != nil {
				return fmt.Errorf("scan pending_send: %w", err)
			}
			row.PendingSends = append(row.PendingSends, ps)
		}
		if err := psrows.Err(); err != nil {
			return fmt.Errorf("iterate pending_sends: %w", err)
		}
	}

	return nil
}

// List returns checkpoints for (threadID, checkpointNS) in descending checkpoint_id
// order. When before is non-empty, restricts results to checkpoint_id < before.
// When filterJSON is non-empty, applies AND metadata @> $n::jsonb (Python: metadata @> %s).
// When limit is nil, all matching rows are returned.
// C8: loads blobs+writes for each row so callers receive fully-populated tuples.
func (s *Store) List(ctx context.Context, threadID, checkpointNS, before string, limit *int64, filterJSON []byte) ([]*CheckpointRow, error) {
	q := `SELECT thread_id::text, checkpoint_ns, checkpoint_id::text,
	             COALESCE(parent_checkpoint_id::text, ''),
	             COALESCE(run_id::text, ''),
	             checkpoint::text::bytea, metadata::text::bytea
	      FROM checkpoints
	      WHERE thread_id = $1::uuid AND checkpoint_ns = $2`
	args := []any{threadID, checkpointNS}
	idx := 3
	if before != "" {
		q += fmt.Sprintf(" AND checkpoint_id < $%d::uuid", idx)
		args = append(args, before)
		idx++
	}
	// C7: filter_json — Python: metadata @> %s (checkpoint.py:437)
	if len(filterJSON) > 0 {
		q += fmt.Sprintf(" AND metadata @> $%d::jsonb", idx)
		args = append(args, string(filterJSON))
		idx++
	}
	q += " ORDER BY checkpoint_id DESC"
	if limit != nil {
		q += fmt.Sprintf(" LIMIT $%d", idx)
		args = append(args, *limit)
	}

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()
	var out []*CheckpointRow
	for rows.Next() {
		var r CheckpointRow
		if err := rows.Scan(&r.ThreadID, &r.CheckpointNS, &r.CheckpointID,
			&r.ParentCheckpointID, &r.RunID, &r.CheckpointJSON, &r.MetadataJSON); err != nil {
			return nil, fmt.Errorf("scan checkpoint row: %w", err)
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}

	// C8: load blobs+writes per returned row
	for _, r := range out {
		if err := s.loadBlobsAndWrites(ctx, r); err != nil {
			return nil, fmt.Errorf("load blobs/writes for %s: %w", r.CheckpointID, err)
		}
	}
	return out, nil
}

// DeleteThread deletes all checkpoint rows (and cascaded blobs/writes) for a thread.
func (s *Store) DeleteThread(ctx context.Context, threadID string) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM checkpoints WHERE thread_id = $1::uuid`,
		threadID,
	); err != nil {
		return fmt.Errorf("delete thread: %w", err)
	}
	return nil
}

// DeleteForRuns deletes all checkpoints whose run_id is in runIDs. No-op on empty input.
func (s *Store) DeleteForRuns(ctx context.Context, runIDs []string) error {
	if len(runIDs) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM checkpoints WHERE run_id = ANY($1::uuid[])`,
		runIDs,
	); err != nil {
		return fmt.Errorf("delete for runs: %w", err)
	}
	return nil
}

// CopyThread copies all checkpoints, checkpoint_blobs, and checkpoint_writes
// rows from fromThreadID to toThreadID in a single transaction (R5 D6).
func (s *Store) CopyThread(ctx context.Context, fromThreadID, toThreadID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// C11: Python ops.py:1100-1103 patches copied checkpoints' metadata with
	// jsonb_set(metadata, '{thread_id}', to_jsonb(new_thread_id)) and PRESERVES run_id.
	if _, err := tx.Exec(ctx, `
		INSERT INTO checkpoints
			(run_id, thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
		SELECT run_id, $2::uuid, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint,
		       jsonb_set(metadata, '{thread_id}', to_jsonb($2::text))
		FROM checkpoints
		WHERE thread_id = $1::uuid
		ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id) DO NOTHING
	`, fromThreadID, toThreadID); err != nil {
		return fmt.Errorf("copy checkpoints: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO checkpoint_blobs
			(thread_id, checkpoint_ns, channel, version, type, blob)
		SELECT $2::uuid, checkpoint_ns, channel, version, type, blob
		FROM checkpoint_blobs
		WHERE thread_id = $1::uuid
		ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING
	`, fromThreadID, toThreadID); err != nil {
		return fmt.Errorf("copy checkpoint_blobs: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO checkpoint_writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob, task_path)
		SELECT $2::uuid, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob, task_path
		FROM checkpoint_writes
		WHERE thread_id = $1::uuid
		ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING
	`, fromThreadID, toThreadID); err != nil {
		return fmt.Errorf("copy checkpoint_writes: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE thread
		    SET state_updated_at = now(),
		        updated_at       = now()
		  WHERE thread_id = $1::uuid`,
		toThreadID,
	); err != nil {
		return fmt.Errorf("bump thread state_updated_at: %w", err)
	}

	return tx.Commit(ctx)
}

// Prune deletes checkpoint rows according to the given strategy.
// KEEP_LATEST: for each thread in threadIDs, keeps only the most recent checkpoint.
// DELETE_ALL: deletes every checkpoint for each thread in threadIDs.
func (s *Store) Prune(ctx context.Context, threadIDs []string, strategy int32) error {
	if len(threadIDs) == 0 {
		return nil
	}
	switch strategy {
	case PruneStrategyDeleteAll:
		if _, err := s.pool.Exec(ctx,
			`DELETE FROM checkpoints WHERE thread_id = ANY($1::uuid[])`,
			threadIDs,
		); err != nil {
			return fmt.Errorf("prune delete_all: %w", err)
		}
		return nil
	case PruneStrategyKeepLatest:
		if _, err := s.pool.Exec(ctx, `
			DELETE FROM checkpoints c
			WHERE c.thread_id = ANY($1::uuid[])
			  AND (c.thread_id, c.checkpoint_ns, c.checkpoint_id) NOT IN (
			      SELECT DISTINCT ON (thread_id, checkpoint_ns)
			             thread_id, checkpoint_ns, checkpoint_id
			        FROM checkpoints
			       WHERE thread_id = ANY($1::uuid[])
			       ORDER BY thread_id, checkpoint_ns, checkpoint_id DESC
			  )
		`, threadIDs); err != nil {
			return fmt.Errorf("prune keep_latest: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown prune strategy: %d", strategy)
	}
}
