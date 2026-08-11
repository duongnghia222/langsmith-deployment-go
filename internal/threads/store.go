package threads

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a thread is not found or is hidden by auth filters.
var ErrNotFound = errors.New("thread not found")

// ErrForbidden is returned when an auth filter excludes the matching thread row.
var ErrForbidden = errors.New("thread forbidden by auth filters")

// Store provides read-only access to the thread table.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store backed by the given connection pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Thread is the internal representation of a thread row.
type Thread struct {
	ThreadID       string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ExpiresAt      *time.Time // nullable; set when TTL is configured
	StateUpdatedAt *time.Time // nullable; populated by checkpointer (Task 6)
	Metadata       []byte
	Config         []byte
	Values         []byte
	Interrupts     []byte
}

// threadCols is the SELECT projection for all Thread fields.
// "values" is quoted because it is a SQL reserved word.
// Canonical order: thread_id, status, created_at, updated_at, expires_at,
// state_updated_at, metadata, config, "values", interrupts.
const threadCols = `thread_id::text, COALESCE(status,''), created_at, updated_at,
	expires_at, state_updated_at,
	COALESCE(metadata, '{}'::jsonb)::text::bytea,
	COALESCE(config, '{}'::jsonb)::text::bytea,
	COALESCE("values", 'null'::jsonb)::text::bytea,
	COALESCE(interrupts, '{}'::jsonb)::text::bytea`

// Get returns the thread with the given UUID string, applying auth filters to
// the metadata column. Returns ErrNotFound if there is no matching row.
func (s *Store) Get(ctx context.Context, threadID string, filters []*coreapi.AuthFilter) (*Thread, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(`SELECT %s FROM thread WHERE thread_id = $1::uuid %s`, threadCols, prefixWithAnd(authSQL))
	row := s.pool.QueryRow(ctx, q, append([]any{threadID}, args...)...)
	var t Thread
	if err := row.Scan(&t.ThreadID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.ExpiresAt, &t.StateUpdatedAt, &t.Metadata, &t.Config, &t.Values, &t.Interrupts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// SearchInput carries the optional filter parameters for Search and Count.
type SearchInput struct {
	Status         string
	MetadataFilter []byte   // optional JSONB containment filter (@>)
	ValuesFilter   []byte   // optional JSONB containment filter on "values" (@>)
	Ids            []string // optional thread_id IN (ANY) filter — ops.py:660-663
	Limit          uint64
	Offset         uint64
	SortBy         string // one of "thread_id","status","created_at","updated_at"; default "created_at"
	SortOrder      string // "asc" or "desc"; default "desc"
}

// validSortColumns maps Python's whitelist (ops.py:686-687) to safe column names.
// Keep-verbatim from Python: valid_sort_fields = ["thread_id", "status", "created_at", "updated_at"]
var validSortColumns = map[string]string{
	"thread_id":  "thread_id",
	"status":     "status",
	"created_at": "created_at",
	"updated_at": "updated_at",
}

// sortClause returns the ORDER BY fragment for the given sort_by/sort_order,
// matching Python's logic exactly (ops.py:684-693).
// Default: "created_at DESC".
func sortClause(sortBy, sortOrder string) string {
	col, ok := validSortColumns[sortBy]
	if !ok {
		return "created_at DESC"
	}
	dir := "DESC"
	if sortOrder == "asc" {
		dir = "ASC"
	}
	return col + " " + dir
}

// whereArgs builds the WHERE fragment and argument slice shared by Search and Count.
// idx is the starting $N placeholder index (1-based). It returns the complete WHERE
// clause string (always non-empty — at minimum "TRUE"), the bound args, the next
// free placeholder index, and any error from auth filter expansion.
func whereArgs(in SearchInput, filters []*coreapi.AuthFilter) (string, []any, int, error) {
	args := []any{}
	wheres := []string{"TRUE"}
	idx := 1
	if in.Status != "" {
		wheres = append(wheres, fmt.Sprintf("status = $%d", idx))
		args = append(args, in.Status)
		idx++
	}
	if len(in.MetadataFilter) > 0 {
		wheres = append(wheres, fmt.Sprintf("metadata @> $%d::jsonb", idx))
		args = append(args, in.MetadataFilter)
		idx++
	}
	if len(in.ValuesFilter) > 0 {
		wheres = append(wheres, fmt.Sprintf("\"values\" @> $%d::jsonb", idx))
		args = append(args, in.ValuesFilter)
		idx++
	}
	// (C13) ids filter — ops.py:660-663: thread_id = ANY(%(ids)s)
	if len(in.Ids) > 0 {
		wheres = append(wheres, fmt.Sprintf("thread_id = ANY($%d::uuid[])", idx))
		args = append(args, in.Ids)
		idx++
	}
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", idx)
	if err != nil {
		return "", nil, 0, fmt.Errorf("auth: %w", err)
	}
	if authSQL != "" {
		wheres = append(wheres, authSQL)
		args = append(args, authArgs...)
		idx += len(authArgs)
	}
	return strings.Join(wheres, " AND "), args, idx, nil
}

// Search returns threads matching the given criteria.
// Sort column and direction follow Python ops.py:684-693 (whitelist validated).
func (s *Store) Search(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) ([]*Thread, error) {
	where, args, idx, err := whereArgs(in, filters)
	if err != nil {
		return nil, err
	}

	limit := in.Limit
	if limit == 0 {
		limit = 100
	}
	q := fmt.Sprintf(
		`SELECT %s FROM thread WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d`,
		threadCols,
		where,
		sortClause(in.SortBy, in.SortOrder),
		idx, idx+1,
	)
	args = append(args, limit, in.Offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Thread
	for rows.Next() {
		var t Thread
		if err := rows.Scan(&t.ThreadID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.ExpiresAt, &t.StateUpdatedAt, &t.Metadata, &t.Config, &t.Values, &t.Interrupts); err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

// Count returns the number of threads matching the given criteria.
func (s *Store) Count(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) (uint64, error) {
	where, args, _, err := whereArgs(in, filters)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM thread WHERE %s`, where)
	var n uint64
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// GetGraphID returns the graph_id from the thread's config->configurable->'graph_id' path.
// Returns ErrNotFound if no such thread exists (after applying auth filters).
func (s *Store) GetGraphID(ctx context.Context, threadID string, filters []*coreapi.AuthFilter) (string, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return "", fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(
		`SELECT COALESCE(config -> 'configurable' ->> 'graph_id', '') FROM thread WHERE thread_id = $1::uuid %s`,
		prefixWithAnd(authSQL),
	)
	var gid string
	row := s.pool.QueryRow(ctx, q, append([]any{threadID}, args...)...)
	if err := row.Scan(&gid); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return gid, nil
}

// prefixWithAnd prepends " AND " to a non-empty SQL fragment.
func prefixWithAnd(frag string) string {
	if frag == "" {
		return ""
	}
	return " AND " + frag
}

// CreateThreadInput carries the fields for a new thread row.
type CreateThreadInput struct {
	ThreadID   string   // optional; empty → gen_random_uuid()
	Metadata   []byte   // raw JSONB; nil → '{}'
	Config     []byte   // raw JSONB; nil → '{}'
	TTLSeconds *float64 // optional; nil → expires_at = NULL
	IfExists   string   // "do_nothing" | "raise" (default) — ops.py:832/848
}

// ErrAlreadyExists is returned by Create when if_exists=="raise" and the
// thread_id already exists (atomic detection, no TOCTOU).
var ErrAlreadyExists = errors.New("thread already exists")

// Create inserts a new thread row atomically.
//
// (C12) Mirrors Python ops.py:820-847: a single CTE that INSERTs with
// ON CONFLICT DO NOTHING, then UNION ALL-selects the existing row when
// if_exists=="do_nothing". This is one round-trip and race-free.
// if_exists=="raise" (or unset) returns ErrAlreadyExists when the thread_id
// already exists (the CTE returns an empty result set).
func (s *Store) Create(ctx context.Context, in CreateThreadInput) (*Thread, error) {
	if in.Metadata == nil {
		in.Metadata = []byte(`{}`)
	}
	if in.Config == nil {
		in.Config = []byte(`{}`)
	}

	// Determine thread_id placeholder and base args.
	var idSQL string
	var args []any
	if in.ThreadID != "" {
		idSQL = "$1::uuid"
		args = []any{in.ThreadID, in.Metadata, in.Config}
	} else {
		idSQL = "gen_random_uuid()"
		args = []any{in.Metadata, in.Config}
	}
	// $N indices for metadata and config in the current args slice.
	metaIdx := len(args) - 1
	configIdx := len(args)

	// Build the INSERT VALUES clause (with optional expires_at).
	var insertCols, insertVals string
	if in.TTLSeconds != nil {
		args = append(args, *in.TTLSeconds)
		ttlIdx := len(args)
		insertCols = "thread_id, metadata, config, status, created_at, updated_at, expires_at"
		insertVals = fmt.Sprintf(
			"%s, $%d::jsonb, $%d::jsonb, 'idle', now(), now(), now() + ($%d::float8 * interval '1 second')",
			idSQL, metaIdx, configIdx, ttlIdx,
		)
	} else {
		insertCols = "thread_id, metadata, config, status, created_at, updated_at"
		insertVals = fmt.Sprintf(
			"%s, $%d::jsonb, $%d::jsonb, 'idle', now(), now()",
			idSQL, metaIdx, configIdx,
		)
	}

	// (C12) Atomic CTE: INSERT ... ON CONFLICT DO NOTHING RETURNING *
	// UNION ALL SELECT existing row WHERE thread_id = $1 LIMIT 1.
	// The first leg returns a row only on a fresh insert; the second leg
	// returns the pre-existing row only on conflict. We tag each leg so we
	// can distinguish them after: "is_new = true" for the inserted row.
	//
	// When if_exists=="raise": omit the UNION ALL leg; an empty result set
	// means conflict → ErrAlreadyExists.
	//
	// Python reference: ops.py:820-847.
	var q string
	if in.IfExists == "do_nothing" && in.ThreadID != "" {
		// Use the thread_id arg (always $1 when ThreadID != "").
		q = fmt.Sprintf(`
WITH inserted AS (
    INSERT INTO thread (%s)
    VALUES (%s)
    ON CONFLICT (thread_id) DO NOTHING
    RETURNING %s
)
SELECT * FROM inserted
UNION ALL
SELECT %s FROM thread WHERE thread_id = $1::uuid AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1`,
			insertCols, insertVals, threadCols, threadCols,
		)
	} else {
		// raise mode (or no explicit thread_id for do_nothing): plain INSERT.
		// A duplicate key error from the DB propagates as-is.
		q = fmt.Sprintf(
			`INSERT INTO thread (%s) VALUES (%s) RETURNING %s`,
			insertCols, insertVals, threadCols,
		)
	}

	var t Thread
	row := s.pool.QueryRow(ctx, q, args...)
	if err := row.Scan(&t.ThreadID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.ExpiresAt, &t.StateUpdatedAt, &t.Metadata, &t.Config, &t.Values, &t.Interrupts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// raise mode conflict: CTE returned no rows.
			return nil, ErrAlreadyExists
		}
		// (C12) raise mode with no explicit thread_id: detect PG unique violation (23505).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrAlreadyExists
		}
		return nil, err
	}
	return &t, nil
}

// PatchThreadInput carries optional mutable fields for a thread update.
type PatchThreadInput struct {
	Metadata   []byte   // nil → no change
	Config     []byte   // nil → no change
	TTLSeconds *float64 // nil → no change; non-nil → set expires_at = now() + TTLSeconds
}

// Patch updates mutable thread fields. Returns the updated thread or ErrNotFound.
func (s *Store) Patch(ctx context.Context, threadID string, in PatchThreadInput, filters []*coreapi.AuthFilter) (*Thread, error) {
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	sets := []string{"updated_at = now()"}
	args := []any{threadID}
	idx := 2 + len(authArgs)
	if len(in.Metadata) > 0 {
		// (C10) Merge semantics — ops.py:883: set metadata = metadata || %(metadata)s
		sets = append(sets, fmt.Sprintf("metadata = metadata || $%d::jsonb", idx))
		args = append(args, in.Metadata)
		idx++
	}
	if len(in.Config) > 0 {
		sets = append(sets, fmt.Sprintf("config = $%d::jsonb", idx))
		args = append(args, in.Config)
		idx++
	}
	if in.TTLSeconds != nil {
		sets = append(sets, fmt.Sprintf("expires_at = now() + ($%d::float8 * interval '1 second')", idx))
		args = append(args, *in.TTLSeconds)
		idx++
	}
	_ = idx // suppress unused variable warning after last use
	finalArgs := append([]any{threadID}, authArgs...)
	finalArgs = append(finalArgs, args[1:]...)
	q := fmt.Sprintf(
		`UPDATE thread SET %s WHERE thread_id = $1::uuid%s RETURNING %s`,
		strings.Join(sets, ", "),
		prefixWithAnd(authSQL),
		threadCols,
	)
	var t Thread
	row := s.pool.QueryRow(ctx, q, finalArgs...)
	if err := row.Scan(&t.ThreadID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.ExpiresAt, &t.StateUpdatedAt, &t.Metadata, &t.Config, &t.Values, &t.Interrupts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// Delete removes the thread matching threadID (with optional auth filters).
// Returns the deleted thread UUID as a string, or ErrNotFound.
func (s *Store) Delete(ctx context.Context, threadID string, filters []*coreapi.AuthFilter) (string, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return "", fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(
		`DELETE FROM thread WHERE thread_id = $1::uuid%s RETURNING thread_id::text`,
		prefixWithAnd(authSQL),
	)
	var id string
	if err := s.pool.QueryRow(ctx, q, append([]any{threadID}, args...)...).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return id, nil
}

// SetStatusInput carries the inputs for the thread-only status update.
// Mirrors Python storage/ops.py Threads.set_status.
type SetStatusInput struct {
	ThreadID   string
	StatusText string // "idle" | "interrupted" | "error" — caller pre-computes
	ValuesJSON []byte // checkpoint["values"]; nil → SQL NULL (ops.py:936: Jsonb(checkpoint["values"]) if checkpoint else None)
	Interrupts []byte // computed from checkpoint tasks; nil → '{}'
}

// SetStatus sets the thread status, values, and interrupts following Python
// ops.py:916-944. The CASE WHEN EXISTS busy check mirrors Python exactly:
// if there are pending/running runs the final status is 'busy' and
// returns true (caller can then wake a worker — ops.py:940-944).
//
// Returns (busy bool, error).
func (s *Store) SetStatus(ctx context.Context, in SetStatusInput) (busy bool, err error) {
	interrupts := in.Interrupts
	if interrupts == nil {
		interrupts = []byte(`{}`)
	}
	// (C11) ops.py:916-931: UPDATE thread SET updated_at, values, interrupts, status=CASE...
	// values arg: nil → pass NULL (ops.py:936: None when checkpoint is None).
	var valuesArg any
	if in.ValuesJSON != nil {
		valuesArg = in.ValuesJSON
	}
	var finalStatus string
	row := s.pool.QueryRow(ctx, `
UPDATE thread SET
    updated_at  = now(),
    "values"    = $1::jsonb,
    interrupts  = $2::jsonb,
    status      = CASE
        WHEN EXISTS (
            SELECT 1 FROM run
            WHERE thread_id = $3::uuid
              AND status IN ('pending', 'running')
        ) THEN 'busy'
        ELSE $4
    END
WHERE thread_id = $3::uuid
RETURNING status`,
		valuesArg, interrupts, in.ThreadID, in.StatusText,
	)
	if scanErr := row.Scan(&finalStatus); scanErr != nil {
		// Thread not found is a no-op (internal call — no auth).
		return false, nil
	}
	return finalStatus == "busy", nil
}

// SetJointStatusInput carries the inputs for the atomic run + thread status update.
// Mirrors Python storage/ops.py Threads.set_joint_status.
type SetJointStatusInput struct {
	ThreadID       string
	RunID          string
	RunStatus      string // one of "pending","running","idle","interrupted","error","timeout","rollback"
	GraphID        string
	ValuesJSON     []byte   // checkpoint values blob; nil → SQL NULL (unconditionally overwrites, matches Python)
	InterruptsJSON []byte   // optional — checkpoint interrupts blob (nil → '{}')
	Next           []string // non-empty → base thread status "interrupted"
	ExceptionJSON  []byte   // optional — non-nil indicates an exception; treated as "error" base status
	// R3 simplification: no UserInterrupt/UserRollback discrimination yet;
	// that nuance is deferred until exception typing is propagated through the proto.
}

// SetJointStatus atomically updates run.status (or deletes the run on "rollback")
// and thread.status / values / interrupts / metadata.{graph_id} in one transaction.
// Internal-only — no auth filters per Python semantics.
func (s *Store) SetJointStatus(ctx context.Context, in SetJointStatusInput) error {
	// Compute base thread status.
	// R3 simplification: any exception → "error". Python further distinguishes
	// UserInterrupt/UserRollback; that nuance is deferred until exception
	// typing is propagated through the proto in a later phase.
	var baseStatus string
	switch {
	case len(in.ExceptionJSON) > 0:
		baseStatus = "error"
	case len(in.Next) > 0:
		baseStatus = "interrupted"
	default:
		baseStatus = "idle"
	}

	interrupts := in.InterruptsJSON
	if interrupts == nil {
		interrupts = []byte(`{}`)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if in.RunStatus == "rollback" {
		if _, err := tx.Exec(ctx,
			`DELETE FROM run WHERE run_id = $1::uuid AND thread_id = $2::uuid`,
			in.RunID, in.ThreadID,
		); err != nil {
			return fmt.Errorf("delete run: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx,
			`UPDATE run SET status = $1, updated_at = now()
             WHERE run_id = $2::uuid AND thread_id = $3::uuid`,
			in.RunStatus, in.RunID, in.ThreadID,
		); err != nil {
			return fmt.Errorf("update run: %w", err)
		}
	}

	// valuesArg: nil means pass NULL to the DB (keeps existing if caller passes nil).
	// R3: caller is responsible for passing the current values when no checkpoint
	// update is intended (matches Python which unconditionally writes the column).
	var valuesArg any
	if in.ValuesJSON != nil {
		valuesArg = in.ValuesJSON
	}

	graphMeta := []byte(fmt.Sprintf(`{"graph_id":%q}`, in.GraphID))

	if _, err := tx.Exec(ctx,
		`UPDATE thread SET
            updated_at = now(),
            "values"   = $1::jsonb,
            interrupts = $2::jsonb,
            metadata   = metadata || $3::jsonb,
            status     = CASE
                WHEN EXISTS (
                    SELECT 1 FROM run
                    WHERE thread_id = $4::uuid AND status IN ('pending','running')
                ) THEN 'busy'
                ELSE $5
            END
         WHERE thread_id = $4::uuid`,
		valuesArg, interrupts, graphMeta, in.ThreadID, baseStatus,
	); err != nil {
		return fmt.Errorf("update thread: %w", err)
	}

	return tx.Commit(ctx)
}

// Copy inserts a new thread row that is a row-only clone of the source thread
// (same metadata/config/values/status; new UUID, new created_at/updated_at).
// Checkpoint tables are NOT copied — see TODO(R5) below.
//
// TODO(R5): copy checkpoints, checkpoint_blobs, checkpoint_writes via internal/checkpointer
// after the Checkpointer service is implemented in R5.
func (s *Store) Copy(ctx context.Context, srcThreadID string, filters []*coreapi.AuthFilter) (*Thread, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(`
		INSERT INTO thread (thread_id, metadata, config, "values", interrupts, status, created_at, updated_at)
		SELECT gen_random_uuid(), metadata, config, "values", interrupts, status, now(), now()
		FROM thread WHERE thread_id = $1::uuid%s
		RETURNING %s`,
		prefixWithAnd(authSQL),
		threadCols,
	)
	var t Thread
	row := s.pool.QueryRow(ctx, q, append([]any{srcThreadID}, args...)...)
	if err := row.Scan(&t.ThreadID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.ExpiresAt, &t.StateUpdatedAt, &t.Metadata, &t.Config, &t.Values, &t.Interrupts); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// ThreadExistsAndAuth checks that the thread with threadID exists and satisfies
// the provided auth filters applied to thread.metadata.
//
// Returns:
//   - nil if the row exists and passes filters.
//   - ErrNotFound if no row matches.
//   - ErrForbidden if the row exists but auth filters exclude it.
//   - other errors for database failures.
func (s *Store) ThreadExistsAndAuth(ctx context.Context, threadID string, filters []*coreapi.AuthFilter) error {
	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid)`,
		threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	if len(filters) == 0 {
		return nil
	}
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	var passes bool
	q := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid AND %s)`,
		authSQL,
	)
	allArgs := append([]any{threadID}, authArgs...)
	if err := s.pool.QueryRow(ctx, q, allArgs...).Scan(&passes); err != nil {
		return err
	}
	if !passes {
		return ErrForbidden
	}
	return nil
}

