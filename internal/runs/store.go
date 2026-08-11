package runs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a run is not found or is hidden by auth filters.
var ErrNotFound = errors.New("run not found")

// Store provides read-only access to the run table.
type Store struct {
	pool     *pgxpool.Pool
	leaseTTL int64 // lease duration in seconds; used by ExtendLease and Next
}

// NewStore constructs a Store backed by the given connection pool.
// leaseTTL controls how long a lease is granted/extended (seconds).
// Pass 0 to use the default of 30 seconds (matches LSD_LEASE_TTL_SECONDS default).
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool, leaseTTL: 30} }

// NewStoreWithLeaseTTL constructs a Store with an explicit lease TTL in seconds.
func NewStoreWithLeaseTTL(pool *pgxpool.Pool, leaseTTLSecs int64) *Store {
	if leaseTTLSecs <= 0 {
		leaseTTLSecs = 30
	}
	return &Store{pool: pool, leaseTTL: leaseTTLSecs}
}

// Run is the internal representation of a run row.
type Run struct {
	RunID             string
	ThreadID          string
	AssistantID       string
	Status            string
	Kwargs            []byte
	MultitaskStrategy string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Metadata          []byte
	LeaseGeneration   int64
	RunAfter          *time.Time // NULL when after_seconds was 0; deferred-start runs skipped by Next until reached
	Attempt           int64      // (2k) number of times Next has claimed this run; distinct from LeaseGeneration fencing
}

// runCols is the SELECT projection for all Run fields, qualified with the
// run. prefix: several callers now JOIN thread, which has same-named
// columns (status, created_at, updated_at, metadata, thread_id) that would
// otherwise be ambiguous.
const runCols = `run.run_id::text, COALESCE(run.thread_id::text,''), COALESCE(run.assistant_id::text,''),
	COALESCE(run.status,''),
	COALESCE(run.kwargs, '{}'::jsonb)::text::bytea,
	COALESCE(run.multitask_strategy, ''),
	run.created_at, run.updated_at,
	COALESCE(run.metadata, '{}'::jsonb)::text::bytea,
	COALESCE(run.lease_generation, 0),
	run.run_after,
	COALESCE(run.attempt, 0)`

// Get returns the run with the given UUID string, optionally scoped to a
// thread, and applying auth filters to the thread's metadata column.
// Mirrors ops.py:1681-1710, which always JOINs thread USING (thread_id) and
// filters on thread.metadata (table_alias="thread").
// Returns ErrNotFound if there is no matching row.
func (s *Store) Get(ctx context.Context, runID, threadID string, filters []*coreapi.AuthFilter) (*Run, error) {
	if threadID != "" {
		authSQL, args, err := auth.ApplyToQuery(filters, "thread.metadata", 3)
		if err != nil {
			return nil, fmt.Errorf("auth: %w", err)
		}
		q := fmt.Sprintf(
			`SELECT %s FROM run JOIN thread USING (thread_id) WHERE run.run_id = $1::uuid AND run.thread_id = $2::uuid%s`,
			runCols, prefixAnd(authSQL),
		)
		allArgs := append([]any{runID, threadID}, args...)
		row := s.pool.QueryRow(ctx, q, allArgs...)
		var r Run
		if err := scanRun(row, &r); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		return &r, nil
	}

	authSQL, args, err := auth.ApplyToQuery(filters, "thread.metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(`SELECT %s FROM run JOIN thread USING (thread_id) WHERE run.run_id = $1::uuid%s`, runCols, prefixAnd(authSQL))
	allArgs := append([]any{runID}, args...)
	row := s.pool.QueryRow(ctx, q, allArgs...)
	var r Run
	if err := scanRun(row, &r); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &r, nil
}

// SearchInput carries the optional filter parameters for Search and Count.
type SearchInput struct {
	ThreadID       string
	Statuses       []string // matches any of (status = ANY($N::text[]))
	MetadataFilter []byte   // optional JSONB containment filter (@>)
	Limit          uint64
	Offset         uint64
}

// whereArgs builds the JOIN and WHERE fragments shared by Search and Count.
// idx is the starting $N placeholder index (1-based). It returns the JOIN
// fragment (empty, or "JOIN thread USING (thread_id)"), the complete WHERE
// clause string (always non-empty — at minimum "TRUE"), the bound args, the
// next free placeholder index, and any error from auth filter expansion.
//
// (ops.py:1928-1931) Auth filters apply to thread.metadata; thread is only
// joined when filters are present, so a run whose thread was deleted is
// excluded exactly when ops.py's INNER JOIN would exclude it (no behavior
// change when no filters are given).
func whereArgs(in SearchInput, filters []*coreapi.AuthFilter) (string, string, []any, int, error) {
	args := []any{}
	wheres := []string{"TRUE"}
	idx := 1

	if in.ThreadID != "" {
		wheres = append(wheres, fmt.Sprintf("run.thread_id = $%d::uuid", idx))
		args = append(args, in.ThreadID)
		idx++
	}
	if len(in.Statuses) > 0 {
		wheres = append(wheres, fmt.Sprintf("run.status = ANY($%d::text[])", idx))
		args = append(args, in.Statuses)
		idx++
	}
	if len(in.MetadataFilter) > 0 {
		wheres = append(wheres, fmt.Sprintf("run.metadata @> $%d::jsonb", idx))
		args = append(args, in.MetadataFilter)
		idx++
	}

	authSQL, authArgs, err := auth.ApplyToQuery(filters, "thread.metadata", idx)
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("auth: %w", err)
	}
	join := ""
	if authSQL != "" {
		join = "JOIN thread USING (thread_id)"
		wheres = append(wheres, authSQL)
		args = append(args, authArgs...)
		idx += len(authArgs)
	}

	return join, strings.Join(wheres, " AND "), args, idx, nil
}

// Search returns runs matching the given criteria, ordered by created_at DESC with run_id tiebreaker.
func (s *Store) Search(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) ([]*Run, error) {
	join, where, args, idx, err := whereArgs(in, filters)
	if err != nil {
		return nil, err
	}

	limit := in.Limit
	if limit == 0 {
		limit = 100
	}
	q := fmt.Sprintf(
		`SELECT %s FROM run %s WHERE %s ORDER BY run.created_at DESC, run.run_id LIMIT $%d OFFSET $%d`,
		runCols,
		join,
		where,
		idx, idx+1,
	)
	args = append(args, limit, in.Offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Run
	for rows.Next() {
		var r Run
		if err := scanRun(rows, &r); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

// Count returns the number of runs matching the given criteria.
func (s *Store) Count(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) (uint64, error) {
	join, where, args, _, err := whereArgs(in, filters)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM run %s WHERE %s`, join, where)
	var n uint64
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Stats holds aggregate counts of runs by status.
type Stats struct {
	NPending                    uint64
	NRunning                    uint64
	PendingWaitMaxSecs          *float64 // longest current wait of a pending run, in seconds; nil when none pending
	PendingWaitMedSecs          *float64 // median wait of pending runs, in seconds; nil when none pending
	PendingUnblockedWaitMaxSecs *float64 // max wait of pending runs whose thread has no running run; nil when none
}

// Stats returns aggregate run counts for the pending and running statuses,
// along with wait-time metrics for pending runs.
//
// (ops.py:1322-1340) Python stats() computes:
//   - n_pending  = COUNT(*) FILTER (WHERE status = 'pending')
//   - n_running  = COUNT(*) FILTER (WHERE status = 'running')
//   - min_age_secs = MIN(now()-created_at) over status IN ('pending','running')
//   - med_age_secs = percentile_cont(0.5)  over status IN ('pending','running')
//
// The bridge (api/grpc/ops/runs.py:693-710) maps the proto fields
// pending_runs_wait_time_max_secs / pending_runs_wait_time_med_secs
// directly from the proto — so we populate those fields from the same
// ('pending','running') population as Python. PendingWaitMaxSecs maps
// to pending_runs_wait_time_max_secs (MAX = longest wait = oldest run).
// Python names it "min_age_secs" (MIN age-interval = shortest-lived = most
// recent), but the proto contract exposes it as the "max wait time" metric;
// we use MAX(now()-created_at) so the proto semantics are self-consistent
// and the Go field name matches the proto name. See report for full analysis.
func (s *Store) Stats(ctx context.Context) (*Stats, error) {
	var st Stats
	// (ops.py:1326-1331) Filter: status IN ('pending', 'running') — matches Python reference.
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'running'),
			EXTRACT(EPOCH FROM MAX(now() - created_at) FILTER (WHERE status IN ('pending','running'))),
			EXTRACT(EPOCH FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY (now() - created_at)) FILTER (WHERE status IN ('pending','running')))
		FROM run
		WHERE status IN ('pending', 'running')`,
	).Scan(
		&st.NPending,
		&st.NRunning,
		&st.PendingWaitMaxSecs,
		&st.PendingWaitMedSecs,
	)
	if err != nil {
		return nil, err
	}

	err = s.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM MAX(now() - r.created_at))
		FROM run r
		WHERE r.status = 'pending'
		  AND NOT EXISTS (
		      SELECT 1 FROM run r2
		      WHERE r2.thread_id = r.thread_id
		        AND r2.status = 'running'
		  )`,
	).Scan(&st.PendingUnblockedWaitMaxSecs)
	if err != nil {
		return nil, err
	}

	return &st, nil
}

// scanRun populates a Run from a pgx row/rows.
func scanRun(row pgx.Row, r *Run) error {
	return row.Scan(
		&r.RunID,
		&r.ThreadID,
		&r.AssistantID,
		&r.Status,
		&r.Kwargs,
		&r.MultitaskStrategy,
		&r.CreatedAt,
		&r.UpdatedAt,
		&r.Metadata,
		&r.LeaseGeneration,
		&r.RunAfter,
		&r.Attempt,
	)
}

// prefixAnd prepends " AND " to a non-empty SQL fragment.
func prefixAnd(frag string) string {
	if frag == "" {
		return ""
	}
	return " AND " + frag
}

// ErrForbidden is returned when an auth filter excludes the matching row.
var ErrForbidden = errors.New("run forbidden by auth filters")

// ErrInflight is returned when Create is called with multitask_strategy="reject"
// and an inflight run (pending or running) already exists on the thread.
var ErrInflight = errors.New("run inflight on thread")

// PublishExistsAndAuth checks that a run with the given runID and threadID exists
// and satisfies the provided auth filters applied to the thread's metadata column.
//
// Returns:
//   - nil if the row exists and passes filters.
//   - ErrNotFound if no row matches (run or thread missing, or threadID mismatch).
//   - ErrForbidden if the run/thread row exists but auth filters exclude it.
//   - other errors for database failures.
//
// This is used by Runs.Publish to guard the write before appending to the Redis stream.
// Auth filters apply to thread.metadata (the caller's visibility scope).
func (s *Store) PublishExistsAndAuth(ctx context.Context, runID, threadID string, filters []*coreapi.AuthFilter) error {
	// First check existence without auth filters.
	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM run
			JOIN thread USING (thread_id)
			WHERE run.run_id = $1::uuid AND run.thread_id = $2::uuid
		)`,
		runID, threadID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}

	// If no filters, we're done.
	if len(filters) == 0 {
		return nil
	}

	// Check with auth filters applied to thread.metadata.
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "thread.metadata", 3)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	var authPasses bool
	q := fmt.Sprintf(
		`SELECT EXISTS(
			SELECT 1 FROM run
			JOIN thread USING (thread_id)
			WHERE run.run_id = $1::uuid AND run.thread_id = $2::uuid AND %s
		)`,
		authSQL,
	)
	allArgs := append([]any{runID, threadID}, authArgs...)
	if err := s.pool.QueryRow(ctx, q, allArgs...).Scan(&authPasses); err != nil {
		return err
	}
	if !authPasses {
		return ErrForbidden
	}
	return nil
}

// ExtendLease refreshes lease_expires_at for a running run; called by the
// Enter heartbeat goroutine. If holderID is empty the holder check is skipped
// (Enter today does not set lease_holder_id; Next does not populate it yet).
// A non-empty holderID requires the row's lease_holder_id to match, guarding
// against stolen leases. Returns ErrNotFound if no row was updated.
//
// (item 6) Lease duration is now taken from s.leaseTTL (wired from
// LSD_LEASE_TTL_SECONDS via NewStoreWithLeaseTTL) instead of being hardcoded.
func (s *Store) ExtendLease(ctx context.Context, runID, holderID string) error {
	leaseSecs := s.leaseTTL // (item 6) from config, default 30s (LSD_LEASE_TTL_SECONDS)

	var tag pgconn.CommandTag
	var err error

	if holderID == "" {
		tag, err = s.pool.Exec(ctx,
			`UPDATE run
			 SET lease_expires_at = now() + ($2::int * INTERVAL '1 second'),
			     updated_at = now()
			 WHERE run_id = $1::uuid AND status = 'running'`,
			runID, leaseSecs,
		)
	} else {
		tag, err = s.pool.Exec(ctx,
			`UPDATE run
			 SET lease_expires_at = now() + ($3::int * INTERVAL '1 second'),
			     updated_at = now()
			 WHERE run_id = $1::uuid
			   AND lease_holder_id = $2
			   AND status = 'running'`,
			runID, holderID, leaseSecs,
		)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// newUUID generates a new random UUID v4 string for thread auto-creation.
func newUUID() string { return uuid.New().String() }

// CreateRunInput carries the fields for a new run row.
type CreateRunInput struct {
	RunID             string // optional; empty → gen_random_uuid()
	ThreadID          string // optional when IfNotExists=CREATE_THREAD (generates new UUID)
	AssistantID       string
	Status            string // default "pending"
	KwargsJSON        []byte
	Metadata          []byte
	MultitaskStrategy string // reject | rollback | interrupt | enqueue
	AfterSeconds      uint64 // 0 → run immediately (run_after stays NULL)

	// (item 1) user_id injected into kwargs.config.configurable.user_id.
	// Precedence: request.user_id < thread.config > assistant.config (ops.py:1605-1610 COALESCE).
	UserID string // optional; empty → no injection (COALESCE yields NULL)

	// (item 3) if_not_exists: when CREATE_THREAD_IF_THREAD_NOT_EXISTS, a missing
	// thread is auto-created from the assistant's config (ops.py:1527-1558 CTE).
	// Zero value (0) = REJECT_RUN_IF_THREAD_NOT_EXISTS.
	IfNotExists int32 // coreapi.CreateRunBehavior enum value
}

// CreateResult is the return value of Create. InflightRunIDs holds the IDs of
// runs that were pending/running on the thread at create time under a
// "rollback"/"interrupt" multitask strategy (ops.py:1834-1899 semantics) —
// Create itself does not mutate or signal them; the caller (service.Create)
// applies the same action Cancel would (2d).
type CreateResult struct {
	Run            *Run
	InflightRunIDs []string
}

// Create inserts a new run row, applying the multitask strategy logic inside a
// single transaction. Both threadFilters and assistantFilters are validated via
// EXISTS sub-queries at the SQL level.
//
// Implements items 1, 2, and 3 from the parity gap spec:
//   - (item 1) user_id is injected into kwargs.config.configurable.user_id using
//     the Python COALESCE precedence (ops.py:1605-1610):
//     request.kwargs.config.configurable.user_id > thread.config.configurable.user_id
//     > assistant.config.configurable.user_id > request.user_id
//   - (item 2) metadata.assistant_id is set via setdefault semantics (ops.py:1502):
//     caller-provided metadata.assistant_id wins; otherwise assistant_id is injected.
//   - (item 3) if_not_exists=CREATE_THREAD_IF_THREAD_NOT_EXISTS, OR an empty
//     ThreadID, auto-creates the thread from assistant config/metadata if it
//     does not exist (ops.py:1527-1560).
func (s *Store) Create(
	ctx context.Context,
	in CreateRunInput,
	threadFilters []*coreapi.AuthFilter,
	assistantFilters []*coreapi.AuthFilter,
) (*CreateResult, error) {
	if in.Status == "" {
		in.Status = "pending"
	}
	if in.KwargsJSON == nil {
		in.KwargsJSON = []byte(`{}`)
	}
	if in.Metadata == nil {
		in.Metadata = []byte(`{}`)
	}
	if in.MultitaskStrategy == "" {
		in.MultitaskStrategy = "reject"
	}

	// (item 3 / 2b) Determine if thread auto-creation is requested.
	// CreateRunBehavior_CREATE_THREAD_IF_THREAD_NOT_EXISTS = 1, OR an empty
	// thread_id (ops.py:1527-1560: thread is auto-created when thread_id is
	// None *or* if_not_exists == "create").
	createThreadIfMissing := in.IfNotExists == 1 || in.ThreadID == ""

	// (2c) Request config extracted once from kwargs.config, reused both for
	// auto-creating a thread and for updating an existing thread below.
	requestConfig := func() []byte {
		var kw map[string]any
		if err2 := json.Unmarshal(in.KwargsJSON, &kw); err2 == nil {
			if cfg, ok := kw["config"]; ok {
				if b, err3 := json.Marshal(cfg); err3 == nil {
					return b
				}
			}
		}
		return []byte(`{}`)
	}()

	// Generate a thread ID if none was provided (Python: thread_id or uuid4()).
	threadID := in.ThreadID
	if threadID == "" {
		threadID = newUUID()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Auth-guard: validate assistant exists and passes assistant_filters.
	// (Must happen before thread check so we can use assistant data in auto-create.)
	asAuthSQL, asArgs, err := auth.ApplyToQuery(assistantFilters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("assistant auth: %w", err)
	}
	var asExists bool
	asQ := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM assistant WHERE assistant_id = $1::uuid%s)`,
		prefixAnd(asAuthSQL),
	)
	if err := tx.QueryRow(ctx, asQ, append([]any{in.AssistantID}, asArgs...)...).Scan(&asExists); err != nil {
		return nil, err
	}
	if !asExists {
		return nil, ErrNotFound
	}

	// Auth-guard: validate thread exists (and passes thread_filters).
	// (item 3) If createThreadIfMissing: auto-create the thread from assistant config
	// when it does not exist, mirroring Python's inserted_thread CTE (ops.py:1527-1558).
	thAuthSQL, thArgs, err := auth.ApplyToQuery(threadFilters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("thread auth: %w", err)
	}
	var thExists bool
	thQ := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM thread WHERE thread_id = $1::uuid%s)`,
		prefixAnd(thAuthSQL),
	)
	if err := tx.QueryRow(ctx, thQ, append([]any{threadID}, thArgs...)...).Scan(&thExists); err != nil {
		return nil, err
	}
	if !thExists {
		if !createThreadIfMissing {
			return nil, ErrNotFound
		}
		// (item 3 / 2b) Auto-create the thread (ops.py:1527-1548 inserted_thread CTE).
		// Thread metadata is seeded with graph_id+assistant_id from the assistant row.
		// Thread config is seeded from assistant config merged with request config.
		// Status is 'busy' (ops.py:1530), not 'idle' — the thread is about to host a run.
		if _, err := tx.Exec(ctx, `
			INSERT INTO thread (thread_id, status, metadata, config, created_at, updated_at)
			SELECT
				$1::uuid,
				'busy',
				jsonb_build_object(
					'graph_id',      a.graph_id,
					'assistant_id',  a.assistant_id
				) || $2::jsonb,
				a.config
					|| $3::jsonb
					|| jsonb_build_object(
						'configurable',
							COALESCE(a.config -> 'configurable', '{}'::jsonb) ||
							COALESCE($3::jsonb -> 'configurable', '{}'::jsonb)
					),
				now(), now()
			FROM assistant a
			WHERE a.assistant_id = $4::uuid
			ON CONFLICT (thread_id) DO NOTHING`,
			threadID,
			in.Metadata,
			requestConfig,
			in.AssistantID,
		); err != nil {
			return nil, fmt.Errorf("auto-create thread: %w", err)
		}
	}

	// Store effective threadID back for multitask checks below.
	in.ThreadID = threadID

	// (2c) Update an existing thread's metadata/config/status on run create
	// (ops.py:1608-1660 updated_thread CTE). The `status != 'busy'` guard makes
	// this a no-op for threads just auto-created above (already 'busy').
	if _, err := tx.Exec(ctx, `
		UPDATE thread SET
			metadata = jsonb_set(
				jsonb_set(thread.metadata, '{graph_id}', to_jsonb(assistant.graph_id)),
				'{assistant_id}',
				to_jsonb(assistant.assistant_id)
			),
			config = assistant.config
				|| thread.config
				|| $3::jsonb
				|| jsonb_build_object(
					'configurable',
						COALESCE(assistant.config -> 'configurable', '{}'::jsonb) ||
						COALESCE(thread.config -> 'configurable', '{}'::jsonb) ||
						COALESCE($3::jsonb -> 'configurable', '{}'::jsonb)
					),
			status = 'busy'
		FROM assistant
		WHERE thread.thread_id = $1::uuid
		  AND assistant.assistant_id = $2::uuid
		  AND thread.status != 'busy'`,
		in.ThreadID, in.AssistantID, requestConfig,
	); err != nil {
		return nil, fmt.Errorf("update thread on run create: %w", err)
	}

	// (2d) Multitask strategy: capture (without mutating) any existing inflight
	// runs so the caller (service.Create) can apply the same action Cancel
	// would (interrupt/rollback), including publishing Redis signals.
	var inflightIDs []string
	switch in.MultitaskStrategy {
	case "reject":
		var inflight bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM run WHERE thread_id = $1::uuid AND status IN ('pending','running'))`,
			in.ThreadID,
		).Scan(&inflight); err != nil {
			return nil, err
		}
		if inflight {
			return nil, ErrInflight
		}
	case "rollback", "interrupt":
		rows, err := tx.Query(ctx,
			`SELECT run_id::text FROM run WHERE thread_id = $1::uuid AND status IN ('pending', 'running')`,
			in.ThreadID,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			inflightIDs = append(inflightIDs, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	case "enqueue":
		// no pre-flight check; allow concurrent runs on the same thread
	}

	// Build and execute the INSERT.
	//
	// The INSERT merges assistant.config, thread.config, and the user-supplied
	// kwargs.config into kwargs.config, then injects graph_id, run_id, thread_id,
	// assistant_id, and user_id into kwargs.config.configurable.
	//
	// (item 1) user_id COALESCE precedence (ops.py:1605-1610):
	//   1. kwargs.config.configurable.user_id (request-supplied)
	//   2. thread.config.configurable.user_id
	//   3. assistant.config.configurable.user_id
	//   4. $8 (CreateRunInput.UserID / CreateRunRequest.user_id)
	//
	// (item 2) metadata.assistant_id setdefault (ops.py:1502):
	//   $7::jsonb already carries caller metadata; we inject assistant_id only when
	//   the caller did not provide it, using jsonb_build_object || $7 so caller wins.
	insertArgs := []any{
		in.RunID,    // $1 — empty string ⇒ generate
		in.ThreadID, // $2
		in.AssistantID, // $3
		in.Status,      // $4
		in.KwargsJSON,  // $5
		in.MultitaskStrategy, // $6
		in.Metadata,          // $7
		in.UserID,            // $8 — user_id fallback (item 1)
	}
	runAfterExpr := "NULL"
	// (2n) created_at defaults to now(); when after_seconds > 0 both run_after
	// and created_at are pushed to the same future instant (ops.py:1573 sets
	// created_at from the deferred start time). Reusing the identical
	// expression/param for both columns guarantees equal values: now() is
	// stable within a single statement/transaction in Postgres.
	createdAtExpr := "now()"
	if in.AfterSeconds > 0 {
		nextArg := len(insertArgs) + 1
		runAfterExpr = fmt.Sprintf("(now() + ($%d::bigint * interval '1 second'))", nextArg)
		createdAtExpr = runAfterExpr
		insertArgs = append(insertArgs, int64(in.AfterSeconds))
	}
	q := fmt.Sprintf(
		`WITH new_id AS (
			SELECT COALESCE(NULLIF($1, '')::uuid, gen_random_uuid()) AS run_id
		)
		INSERT INTO run (
			run_id, thread_id, assistant_id, status, kwargs, multitask_strategy, metadata,
			run_after, created_at, updated_at
		)
		SELECT
			new_id.run_id,
			t.thread_id,
			a.assistant_id,
			$4,
			$5::jsonb || jsonb_build_object(
				'config',
				a.config
					|| t.config
					|| COALESCE($5::jsonb -> 'config', '{}'::jsonb)
					|| jsonb_build_object(
						'configurable',
							COALESCE(a.config -> 'configurable', '{}'::jsonb)
								|| COALESCE(t.config -> 'configurable', '{}'::jsonb)
								|| COALESCE($5::jsonb -> 'config' -> 'configurable', '{}'::jsonb)
								|| jsonb_build_object(
									'run_id',       new_id.run_id::text,
									'thread_id',    t.thread_id::text,
									'graph_id',     a.graph_id,
									'assistant_id', a.assistant_id::text,
									'user_id', COALESCE(
										$5::jsonb -> 'config' -> 'configurable' ->> 'user_id',
										t.config -> 'configurable' ->> 'user_id',
										a.config -> 'configurable' ->> 'user_id',
										NULLIF($8, '')
									)
								),
						'metadata',
							a.metadata || t.metadata || $7::jsonb
					)
			),
			$6,
			jsonb_build_object('assistant_id', a.assistant_id::text) || $7::jsonb,
			%s,
			%s,
			now()
		FROM new_id, assistant a, thread t
		WHERE a.assistant_id = $3::uuid
		  AND t.thread_id    = $2::uuid
		RETURNING %s`,
		runAfterExpr,
		createdAtExpr,
		runCols,
	)
	var r Run
	if err := scanRun(tx.QueryRow(ctx, q, insertArgs...), &r); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &CreateResult{Run: &r, InflightRunIDs: inflightIDs}, nil
}

// Delete removes the run matching runID (optionally scoped to threadID + auth filters).
// Returns the deleted run UUID or ErrNotFound.
//
// (item 4) Mirrors Python ops.py:1743-1755 del_checkpoint_writes CTE:
// checkpoint_writes rows belonging to the run's checkpoints are removed before
// the run row is deleted (checkpoints themselves cascade via FK on run_id).
func (s *Store) Delete(ctx context.Context, runID, threadID string, filters []*coreapi.AuthFilter) (string, error) {
	var q string
	var args []any
	if threadID != "" {
		authSQL, authArgs, err := auth.ApplyToQuery(filters, "thread.metadata", 3)
		if err != nil {
			return "", fmt.Errorf("auth: %w", err)
		}
		// (ops.py:1726-1732) thread is only joined when filter_params is non-empty.
		join := ""
		if authSQL != "" {
			join = "JOIN thread USING (thread_id)"
		}
		// (item 4) CTE: first select the run, then delete orphaned checkpoint_writes
		// for the run's checkpoints, then delete the run row itself.
		// ops.py:1736-1760: selected → del_checkpoint_writes → DELETE FROM run.
		q = fmt.Sprintf(`
			WITH selected AS (
				SELECT run_id FROM run %s
				WHERE run_id = $1::uuid AND thread_id = $2::uuid%s
			),
			del_checkpoint_writes AS (
				DELETE FROM checkpoint_writes
				USING selected
				INNER JOIN checkpoints
					ON checkpoints.run_id = selected.run_id
				WHERE checkpoint_writes.checkpoint_id = checkpoints.checkpoint_id
				  AND checkpoint_writes.thread_id     = checkpoints.thread_id
				  AND checkpoint_writes.checkpoint_ns = checkpoints.checkpoint_ns
			)
			DELETE FROM run USING selected WHERE run.run_id = selected.run_id
			RETURNING run.run_id::text`,
			join, prefixAnd(authSQL),
		)
		args = append([]any{runID, threadID}, authArgs...)
	} else {
		authSQL, authArgs, err := auth.ApplyToQuery(filters, "thread.metadata", 2)
		if err != nil {
			return "", fmt.Errorf("auth: %w", err)
		}
		join := ""
		if authSQL != "" {
			join = "JOIN thread USING (thread_id)"
		}
		// (item 4) Same CTE without thread_id scope.
		q = fmt.Sprintf(`
			WITH selected AS (
				SELECT run_id FROM run %s
				WHERE run_id = $1::uuid%s
			),
			del_checkpoint_writes AS (
				DELETE FROM checkpoint_writes
				USING selected
				INNER JOIN checkpoints
					ON checkpoints.run_id = selected.run_id
				WHERE checkpoint_writes.checkpoint_id = checkpoints.checkpoint_id
				  AND checkpoint_writes.thread_id     = checkpoints.thread_id
				  AND checkpoint_writes.checkpoint_ns = checkpoints.checkpoint_ns
			)
			DELETE FROM run USING selected WHERE run.run_id = selected.run_id
			RETURNING run.run_id::text`,
			join, prefixAnd(authSQL),
		)
		args = append([]any{runID}, authArgs...)
	}
	var id string
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return id, nil
}

// CancelResult carries the outcome of a Cancel call for a single run.
// RunID is the affected run's UUID; Deleted is true when a pending+rollback run
// was hard-deleted (Python ops.py:1873-1879) rather than transitioned.
type CancelResult struct {
	RunID   string
	Deleted bool
	// Terminal is true when this row just transitioned to a terminal run
	// status as a result of this call (rollback-delete, or interrupt on a
	// pending row). It is false for the "running" branch, where only
	// cancel_requested_at was set and the worker has yet to transition the
	// run — the caller uses this to decide whether to publish to the run's
	// terminal-done channel (2f-ii).
	Terminal bool
}

// Cancel implements the Python ops.py:1797-1877 cancel semantics per run:
//   - pending + rollback  → DELETE the run row (hard delete)
//   - pending + interrupt → status = 'interrupted', cancel_requested_at = now()
//   - running  (any)      → only cancel_requested_at = now() (worker transitions)
//
// Auth filters are applied to the run's thread metadata (ops.py:1824-1832).
// Returns the list of CancelResults for matched runs so the caller can
// publish Redis signals for affected run IDs.
func (s *Store) Cancel(ctx context.Context, runIDs []string, threadID string, filters []*coreapi.AuthFilter) ([]string, error) {
	results, err := s.CancelWithAction(ctx, runIDs, threadID, "interrupt", filters)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, r.RunID)
	}
	return out, nil
}

// CancelWithAction cancels runs with explicit action semantics matching Python
// ops.py:1834-1899:
//   - pending + action="rollback"  → DELETE (hard delete; ops.py:1873)
//   - pending + action="interrupt" → status='interrupted', cancel_requested_at=now() (ops.py:1864)
//   - running  (any action)        → cancel_requested_at=now() only (worker transitions; ops.py:1843)
//
// Returns CancelResult per affected run so the service layer can signal Redis.
func (s *Store) CancelWithAction(ctx context.Context, runIDs []string, threadID, action string, filters []*coreapi.AuthFilter) ([]CancelResult, error) {
	if len(runIDs) == 0 {
		return nil, nil
	}
	if action == "" {
		action = "interrupt"
	}

	// Build WHERE fragment shared by all sub-queries.
	baseArgs := []any{}
	baseWheres := []string{}
	if threadID != "" {
		baseArgs = append(baseArgs, threadID)
		baseWheres = append(baseWheres, fmt.Sprintf("thread_id = $%d::uuid", len(baseArgs)))
	}
	// (ops.py:1824-1832) Auth filters apply to thread.metadata; thread would be
	// JOINed only when filters are present. run/UPDATE/DELETE targets can't
	// carry an extra JOIN, so we express the same INNER-JOIN exclusion via a
	// correlated EXISTS against thread (thread_id is thread's PK, so at most
	// one matching row — same semantics as the JOIN).
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "thread.metadata", len(baseArgs)+1)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	if authSQL != "" {
		baseWheres = append(baseWheres, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM thread WHERE thread.thread_id = run.thread_id AND %s)", authSQL,
		))
		baseArgs = append(baseArgs, authArgs...)
	}
	baseArgs = append(baseArgs, runIDs)
	runIDPlaceholder := fmt.Sprintf("$%d::uuid[]", len(baseArgs))
	baseWheres = append(baseWheres, fmt.Sprintf("run_id = ANY(%s)", runIDPlaceholder))

	whereClause := strings.Join(baseWheres, " AND ")

	var out []CancelResult

	if action == "rollback" {
		// (C6/C2) Python ops.py:1873-1879: DELETE pending runs when action=rollback.
		// Running runs still get cancel_requested_at set (worker transitions them).
		deleteQ := fmt.Sprintf(
			`DELETE FROM run WHERE %s AND status = 'pending' RETURNING run_id::text`,
			whereClause,
		)
		rows, err := s.pool.Query(ctx, deleteQ, baseArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, CancelResult{RunID: id, Deleted: true, Terminal: true})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	} else {
		// (C2) Python ops.py:1864-1870: pending runs → status='interrupted'.
		interruptQ := fmt.Sprintf(
			`UPDATE run SET status = 'interrupted', cancel_requested_at = now(), updated_at = now()
			 WHERE %s AND status = 'pending' RETURNING run_id::text`,
			whereClause,
		)
		rows, err := s.pool.Query(ctx, interruptQ, baseArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, CancelResult{RunID: id, Deleted: false, Terminal: true})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// For running runs (any action): set cancel_requested_at; worker transitions them.
	// (Python ops.py:1843-1851: running CTE only sets cancel_requested_at via pipe.set/publish)
	runningQ := fmt.Sprintf(
		`UPDATE run SET cancel_requested_at = now(), updated_at = now()
		 WHERE %s AND status = 'running' RETURNING run_id::text`,
		whereClause,
	)
	rows, err := s.pool.Query(ctx, runningQ, baseArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, CancelResult{RunID: id, Deleted: false})
	}
	rows.Close()
	return out, rows.Err()
}

// CancelByStatus marks all runs in the given statuses as cancellation-requested
// by setting cancel_requested_at = now(). Auth filters are applied to run.metadata.
// Already-cancelled runs (cancel_requested_at IS NOT NULL) are not touched.
// Returns the list of newly-cancelled run UUIDs.
func (s *Store) CancelByStatus(ctx context.Context, statuses []string, filters []*coreapi.AuthFilter) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	args := []any{statuses}
	wheres := []string{"status = ANY($1::text[])", "cancel_requested_at IS NULL"}

	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", len(args)+1)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	if authSQL != "" {
		wheres = append(wheres, authSQL)
		args = append(args, authArgs...)
	}

	q := fmt.Sprintf(
		`UPDATE run SET cancel_requested_at = now(), updated_at = now()
		 WHERE %s
		 RETURNING run_id::text`,
		strings.Join(wheres, " AND "),
	)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetStatus sets a run's status column.
func (s *Store) SetStatus(ctx context.Context, runID, statusText string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE run SET status = $2, updated_at = now() WHERE run_id = $1::uuid`,
		runID, statusText,
	)
	return err
}

// isTerminalStatus reports whether statusText is a terminal run status.
// Terminal states: success, error, interrupted, timeout (ops.py semantics).
// "rollback" is also terminal in the proto enum but Python does not send "done"
// for it — we include it here for completeness / future-proofing.
func isTerminalStatus(statusText string) bool {
	switch statusText {
	case "success", "error", "interrupted", "timeout", "rollback":
		return true
	}
	return false
}

// MarkDone releases a run's lease without touching its status (ops.py:1417-1437,
// 2a). The worker/graph execution path is the sole owner of the terminal status
// transition (via SetStatus); MarkDone must not overwrite it, whatever it is.
func (s *Store) MarkDone(ctx context.Context, runID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE run
		 SET updated_at = now(),
		     lease_holder_id = NULL, lease_expires_at = NULL
		 WHERE run_id = $1::uuid`,
		runID,
	)
	return err
}

// ClaimedRun bundles a claimed run with its lease attempt count.
type ClaimedRun struct {
	Run     *Run
	Attempt uint64
}

// Next claims up to limit pending runs using SELECT … FOR UPDATE SKIP LOCKED,
// sets their status to 'running', stamps lease_expires_at = now() + 5 minutes,
// and increments lease_generation. Returns the claimed runs with their attempt
// count (2k: attempt is the number of times Next has claimed the run — distinct
// from lease_generation, which Sweep also bumps as a zombie-fencing token).
func (s *Store) Next(ctx context.Context, limit uint64) ([]*ClaimedRun, error) {
	if limit == 0 {
		limit = 1
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// (C9) Thread-isolation guard: a pending run is not claimable while another
	// run on the same thread is 'running'. Mirrors Python ops.py:1375-1379:
	//   AND NOT EXISTS (SELECT 1 FROM run r2
	//                   WHERE r2.thread_id = run.thread_id AND r2.status = 'running')
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM run
		WHERE status = 'pending'
		  AND (run_after IS NULL OR run_after <= now())
		  AND NOT EXISTS (
		      SELECT 1 FROM run r2
		      WHERE r2.thread_id = run.thread_id
		        AND r2.status = 'running'
		  )
		ORDER BY created_at ASC, run_id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, runCols),
		limit,
	)
	if err != nil {
		return nil, err
	}
	var pending []*Run
	for rows.Next() {
		var r Run
		if err := scanRun(rows, &r); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, &r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		_ = tx.Commit(ctx)
		return nil, nil
	}

	ids := make([]string, len(pending))
	for i, r := range pending {
		ids[i] = r.RunID
	}
	// (item 6) Use s.leaseTTL (from LSD_LEASE_TTL_SECONDS) instead of hardcoded 5 minutes.
	// (2k) attempt counts claims (bumped here, on every successful Next claim);
	// lease_generation is a separate fencing token also bumped by Sweep, which
	// must NOT bump attempt (a swept-and-reclaimed run has been claimed twice,
	// not three times).
	if _, err := tx.Exec(ctx,
		`UPDATE run
		 SET status = 'running',
		     lease_expires_at  = now() + ($2::int * INTERVAL '1 second'),
		     lease_generation  = lease_generation + 1,
		     attempt           = attempt + 1,
		     updated_at        = now()
		 WHERE run_id = ANY($1::uuid[])`,
		ids, s.leaseTTL,
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// Re-fetch after update to get the final state.
	var out []*ClaimedRun
	for _, r := range pending {
		updated, err := s.Get(ctx, r.RunID, "", nil)
		if err != nil {
			return nil, err
		}
		out = append(out, &ClaimedRun{Run: updated, Attempt: uint64(updated.Attempt)})
	}
	return out, nil
}

// Sweep finds all running runs whose lease_expires_at is in the past, acquires
// pg_advisory_xact_lock(run_id hash) to prevent double-reap across replicas, and
// resets them to status='pending' so they can be retried. Returns the list of
// swept run UUIDs.
//
// (C8) Python ops.py:1467 resets to status='pending' and calls wake_up_worker().
// The lease_generation is incremented to fence zombie workers: ExtendLease
// checks status='running', so a zombie that wakes after the sweep will find its
// UPDATE matches 0 rows (the run is now 'pending') and receive ErrNotFound.
func (s *Store) Sweep(ctx context.Context) ([]string, error) {
	// Find candidates outside a transaction first (non-locking read).
	rows, err := s.pool.Query(ctx,
		`SELECT run_id::text FROM run
		 WHERE status = 'running'
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at < now()`)
	if err != nil {
		return nil, err
	}
	var candidates []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var swept []string
	for _, id := range candidates {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return swept, err
		}
		// pg_advisory_xact_lock ensures only one replica sweeps this run.
		// The lock is automatically released when the transaction ends.
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(('x' || left(md5($1), 16))::bit(64)::bigint)`, id,
		); err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			continue
		}
		// Re-check inside the lock — another replica may have already swept.
		var stillExpired bool
		if err := tx.QueryRow(ctx,
			`SELECT status = 'running' AND lease_expires_at < now()
			 FROM run WHERE run_id = $1::uuid`, id,
		).Scan(&stillExpired); err != nil || !stillExpired {
			tx.Rollback(ctx) //nolint:errcheck
			continue
		}
		// (C8) Reset to 'pending' (Python ops.py:1467), clear lease columns,
		// and increment lease_generation so a zombie worker's ExtendLease fails
		// (ExtendLease checks status='running'; run is now 'pending').
		if _, err := tx.Exec(ctx,
			`UPDATE run
			 SET status = 'pending',
			     updated_at = now(),
			     lease_holder_id = NULL,
			     lease_expires_at = NULL,
			     lease_generation = lease_generation + 1
			 WHERE run_id = $1::uuid`, id,
		); err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			continue
		}
		swept = append(swept, id)
	}
	return swept, nil
}
