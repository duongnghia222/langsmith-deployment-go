package crons

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	"github.com/duongnghia222/langsmith-deployment-go/internal/auth"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	robfigcron "github.com/robfig/cron/v3"
)

// newUUID generates an app-side cron_id, mirroring runs/store.go's newUUID —
// used so Create's idempotent INSERT ... ON CONFLICT (cron_id) DO NOTHING and
// its UNION ALL fallback SELECT always target the same, already-known id.
func newUUID() string { return uuid.New().String() }

// Store provides read-only access to the cron table.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store backed by the given connection pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Cron is the internal representation of a cron row.
type Cron struct {
	CronID         string
	ThreadID       string // empty string when NULL
	UserID         string // empty string when NULL
	AssistantID    string
	Schedule       string
	NextRunDate    *time.Time // nullable
	EndTime        *time.Time // nullable
	Payload        []byte
	Metadata       []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Enabled        bool
	Timezone       string
	OnRunCompleted string // empty string = NULL; otherwise an enum name
}

// cronCols is the SELECT projection for all Cron fields (unqualified, for single-table queries).
const cronCols = `cron_id::text,
	COALESCE(thread_id::text, ''),
	COALESCE(user_id, ''),
	assistant_id::text,
	schedule,
	next_run_date,
	end_time,
	COALESCE(payload, '{}'::jsonb)::text::bytea,
	COALESCE(metadata, '{}'::jsonb)::text::bytea,
	created_at,
	updated_at,
	COALESCE(enabled, TRUE),
	COALESCE(timezone, ''),
	COALESCE(on_run_completed, '')`

// cronColsAliased returns the SELECT projection for all Cron fields.
// When aliased is true (i.e. a JOIN is in play), all columns are qualified with "c."
// to avoid ambiguity with the joined thread table.
func cronColsAliased(aliased bool) string {
	if !aliased {
		return cronCols
	}
	return `c.cron_id::text,
	COALESCE(c.thread_id::text, ''),
	COALESCE(c.user_id, ''),
	c.assistant_id::text,
	c.schedule,
	c.next_run_date,
	c.end_time,
	COALESCE(c.payload, '{}'::jsonb)::text::bytea,
	COALESCE(c.metadata, '{}'::jsonb)::text::bytea,
	c.created_at,
	c.updated_at,
	COALESCE(c.enabled, TRUE),
	COALESCE(c.timezone, ''),
	COALESCE(c.on_run_completed, '')`
}

// SearchInput carries the optional filter parameters for Search and Count.
type SearchInput struct {
	AssistantID   string
	ThreadID      string
	ThreadFilters []*coreapi.AuthFilter // filters applied to t.metadata via JOIN with thread table
	Limit         uint64
	Offset        uint64
	SortBy        string // one of cronsSortColumns keys; default "created_at"
	SortOrder     string // "asc" or "desc"; default "desc"
	Enabled       *bool  // LSD-only: when non-nil, filters WHERE enabled = $n
}

// cronsSortColumns maps the proto CronsSortBy enum names to safe SQL column names.
// Default sort is created_at DESC (ops.py:2398: ORDER BY cron.created_at DESC).
var cronsSortColumns = map[string]string{
	"cron_id":       "cron_id",
	"assistant_id":  "assistant_id",
	"thread_id":     "thread_id",
	"next_run_date": "next_run_date",
	"end_time":      "end_time",
	"created_at":    "created_at",
	"updated_at":    "updated_at",
}

// cronsSortClause returns the ORDER BY fragment for the given sort_by/sort_order.
// Default: "created_at DESC" (ops.py:2398). NULLS ordering follows ops.py:2494:
// ASC gets NULLS FIRST, DESC gets NULLS LAST (nullable columns: thread_id,
// next_run_date, end_time).
func cronsSortClause(sortBy, sortOrder string) string {
	col, ok := cronsSortColumns[sortBy]
	if !ok {
		col, sortOrder = "created_at", "desc"
	}
	dir, nulls := "DESC", "NULLS LAST"
	if sortOrder == "asc" {
		dir, nulls = "ASC", "NULLS FIRST"
	}
	return col + " " + dir + " " + nulls
}

// whereArgsResult holds the output of whereArgs.
type whereArgsResult struct {
	from  string // FROM clause (e.g. "cron c" or "cron c INNER JOIN thread t ON c.thread_id = t.thread_id")
	where string // WHERE clause (always non-empty — at minimum "TRUE")
	args  []any
	idx   int // next free placeholder index
}

// whereArgs builds the FROM/WHERE fragment and argument slice shared by Search and Count.
// idx is the starting $N placeholder index (1-based).
//
// When in.ThreadFilters is non-empty a LEFT JOIN with the thread table is added
// (ops.py:2430-2441). LEFT JOIN + "(c.thread_id IS NULL OR (<thread filter>))"
// means crons with no thread (NULL thread_id) are exempt from thread_filters —
// they pass unconditionally — while crons with a thread are still constrained.
func whereArgs(in SearchInput, filters []*coreapi.AuthFilter) (whereArgsResult, error) {
	// Determine whether we need to join with the thread table.
	joinThread := len(in.ThreadFilters) > 0

	var from string
	// Column prefix for cron-table columns (needed when joining to avoid ambiguity).
	cp := "" // cron prefix: "" when no join, "c." when joining
	if joinThread {
		from = "cron c LEFT JOIN thread t ON c.thread_id = t.thread_id"
		cp = "c."
	} else {
		from = "cron c"
	}

	args := []any{}
	wheres := []string{"TRUE"}
	idx := 1

	if in.AssistantID != "" {
		wheres = append(wheres, fmt.Sprintf("%sassistant_id = $%d::uuid", cp, idx))
		args = append(args, in.AssistantID)
		idx++
	}
	if in.ThreadID != "" {
		wheres = append(wheres, fmt.Sprintf("%sthread_id = $%d::uuid", cp, idx))
		args = append(args, in.ThreadID)
		idx++
	}
	// LSD-only: enabled filter (no Python equivalent; added as an LSD extension).
	if in.Enabled != nil {
		wheres = append(wheres, fmt.Sprintf("%senabled = $%d", cp, idx))
		args = append(args, *in.Enabled)
		idx++
	}

	// Auth filters on the cron's own metadata column.
	metaCol := cp + "metadata"
	authSQL, authArgs, err := auth.ApplyToQuery(filters, metaCol, idx)
	if err != nil {
		return whereArgsResult{}, fmt.Errorf("auth: %w", err)
	}
	if authSQL != "" {
		wheres = append(wheres, authSQL)
		args = append(args, authArgs...)
		idx += len(authArgs)
	}

	// Thread auth filters applied to t.metadata via the LEFT JOIN. A cron with
	// no thread (c.thread_id IS NULL) is exempt — it passes regardless of the
	// filter (ops.py:2441).
	if joinThread {
		threadAuthSQL, threadAuthArgs, err := auth.ApplyToQuery(in.ThreadFilters, "t.metadata", idx)
		if err != nil {
			return whereArgsResult{}, fmt.Errorf("thread auth: %w", err)
		}
		if threadAuthSQL != "" {
			wheres = append(wheres, fmt.Sprintf("(c.thread_id IS NULL OR (%s))", threadAuthSQL))
			args = append(args, threadAuthArgs...)
			idx += len(threadAuthArgs)
		}
	}

	return whereArgsResult{
		from:  from,
		where: strings.Join(wheres, " AND "),
		args:  args,
		idx:   idx,
	}, nil
}

// Search returns crons matching the given criteria.
// Default sort: created_at DESC (ops.py:2398: ORDER BY cron.created_at DESC).
// SortBy/SortOrder follow a whitelist; unrecognised values fall back to created_at DESC.
func (s *Store) Search(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) ([]*Cron, error) {
	wa, err := whereArgs(in, filters)
	if err != nil {
		return nil, err
	}

	limit := in.Limit
	if limit == 0 {
		limit = 100
	}
	q := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d`,
		cronColsAliased(len(in.ThreadFilters) > 0),
		wa.from,
		wa.where,
		cronsSortClause(in.SortBy, in.SortOrder),
		wa.idx, wa.idx+1,
	)
	wa.args = append(wa.args, limit, in.Offset)

	rows, err := s.pool.Query(ctx, q, wa.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Cron
	for rows.Next() {
		var c Cron
		if err := scanCron(rows, &c); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

// Count returns the number of crons matching the given criteria.
func (s *Store) Count(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) (uint64, error) {
	wa, err := whereArgs(in, filters)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, wa.from, wa.where)
	var n uint64
	if err := s.pool.QueryRow(ctx, q, wa.args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// scanCron populates a Cron from a pgx row/rows.
func scanCron(row pgx.Row, c *Cron) error {
	return row.Scan(
		&c.CronID,
		&c.ThreadID,
		&c.UserID,
		&c.AssistantID,
		&c.Schedule,
		&c.NextRunDate,
		&c.EndTime,
		&c.Payload,
		&c.Metadata,
		&c.CreatedAt,
		&c.UpdatedAt,
		&c.Enabled,
		&c.Timezone,
		&c.OnRunCompleted,
	)
}

// prefixWithAnd prepends " AND " to a non-empty SQL fragment.
func prefixWithAnd(frag string) string {
	if frag == "" {
		return ""
	}
	return " AND " + frag
}

// CreateCronInput carries the fields for a new cron row.
type CreateCronInput struct {
	CronID           string // optional; empty → app-generated newUUID() (4e: resolved before INSERT so ON CONFLICT/UNION ALL fallback target the same row)
	AssistantID      string
	ThreadID         string // optional
	UserID           string // optional
	Schedule         string
	Timezone         string
	Enabled          bool
	EndTime          *time.Time
	Payload          []byte
	Metadata         []byte
	OnRunCompleted   string                // optional; empty → NULL
	Filters          []*coreapi.AuthFilter // cron-scoped auth filters (applied to cron.metadata in conflict SELECT)
	AssistantFilters []*coreapi.AuthFilter // auth filters applied to assistant.metadata (ops.py:2185)
	ThreadFilters    []*coreapi.AuthFilter // auth filters applied to thread.metadata   (ops.py:2210)
}

// Create inserts a new cron row, or returns the pre-existing row when
// cron_id already exists — an idempotent create (4e) mirroring ops.py:2237-
// 2260's "cron_id = cron_id or uuid6()" + "ON CONFLICT (cron_id) DO NOTHING"
// + UNION ALL fallback SELECT. cron_id is always resolved app-side (newUUID()
// when in.CronID is empty) so both the INSERT and the fallback SELECT target
// the same row. next_run_date is computed from the schedule using
// robfig/cron/v3 with the row's timezone (or UTC if empty).
//
// When AssistantFilters or ThreadFilters are provided the INSERT leg is
// wrapped in CTEs that gate it on auth visibility of the assistant and/or
// thread — mirroring Python ops.py:2182-2260. in.Filters (cron-scoped) gates
// the fallback SELECT's pre-existing-row leg the same way. A zero-row result
// (neither leg matches) means not found or not authorized on both legs; the
// caller should map this to codes.NotFound.
func (s *Store) Create(ctx context.Context, in CreateCronInput) (*Cron, error) {
	if in.Payload == nil {
		in.Payload = []byte(`{}`)
	}
	if in.Metadata == nil {
		in.Metadata = []byte(`{}`)
	}

	cronID := in.CronID
	if cronID == "" {
		cronID = newUUID()
	}

	// Compute next_run_date. computeNextRun prefixes parse errors with "schedule parse:".
	nextRun, err := computeNextRun(in.Schedule, in.Timezone)
	if err != nil {
		return nil, err // already prefixed by computeNextRunFrom
	}

	// ── Build args and SQL fragments ───────────────────────────────────────
	args := []any{cronID}
	idSQL := "$1::uuid"

	// base is the $N index for assistant_id (first fixed positional column).
	// Compute BEFORE the append so it reflects the next available slot.
	base := len(args) + 1
	// Fixed positional columns: assistant_id, schedule, timezone, enabled, next_run_date, payload, metadata
	args = append(args, in.AssistantID, in.Schedule, in.Timezone, in.Enabled, nextRun, in.Payload, in.Metadata)

	// Optional nullable columns.
	var threadSQL, userSQL, endSQL, orcSQL string
	if in.ThreadID != "" {
		args = append(args, in.ThreadID)
		threadSQL = fmt.Sprintf("$%d::uuid", len(args))
	} else {
		threadSQL = "NULL"
	}
	if in.UserID != "" {
		args = append(args, in.UserID)
		userSQL = fmt.Sprintf("$%d", len(args))
	} else {
		userSQL = "NULL"
	}
	if in.EndTime != nil {
		args = append(args, *in.EndTime)
		endSQL = fmt.Sprintf("$%d", len(args))
	} else {
		endSQL = "NULL"
	}
	if in.OnRunCompleted != "" {
		args = append(args, in.OnRunCompleted)
		orcSQL = fmt.Sprintf("$%d", len(args))
	} else {
		orcSQL = "NULL"
	}

	// ── Auth CTE construction (ops.py:2182-2260) ──────────────────────────
	//
	// Always wrap the INSERT in an "inserted_cron" CTE (needed for the
	// ON CONFLICT DO NOTHING + UNION ALL idempotent-create shape below);
	// additional authorized_assistant/authorized_thread CTEs are prepended
	// only when AssistantFilters/ThreadFilters are present.
	idx := len(args) + 1

	var (
		ctePrefix             string
		insertAssistantSelect string
		insertAssistantFrom   string
		insertThreadSelect    string
		insertThreadJoin      string
	)

	if len(in.AssistantFilters) > 0 {
		// authorized_assistant CTE (ops.py:2191-2199).
		// Reuse $base (already in args) as the assistant_id parameter.
		// This avoids leaving an unreferenced $N which PG can't type-infer.
		aSQL, aArgs, err := auth.ApplyToQuery(in.AssistantFilters, "assistant.metadata", idx)
		if err != nil {
			return nil, fmt.Errorf("assistant auth: %w", err)
		}
		args = append(args, aArgs...)
		idx += len(aArgs)

		var andFilter string
		if aSQL != "" {
			andFilter = " AND " + aSQL
		}
		// $base is already the assistant_id in args (reuse it in the CTE WHERE).
		ctePrefix = fmt.Sprintf(
			"WITH authorized_assistant AS (\n"+
				"    SELECT assistant.assistant_id FROM assistant\n"+
				"    WHERE assistant.assistant_id = $%d::uuid%s\n"+
				"),\n",
			base, andFilter,
		)
		insertAssistantSelect = "authorized_assistant.assistant_id"
		insertAssistantFrom = "FROM authorized_assistant"
	} else {
		// No assistant filter: $base is already the assistant_id literal.
		insertAssistantSelect = fmt.Sprintf("$%d::uuid", base)
		insertAssistantFrom = ""
	}

	if in.ThreadID != "" && len(in.ThreadFilters) > 0 {
		// authorized_thread CTE (ops.py:2217-2230).
		// Reuse threadSQL (already in args as $N::uuid) in the CTE WHERE.
		tSQL, tArgs, err := auth.ApplyToQuery(in.ThreadFilters, "thread.metadata", idx)
		if err != nil {
			return nil, fmt.Errorf("thread auth: %w", err)
		}
		args = append(args, tArgs...)
		idx += len(tArgs)

		var andFilter string
		if tSQL != "" {
			andFilter = " AND " + tSQL
		}

		if ctePrefix == "" {
			ctePrefix = "WITH "
		}
		// threadSQL is already "$N::uuid" where N references the thread_id in args.
		ctePrefix += fmt.Sprintf(
			"authorized_thread AS (\n"+
				"    SELECT thread.thread_id FROM thread\n"+
				"    WHERE thread.thread_id = %s%s\n"+
				"),\n",
			threadSQL, andFilter,
		)

		insertThreadSelect = "authorized_thread.thread_id"
		if insertAssistantFrom != "" {
			insertThreadJoin = "CROSS JOIN authorized_thread"
		} else {
			insertThreadJoin = "FROM authorized_thread"
		}
	} else {
		insertThreadSelect = threadSQL // already "NULL" or "$N::uuid"
		insertThreadJoin = ""
	}

	insertFrom := insertAssistantFrom
	if insertThreadJoin != "" {
		if insertFrom != "" {
			insertFrom += "\n        " + insertThreadJoin
		} else {
			insertFrom = insertThreadJoin
		}
	}
	if ctePrefix == "" {
		// No auth CTEs needed — still need the wrapping WITH for inserted_cron.
		ctePrefix = "WITH "
	}

	// ── Fallback SELECT auth filter (ops.py:2249's UNION ALL leg) ─────────
	// Gates visibility of a pre-existing row: a caller without in.Filters
	// visibility into that row gets ErrNotFound rather than someone else's cron.
	fallbackAuthSQL, fallbackAuthArgs, err := auth.ApplyToQuery(in.Filters, "c.metadata", idx)
	if err != nil {
		return nil, fmt.Errorf("fallback auth: %w", err)
	}
	args = append(args, fallbackAuthArgs...)
	var fallbackAndFilter string
	if fallbackAuthSQL != "" {
		fallbackAndFilter = " AND " + fallbackAuthSQL
	}

	// base points to assistant_id ($base), so schedule is $base+1, etc.
	// The inserted_cron leg selects * to avoid re-evaluating the RETURNING
	// expressions (which would fail to resolve unqualified column names
	// against the CTE, since e.g. COALESCE(payload,...) is auto-named
	// "coalesce" by Postgres in inserted_cron's output, not "payload");
	// the fallback leg re-applies cronColsAliased itself since it selects
	// straight from the cron table.
	q := fmt.Sprintf(`%sinserted_cron AS (
    INSERT INTO cron
        (cron_id, assistant_id, thread_id, user_id, schedule, timezone, enabled,
         next_run_date, end_time, payload, metadata, on_run_completed, created_at, updated_at)
    SELECT
        %s, %s, %s, %s, $%d, $%d, $%d, $%d, %s, $%d::jsonb, $%d::jsonb, %s, now(), now()
    %s
    ON CONFLICT (cron_id) DO NOTHING
    RETURNING %s
)
SELECT * FROM inserted_cron
UNION ALL
SELECT %s FROM cron c WHERE c.cron_id = %s%s
LIMIT 1`,
		ctePrefix,
		idSQL,
		insertAssistantSelect, // CTE col or literal $N::uuid
		insertThreadSelect,    // CTE col or "NULL" or "$N::uuid"
		userSQL,
		base+1, // schedule
		base+2, // timezone
		base+3, // enabled
		base+4, // next_run_date
		endSQL, // end_time
		base+5, // payload
		base+6, // metadata
		orcSQL, // on_run_completed
		insertFrom,
		cronCols,
		cronColsAliased(true),
		idSQL,
		fallbackAndFilter,
	)

	var c Cron
	if err := scanCron(s.pool.QueryRow(ctx, q, args...), &c); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Neither leg matched: assistant/thread not found/not authorized
			// on the insert leg, or the pre-existing row is hidden by
			// in.Filters on the fallback leg. Caller maps this to
			// codes.NotFound (ops.py:2278-2281).
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &c, nil
}

// PatchCronInput carries optional mutable fields for a cron update.
type PatchCronInput struct {
	Schedule       string
	Timezone       string
	Enabled        *bool
	EndTime        *time.Time // nil = no change; non-nil = set (matches Python ops.py PatchCronRequest.end_time)
	Payload        []byte
	Metadata       []byte
	OnRunCompleted *string // nil = no change; non-nil = write (empty string clears)
}

// Patch updates mutable cron fields. If Schedule or Timezone changes,
// next_run_date is recomputed using the stored value of whichever of the two
// is not itself being patched (4d-iii). Payload and Metadata are merged
// (JSONB ||), not replaced (4d-ii) — matching ops.py's PATCH semantics of
// sending only the changed keys. Returns the updated Cron or ErrNotFound.
func (s *Store) Patch(ctx context.Context, cronID string, in PatchCronInput, filters []*coreapi.AuthFilter) (*Cron, error) {
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	sets := []string{"updated_at = now()"}
	args := []any{cronID}
	idx := 2 + len(authArgs)

	// Recompute next_run_date when either schedule OR timezone is patched
	// (4d-iii): load the stored value for whichever field is absent so e.g.
	// patching only timezone doesn't silently recompute against schedule=""
	// or patching only schedule doesn't drop the row's existing timezone.
	if in.Schedule != "" || in.Timezone != "" {
		schedule, timezone := in.Schedule, in.Timezone
		if schedule == "" || timezone == "" {
			var storedSchedule, storedTimezone string
			row := s.pool.QueryRow(ctx, `SELECT schedule, COALESCE(timezone, '') FROM cron WHERE cron_id = $1::uuid`, cronID)
			if err := row.Scan(&storedSchedule, &storedTimezone); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, ErrNotFound
				}
				return nil, err
			}
			if schedule == "" {
				schedule = storedSchedule
			}
			if timezone == "" {
				timezone = storedTimezone
			}
		}
		// computeNextRun already prefixes parse errors with "schedule parse:".
		nextRun, err := computeNextRun(schedule, timezone)
		if err != nil {
			return nil, err
		}
		sets = append(sets, fmt.Sprintf("next_run_date = $%d", idx))
		args = append(args, nextRun)
		idx++
	}
	if in.Schedule != "" {
		sets = append(sets, fmt.Sprintf("schedule = $%d", idx))
		args = append(args, in.Schedule)
		idx++
	}
	if in.Timezone != "" {
		sets = append(sets, fmt.Sprintf("timezone = $%d", idx))
		args = append(args, in.Timezone)
		idx++
	}
	if in.Enabled != nil {
		sets = append(sets, fmt.Sprintf("enabled = $%d", idx))
		args = append(args, *in.Enabled)
		idx++
	}
	if in.EndTime != nil {
		// PatchCronRequest.end_time — matches the bridge (crons.py) sending end_time.
		sets = append(sets, fmt.Sprintf("end_time = $%d", idx))
		args = append(args, *in.EndTime)
		idx++
	}
	if len(in.Payload) > 0 {
		// Merge, not replace (4d-ii): payload = payload || $n::jsonb —
		// ops.py's PATCH sends only the changed run-payload keys.
		sets = append(sets, fmt.Sprintf("payload = payload || $%d::jsonb", idx))
		args = append(args, in.Payload)
		idx++
	}
	if len(in.Metadata) > 0 {
		// Merge, not replace (4d-ii): metadata = metadata || $n::jsonb.
		sets = append(sets, fmt.Sprintf("metadata = metadata || $%d::jsonb", idx))
		args = append(args, in.Metadata)
		idx++
	}
	if in.OnRunCompleted != nil {
		sets = append(sets, fmt.Sprintf("on_run_completed = $%d", idx))
		args = append(args, *in.OnRunCompleted)
		idx++
	}
	_ = idx

	finalArgs := append([]any{cronID}, authArgs...)
	finalArgs = append(finalArgs, args[1:]...)
	q := fmt.Sprintf(
		`UPDATE cron SET %s WHERE cron_id = $1::uuid%s RETURNING %s`,
		strings.Join(sets, ", "),
		prefixWithAnd(authSQL),
		cronCols,
	)
	var c Cron
	if err := scanCron(s.pool.QueryRow(ctx, q, finalArgs...), &c); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &c, nil
}

// ErrNotFound is returned when a cron is not found or hidden by auth filters.
var ErrNotFound = errors.New("cron not found")

// Delete removes the cron matching cronID (with optional auth filters).
// Returns ErrNotFound when no row matched (not found, or hidden by auth
// filters) — mirroring ops.py:2290-2318 + the route's fetchone 404 (4f).
func (s *Store) Delete(ctx context.Context, cronID string, filters []*coreapi.AuthFilter) error {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(
		`DELETE FROM cron WHERE cron_id = $1::uuid%s`,
		prefixWithAnd(authSQL),
	)
	tag, err := s.pool.Exec(ctx, q, append([]any{cronID}, args...)...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// CronWithNow pairs a cron row with the DB-clock snapshot (now()) captured
// at query time.  Using the DB clock as the "now" base for next-run computation
// avoids clock-skew between scheduler replicas (ops.py:2325 "select *, now() as now").
type CronWithNow struct {
	Cron *Cron
	Now  time.Time // DB-side now() snapshot
}

// Next atomically claims and advances all enabled, due crons in one
// transaction: SELECT ... FOR NO KEY UPDATE SKIP LOCKED (ops.py:2320-2338
// shape) claims the rows so a concurrent Next() on another replica can't
// double-claim them, then next_run_date is recomputed and persisted for each
// claimed row before COMMIT — replacing the old design where the scheduler
// advanced next_run_date itself via a separate SetNextRunDate call after the
// lock had already been released (a race window across scheduler replicas).
// Used by the CronScheduler goroutine.
//
// Predicates follow ops.py:2325-2328:
//   - (end_time IS NULL OR end_time >= now())  -- keep-verbatim from Python
//   - next_run_date <= now()
//
// enabled = TRUE is an LSD-only extension (not in Python's internal API which
// has no concept of per-cron disabling); kept because it is an LSD-exposed feature.
func (s *Store) Next(ctx context.Context) ([]*CronWithNow, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit succeeds

	q := fmt.Sprintf(
		`SELECT %s, now() FROM cron
		 WHERE enabled = TRUE
		   AND next_run_date IS NOT NULL
		   AND next_run_date <= now()
		   AND (end_time IS NULL OR end_time >= now())
		 ORDER BY next_run_date ASC, cron_id ASC
		 FOR NO KEY UPDATE SKIP LOCKED`,
		cronCols,
	)
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	var out []*CronWithNow
	for rows.Next() {
		var c Cron
		var dbNow time.Time
		if err := scanCronWithNow(rows, &c, &dbNow); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, &CronWithNow{Cron: &c, Now: dbNow})
	}
	closeErr := rows.Err()
	rows.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	for _, cw := range out {
		// Base the recompute on the DB-side now() snapshot, matching Python
		// cron_scheduler.py:131 (cron["now"]).
		next, err := computeNextRunFrom(cw.Cron.Schedule, cw.Cron.Timezone, cw.Now)
		if err != nil {
			// ponytail: schedule is already validated at Create/Patch time, so
			// this should be unreachable in practice. Leave next_run_date
			// unadvanced rather than failing the whole claim; the row stays
			// due and the caller still gets it (and can log the anomaly).
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE cron SET next_run_date = $2 WHERE cron_id = $1::uuid`, cw.Cron.CronID, next); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// scanCronWithNow populates a Cron and the trailing now() column from a pgx row.
func scanCronWithNow(row pgx.Row, c *Cron, dbNow *time.Time) error {
	return row.Scan(
		&c.CronID,
		&c.ThreadID,
		&c.UserID,
		&c.AssistantID,
		&c.Schedule,
		&c.NextRunDate,
		&c.EndTime,
		&c.Payload,
		&c.Metadata,
		&c.CreatedAt,
		&c.UpdatedAt,
		&c.Enabled,
		&c.Timezone,
		&c.OnRunCompleted,
		dbNow,
	)
}

// SetNextRunDate updates a cron's next_run_date to the given time.
// Does not bump updated_at (ops.py:2341-2351 — 4h: this is an internal
// scheduling field, not a user-visible mutation).
func (s *Store) SetNextRunDate(ctx context.Context, cronID string, next time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE cron SET next_run_date = $2 WHERE cron_id = $1::uuid`,
		cronID, next,
	)
	return err
}

// computeNextRun parses a cron schedule and returns the next time after now().
// timezone is an IANA location name (e.g., "America/New_York"); empty uses UTC.
// Returns errors prefixed "schedule parse:" so callers can detect parse failures.
func computeNextRun(schedule, timezone string) (time.Time, error) {
	return computeNextRunFrom(schedule, timezone, time.Now())
}

// computeNextRunFrom parses a cron schedule and returns the next time after base.
// Used by the scheduler to compute next_run_date from the DB-side now() snapshot
// (cron_scheduler.py:131: next_cron_date(cron["schedule"], cron["now"])).
// timezone is an IANA location name (e.g., "America/New_York"); empty uses UTC.
// Returns errors prefixed "schedule parse:" so callers can detect parse failures.
func computeNextRunFrom(schedule, timezone string, base time.Time) (time.Time, error) {
	loc := time.UTC
	if timezone != "" {
		var err error
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("schedule parse: load timezone %q: %w", timezone, err)
		}
	}
	// Descriptor enables @daily/@hourly/etc; SecondOptional accepts 6-field
	// schedules — both of which croniter (the Python reference parser) accepts
	// (ops.py:2174 validates via croniter). robfig still lacks croniter's L/#/
	// "7=Sunday" forms.
	// ponytail: robfig lacks L/#; swap cron lib if users hit it.
	p := robfigcron.NewParser(robfigcron.Minute | robfigcron.Hour | robfigcron.Dom | robfigcron.Month | robfigcron.Dow | robfigcron.Descriptor | robfigcron.SecondOptional)
	sched, err := p.Parse(schedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("schedule parse: %w", err)
	}
	return sched.Next(base.In(loc)), nil
}
