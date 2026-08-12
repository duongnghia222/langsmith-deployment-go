package assistants

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

// ErrNotFound is returned when an assistant is not found or is hidden by auth filters.
var ErrNotFound = errors.New("assistant not found")

// ErrAlreadyExists is returned by Create when if_exists=="raise" and the
// assistant_id already exists (atomic detection, no TOCTOU).
var ErrAlreadyExists = errors.New("assistant already exists")

// validSortColumns maps Python's whitelist (ops.py:187-193) to safe column names.
// Keep-verbatim from Python: valid_sort_fields = ["assistant_id","graph_id","name","created_at","updated_at"]
var validSortColumns = map[string]string{
	"assistant_id": "assistant_id",
	"graph_id":     "graph_id",
	"name":         `"name"`,
	"created_at":   "created_at",
	"updated_at":   "updated_at",
}

// sortClause returns the ORDER BY fragment for the given sort_by/sort_order,
// matching Python's logic exactly (ops.py:184-200).
// Default: "created_at DESC" (no secondary sort — Python has none).
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

// Store provides read-only access to the assistant and assistant_versions tables.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store backed by the given connection pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Assistant is the internal representation of an assistant row.
type Assistant struct {
	AssistantID string
	GraphID     string
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Config      []byte
	ContextJSON []byte
	Metadata    []byte
	Name        string
	Description *string
}

// AssistantVersion is the internal representation of an assistant_versions row.
type AssistantVersion struct {
	AssistantID string
	GraphID     string
	Name        string // mirrors assistant.name at the time of versioning
	Version     int64
	CreatedAt   time.Time
	Config      []byte
	ContextJSON []byte
	Metadata    []byte
	Description *string
}

// assistantCols is the SELECT projection for all Assistant fields.
const assistantCols = `assistant_id::text, COALESCE(graph_id, ''),
	COALESCE("version", 1),
	created_at, COALESCE(updated_at, created_at),
	COALESCE(config, '{}'::jsonb)::text::bytea,
	COALESCE(context, '{}'::jsonb)::text::bytea,
	COALESCE(metadata, '{}'::jsonb)::text::bytea,
	COALESCE("name", ''),
	description`

// assistantColsAliased is the same projection as assistantCols but qualified with
// table alias "a". Used in UPDATE...FROM queries where the target table is aliased
// to avoid ambiguous column references in the RETURNING clause.
const assistantColsAliased = `a.assistant_id::text, COALESCE(a.graph_id, ''),
	COALESCE(a."version", 1),
	a.created_at, COALESCE(a.updated_at, a.created_at),
	COALESCE(a.config, '{}'::jsonb)::text::bytea,
	COALESCE(a.context, '{}'::jsonb)::text::bytea,
	COALESCE(a.metadata, '{}'::jsonb)::text::bytea,
	COALESCE(a."name", ''),
	a.description`

// assistantVersionCols is the SELECT projection for all AssistantVersion fields,
// qualified with the assistant_versions. prefix: GetVersions optionally JOINs
// assistant, which has same-named columns (version, graph_id, config, metadata,
// created_at, context, description, name) that would otherwise be ambiguous.
const assistantVersionCols = `assistant_versions.assistant_id::text, COALESCE(assistant_versions.graph_id, ''),
	COALESCE(assistant_versions."name", ''),
	COALESCE(assistant_versions."version", 1),
	assistant_versions.created_at,
	COALESCE(assistant_versions.config, '{}'::jsonb)::text::bytea,
	COALESCE(assistant_versions.context, '{}'::jsonb)::text::bytea,
	COALESCE(assistant_versions.metadata, '{}'::jsonb)::text::bytea,
	assistant_versions.description`

func scanAssistant(row pgx.Row) (*Assistant, error) {
	var a Assistant
	if err := row.Scan(
		&a.AssistantID,
		&a.GraphID,
		&a.Version,
		&a.CreatedAt,
		&a.UpdatedAt,
		&a.Config,
		&a.ContextJSON,
		&a.Metadata,
		&a.Name,
		&a.Description,
	); err != nil {
		return nil, err
	}
	return &a, nil
}

// Get returns the assistant with the given UUID string, applying auth filters to
// the metadata column. Returns ErrNotFound if there is no matching row.
func (s *Store) Get(ctx context.Context, assistantID string, filters []*coreapi.AuthFilter) (*Assistant, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	q := fmt.Sprintf(
		`SELECT %s FROM assistant WHERE assistant_id = $1::uuid%s`,
		assistantCols,
		prefixWithAnd(authSQL),
	)
	a, err := scanAssistant(s.pool.QueryRow(ctx, q, append([]any{assistantID}, args...)...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return a, nil
}

// SearchInput carries the optional filter parameters for Search and Count.
type SearchInput struct {
	GraphID        string
	Name           string
	MetadataFilter []byte // optional JSONB containment filter (@>)
	Limit          uint64
	Offset         uint64
	SortBy         string // one of "assistant_id","graph_id","name","created_at","updated_at"; default "created_at"
	SortOrder      string // "asc" or "desc"; default "desc"
}

// whereArgs builds the WHERE fragment and argument slice shared by Search and Count.
// idx is the starting $N placeholder index (1-based). It returns the complete WHERE
// clause string (always non-empty — at minimum "TRUE"), the bound args, the next
// free placeholder index, and any error from auth filter expansion.
func whereArgs(in SearchInput, filters []*coreapi.AuthFilter) (string, []any, int, error) {
	args := []any{}
	wheres := []string{"TRUE"}
	idx := 1

	if in.GraphID != "" {
		wheres = append(wheres, fmt.Sprintf("graph_id = $%d", idx))
		args = append(args, in.GraphID)
		idx++
	}
	if in.Name != "" {
		wheres = append(wheres, fmt.Sprintf(`"name" = $%d`, idx))
		args = append(args, in.Name)
		idx++
	}
	if len(in.MetadataFilter) > 0 {
		wheres = append(wheres, fmt.Sprintf("metadata @> $%d::jsonb", idx))
		args = append(args, in.MetadataFilter)
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

// Search returns assistants matching the given criteria.
// Sort column and direction follow Python ops.py:184-200 (whitelist validated, no secondary sort).
func (s *Store) Search(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) ([]*Assistant, error) {
	where, args, idx, err := whereArgs(in, filters)
	if err != nil {
		return nil, err
	}

	limit := in.Limit
	if limit == 0 {
		limit = 100
	}
	q := fmt.Sprintf(
		`SELECT %s FROM assistant WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d`,
		assistantCols,
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

	var out []*Assistant
	for rows.Next() {
		a, err := scanAssistant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Count returns the number of assistants matching the given criteria.
func (s *Store) Count(ctx context.Context, in SearchInput, filters []*coreapi.AuthFilter) (uint64, error) {
	where, args, _, err := whereArgs(in, filters)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM assistant WHERE %s`, where)
	var n uint64
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// GetVersions returns the version history for the given assistant UUID, ordered
// by version DESC. Auth filters are applied to the parent assistant's metadata
// (ops.py:584-599: table_alias="assistant", JOIN assistant USING (assistant_id) —
// only joined when filters are present), not each version row's own metadata.
// An empty result means either no versions exist or the assistant is excluded
// by the auth filter.
//
// metadataFilter applies assistant_versions.metadata @> $n::jsonb (ops.py:599) —
// a separate, non-auth containment filter on the version row itself.
func (s *Store) GetVersions(ctx context.Context, assistantID string, limit, offset uint64, metadataFilter []byte, filters []*coreapi.AuthFilter) ([]*AssistantVersion, error) {
	if limit == 0 {
		limit = 100
	}

	// Build WHERE clauses: start after $1 (assistantID).
	wheres := []string{}
	args := []any{assistantID}
	idx := 2

	// (ops.py:599) metadata containment filter on assistant_versions.
	if len(metadataFilter) > 0 {
		wheres = append(wheres, fmt.Sprintf("assistant_versions.metadata @> $%d::jsonb", idx))
		args = append(args, metadataFilter)
		idx++
	}

	authSQL, authArgs, err := auth.ApplyToQuery(filters, "assistant.metadata", idx)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	join := ""
	if authSQL != "" {
		join = "JOIN assistant USING (assistant_id)"
		wheres = append(wheres, authSQL)
		args = append(args, authArgs...)
		idx += len(authArgs)
	}

	whereClause := "assistant_versions.assistant_id = $1::uuid"
	if len(wheres) > 0 {
		whereClause += " AND " + strings.Join(wheres, " AND ")
	}

	q := fmt.Sprintf(
		`SELECT %s FROM assistant_versions %s WHERE %s ORDER BY assistant_versions."version" DESC LIMIT $%d OFFSET $%d`,
		assistantVersionCols,
		join,
		whereClause,
		idx,
		idx+1,
	)
	args = append(args, limit, offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*AssistantVersion
	for rows.Next() {
		var v AssistantVersion
		if err := rows.Scan(
			&v.AssistantID,
			&v.GraphID,
			&v.Name,
			&v.Version,
			&v.CreatedAt,
			&v.Config,
			&v.ContextJSON,
			&v.Metadata,
			&v.Description,
		); err != nil {
			return nil, err
		}
		out = append(out, &v)
	}
	return out, rows.Err()
}

// prefixWithAnd prepends " AND " to a non-empty SQL fragment.
func prefixWithAnd(frag string) string {
	if frag == "" {
		return ""
	}
	return " AND " + frag
}

// CreateInput carries the fields for a new assistant row.
type CreateInput struct {
	AssistantID string  // optional; if empty, DB generates via gen_random_uuid()
	GraphID     string
	Name        string
	Description *string
	Config      []byte // raw JSONB; nil → '{}'
	ContextJSON []byte // raw JSONB; nil → '{}'
	Metadata    []byte // raw JSONB; nil → '{}'
	IfExists    string // "do_nothing" | "raise" (default) — ops.py:356-374

	// Filters are auth filters applied only to the do_nothing "return
	// pre-existing row" leg (ops.py:356-371). Not used for raise mode: an
	// INSERT conflict there already returns ErrAlreadyExists.
	Filters []*coreapi.AuthFilter
}

// Create inserts a new assistant and version=1 row atomically using a single CTE,
// matching Python ops.py:332-381 (INSERT ... ON CONFLICT DO NOTHING + UNION ALL for
// do_nothing; raise mode returns ErrAlreadyExists on conflict).
//
// (C3) atomic CTE: no TOCTOU race. Python: ops.py:332-344.
func (s *Store) Create(ctx context.Context, in CreateInput) (*Assistant, error) {
	if in.Config == nil {
		in.Config = []byte(`{}`)
	}
	if in.ContextJSON == nil {
		in.ContextJSON = []byte(`{}`)
	}
	if in.Metadata == nil {
		in.Metadata = []byte(`{}`)
	}

	// Build args: assistant_id (optional $1), then named fields.
	var idSQL string
	var args []any
	if in.AssistantID != "" {
		idSQL = "$1::uuid"
		// $1=assistantID, $2=graphID, $3=name, $4=description, $5=config, $6=context, $7=metadata
		args = []any{in.AssistantID, in.GraphID, in.Name, in.Description, in.Config, in.ContextJSON, in.Metadata}
	} else {
		idSQL = "gen_random_uuid()"
		// $1=graphID, $2=name, $3=description, $4=config, $5=context, $6=metadata
		args = []any{in.GraphID, in.Name, in.Description, in.Config, in.ContextJSON, in.Metadata}
	}

	// Compute positional indices based on whether assistant_id occupies $1.
	base := 1
	if in.AssistantID != "" {
		base = 2 // $1 is already assistantID
	}
	graphIdx := base
	nameIdx := base + 1
	descIdx := base + 2
	cfgIdx := base + 3
	ctxIdx := base + 4
	metaIdx := base + 5

	// (ops.py:332-344) CTE: inserted_assistant (RETURNING *) + inserted_version.
	//
	// KEY: Use RETURNING * in inserted_assistant so the derived CTE table has the
	// same raw column names as the assistant table. Then the final SELECT applies
	// assistantCols expressions against those raw column names — this works because
	// RETURNING * preserves original column names (unlike RETURNING <expressions>
	// which can rename them, e.g., COALESCE(graph_id,'') → "coalesce").
	//
	// Also KEY: the final SELECT reads from inserted_assistant (the CTE RETURNING data),
	// NOT from the assistant table, because within a single statement PostgreSQL's
	// snapshot isolation means the newly inserted row is NOT visible in the underlying
	// table to other parts of the same statement. The CTE RETURNING data IS visible.
	//
	// Version row insert reads from inserted_assistant (same visibility rule).
	insertSQL := fmt.Sprintf(`
WITH inserted_assistant AS (
    INSERT INTO assistant
        (assistant_id, graph_id, "name", description, config, context, metadata, "version", created_at, updated_at)
    VALUES (%s, $%d, $%d, $%d, $%d::jsonb, $%d::jsonb, $%d::jsonb, 1, now(), now())
    ON CONFLICT (assistant_id) DO NOTHING
    RETURNING *
),
inserted_version AS (
    INSERT INTO assistant_versions
        (assistant_id, "version", graph_id, "name", config, context, metadata, description, created_at)
    SELECT assistant_id, 1, graph_id, "name", config, context, metadata, description, now()
    FROM inserted_assistant
    ON CONFLICT (assistant_id, "version") DO NOTHING
)
SELECT %s FROM inserted_assistant`,
		idSQL, graphIdx, nameIdx, descIdx, cfgIdx, ctxIdx, metaIdx,
		assistantCols,
	)

	// (ops.py:356-371) do_nothing: UNION ALL selects existing row when conflict.
	// raise (default): empty result set on conflict → ErrAlreadyExists.
	var q string
	if in.IfExists == "do_nothing" && in.AssistantID != "" {
		// Auth filters apply only to this "return pre-existing row" leg.
		authSQL, authArgs, err := auth.ApplyToQuery(in.Filters, "metadata", len(args)+1)
		if err != nil {
			return nil, fmt.Errorf("auth: %w", err)
		}
		q = insertSQL + fmt.Sprintf(`
UNION ALL
SELECT %s FROM assistant
WHERE assistant_id = $1::uuid AND NOT EXISTS (SELECT 1 FROM inserted_assistant)%s
LIMIT 1`, assistantCols, prefixWithAnd(authSQL))
		args = append(args, authArgs...)
	} else {
		q = insertSQL
	}

	row := s.pool.QueryRow(ctx, q, args...)
	a, err := scanAssistant(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// raise mode: CTE returned no rows → conflict.
			return nil, ErrAlreadyExists
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrAlreadyExists
		}
		return nil, err
	}
	return a, nil
}

// PatchInput carries optional fields to update on an assistant.
// Zero values are ignored (not written).
type PatchInput struct {
	Name        *string // (5e) presence-tracked: nil = unset, ""=explicit clear (ops.py:457-458 uses "is not None")
	Description *string
	GraphID     string
	Config      []byte
	ContextJSON []byte
	Metadata    []byte
}

// Patch updates mutable fields on an assistant using a single CTE that:
//   1. Selects current_assistant (auth-gated).
//   2. Inserts inserted_version with the merged post-patch state and bumped version.
//   3. UPDATEs the assistant row, setting updated_at = inserted_version.created_at.
//
// Metadata is MERGED (assistant.metadata || $n::jsonb), not replaced — ops.py:455.
// updated_at is set to inserted_version.created_at, not a separate now() — ops.py:488.
// Version row metadata = current.metadata merged with patch (ops.py:477-479).
// Returns ErrNotFound if the assistant doesn't exist or the auth filter excludes it.
func (s *Store) Patch(ctx context.Context, assistantID string, in PatchInput, filters []*coreapi.AuthFilter) (*Assistant, error) {
	// Args layout: $1 = assistantID, $2..$M = auth args (if any), $M+1.. = patch fields.
	args := []any{assistantID}
	idx := 2

	authSQL, authArgs, err := auth.ApplyToQuery(filters, "metadata", idx)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	args = append(args, authArgs...)
	idx += len(authArgs)

	// --- version row columns (ops.py:471-484) ---
	// Each COALESCE picks the incoming param when non-NULL, otherwise falls back to
	// the existing column value from current_assistant.

	// graph_id for version row: COALESCE($graph_id, current_assistant.graph_id)
	var graphIDExpr string
	if in.GraphID != "" {
		graphIDExpr = fmt.Sprintf("COALESCE($%d, current_assistant.graph_id)", idx)
		args = append(args, in.GraphID)
		idx++
	} else {
		graphIDExpr = "current_assistant.graph_id"
	}

	// config for version row: COALESCE($config, current_assistant.config)
	var configExpr string
	if len(in.Config) > 0 {
		configExpr = fmt.Sprintf("COALESCE($%d::jsonb, current_assistant.config)", idx)
		args = append(args, in.Config)
		idx++
	} else {
		configExpr = "current_assistant.config"
	}

	// context for version row: COALESCE($context, current_assistant.context)
	var contextExpr string
	if len(in.ContextJSON) > 0 {
		contextExpr = fmt.Sprintf("COALESCE($%d::jsonb, current_assistant.context)", idx)
		args = append(args, in.ContextJSON)
		idx++
	} else {
		contextExpr = "current_assistant.context"
	}

	// metadata for version row (ops.py:477-479):
	//   CASE WHEN $metadata IS NULL THEN current.metadata ELSE current.metadata || $metadata::jsonb END
	// For the assistant row update (ops.py:455): metadata = assistant.metadata || $metadata::jsonb
	var metaVersionExpr string
	var metaUpdateExpr string
	if len(in.Metadata) > 0 {
		metaVersionExpr = fmt.Sprintf(
			"current_assistant.metadata || $%d::jsonb", idx)
		metaUpdateExpr = fmt.Sprintf(
			"a.metadata || $%d::jsonb", idx) // alias "a" on the UPDATE target
		args = append(args, in.Metadata)
		idx++
	} else {
		metaVersionExpr = "current_assistant.metadata"
		metaUpdateExpr = "" // no-op; handled by conditional set below
	}

	// description for version row: COALESCE($desc, current_assistant.description)
	var descExpr string
	if in.Description != nil {
		descExpr = fmt.Sprintf("COALESCE($%d, current_assistant.description)", idx)
		args = append(args, *in.Description)
		idx++
	} else {
		descExpr = "current_assistant.description"
	}

	// name for version row (LSD-only column, not in Python): use patched name or current.
	// (5e) nil = unset (fall back to current); non-nil (incl. "") = explicit value.
	var nameExpr string
	if in.Name != nil {
		nameExpr = fmt.Sprintf("COALESCE($%d, current_assistant.\"name\")", idx)
		args = append(args, *in.Name)
		idx++
	} else {
		nameExpr = `current_assistant."name"`
	}

	// --- assistant row SET fields (ops.py:445-465) ---
	// version and updated_at are always updated.
	sets := []string{
		`"version" = inserted_version."version"`,
		`updated_at = inserted_version.created_at`, // ops.py:488: updated_at = inserted_version.created_at
	}
	if in.GraphID != "" {
		sets = append(sets, fmt.Sprintf(`graph_id = $%d`, findArgIdx(args, in.GraphID, graphIDExpr)))
	}
	if len(in.Config) > 0 {
		sets = append(sets, fmt.Sprintf(`config = $%d::jsonb`, findArgIdx(args, in.Config, configExpr)))
	}
	if len(in.ContextJSON) > 0 {
		sets = append(sets, fmt.Sprintf(`context = $%d::jsonb`, findArgIdx(args, in.ContextJSON, contextExpr)))
	}
	if len(in.Metadata) > 0 {
		// (ops.py:455) metadata = assistant.metadata || $n::jsonb  (MERGE, not replace)
		sets = append(sets, fmt.Sprintf(`metadata = %s`, metaUpdateExpr))
	}
	if in.Name != nil {
		sets = append(sets, fmt.Sprintf(`"name" = $%d`, findArgIdx(args, *in.Name, nameExpr)))
	}
	if in.Description != nil {
		sets = append(sets, fmt.Sprintf(`description = $%d`, findArgIdx(args, *in.Description, descExpr)))
	}

	whereClause := "assistant_id = $1::uuid"
	if authSQL != "" {
		whereClause += " AND " + authSQL
	}

	// Use alias "a" on the UPDATE target to disambiguate RETURNING columns
	// (assistant_versions also has graph_id, config, etc.).
	q := fmt.Sprintf(`
WITH current_assistant AS (
    SELECT * FROM assistant WHERE %s
),
inserted_version AS (
    INSERT INTO assistant_versions
        (assistant_id, "version", graph_id, "name", config, context, metadata, description, created_at)
    SELECT
        current_assistant.assistant_id,
        COALESCE((SELECT MAX("version") FROM assistant_versions WHERE assistant_id = $1::uuid) + 1, 1),
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        now()
    FROM current_assistant
    RETURNING *
)
UPDATE assistant a
SET %s
FROM inserted_version
WHERE a.assistant_id = $1::uuid
RETURNING %s`,
		whereClause,
		graphIDExpr,
		nameExpr,
		configExpr,
		contextExpr,
		metaVersionExpr,
		descExpr,
		strings.Join(sets, ", "),
		assistantColsAliased,
	)

	row := s.pool.QueryRow(ctx, q, args...)
	a, err := scanAssistant(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return a, nil
}

// findArgIdx is a helper used by Patch to locate the positional $N for a value
// that was already appended to args earlier during expression building.
// It scans args in reverse for the last occurrence of val, returning its 1-based index.
// This avoids appending the same value twice for the assistant SET clause.
func findArgIdx(args []any, val any, _ string) int {
	for i := len(args) - 1; i >= 0; i-- {
		switch v := val.(type) {
		case string:
			if s, ok := args[i].(string); ok && s == v {
				return i + 1
			}
		case []byte:
			if b, ok := args[i].([]byte); ok && string(b) == string(v) {
				return i + 1
			}
		}
	}
	// Fallback: should not happen if caller always appended before calling.
	return len(args)
}

// Delete removes the assistant(s) matching assistantID and optional auth filters.
// Returns the list of deleted assistant UUIDs as strings.
//
// (5b) deleteThreads: when set, threads tagged with this assistant_id are also
// removed in the same transaction (thread FK cascades handle runs/checkpoints).
// No Python parity for this flag — it's an LSD-only convenience addition;
// ops.py:498-524 has no delete_threads parameter at all.
func (s *Store) Delete(ctx context.Context, assistantID string, deleteThreads bool, filters []*coreapi.AuthFilter) ([]string, error) {
	authSQL, args, err := auth.ApplyToQuery(filters, "metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit succeeds

	q := fmt.Sprintf(
		`DELETE FROM assistant WHERE assistant_id = $1::uuid%s RETURNING assistant_id::text`,
		prefixWithAnd(authSQL),
	)
	rows, err := tx.Query(ctx, q, append([]any{assistantID}, args...)...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	closeErr := rows.Err()
	rows.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	if deleteThreads && len(ids) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM thread WHERE metadata->>'assistant_id' = $1::text`, assistantID); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

// SetLatest rolls the assistant pointer to the given version by copying only
// config, metadata, and version from the version row back into the assistant row.
//
// (ops.py:556-560) Python restores ONLY config, metadata, version — not graph_id,
// context, or description. Those fields remain as they are on the assistant row.
// (5d) updated_at is deliberately NOT bumped here — ops.py's SET list has no
// updated_at either, so rolling back a version must not look like a fresh edit.
func (s *Store) SetLatest(ctx context.Context, assistantID string, version int64, filters []*coreapi.AuthFilter) (*Assistant, error) {
	authSQL, authArgs, err := auth.ApplyToQuery(filters, "a.metadata", 2)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	// (ops.py:556-560): SET config, metadata, version only.
	q := fmt.Sprintf(`
		WITH versioned_assistant AS (
		    SELECT assistant_versions.*
		    FROM assistant_versions
		    WHERE assistant_versions.assistant_id = $1::uuid
		      AND assistant_versions."version" = $2
		)
		UPDATE assistant a
		SET config    = versioned_assistant.config,
		    metadata  = versioned_assistant.metadata,
		    "version" = versioned_assistant."version"
		FROM versioned_assistant
		WHERE a.assistant_id = $1::uuid%s
		RETURNING %s`,
		prefixWithAnd(authSQL),
		assistantColsAliased,
	)
	finalArgs := append([]any{assistantID, version}, authArgs...)
	row := s.pool.QueryRow(ctx, q, finalArgs...)
	a, err := scanAssistant(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return a, nil
}
