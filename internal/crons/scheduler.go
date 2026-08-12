package crons

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	enumcronorc "github.com/duongnghia222/langsmith-deployment-go/gen/enum_cron_on_run_completed"
	enummultitaskstrategy "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RunsCreator is the minimal interface needed by the scheduler to create runs.
// Implemented by *runs.Service — passed in to avoid an import cycle.
type RunsCreator interface {
	Create(ctx context.Context, req *coreapi.CreateRunRequest) (*coreapi.CreateRunResponse, error)
}

// SchedulerConfig controls the Scheduler loop cadence and leader lock key.
type SchedulerConfig struct {
	Interval    time.Duration // how often to check due crons; default 60s
	AdvisoryKey int64         // pg_advisory_lock key; default 8472637261
}

// CronScheduler attempts leader election via pg_try_advisory_lock, then fires
// synthetic Runs.Create for each due cron. Only one replica at a time holds the
// advisory lock; all replicas run the goroutine, but only the leader fires.
// Returns when ctx is cancelled.
func CronScheduler(ctx context.Context, pool *pgxpool.Pool, store *Store, runsCreator RunsCreator, log *slog.Logger, cfg SchedulerConfig) {
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.AdvisoryKey == 0 {
		cfg.AdvisoryKey = 8472637261
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	// Fire the first tick immediately instead of waiting a full interval
	// (time.NewTicker's first tick lands after cfg.Interval elapses).
	if err := schedulerTick(ctx, pool, store, runsCreator, log, cfg.AdvisoryKey); err != nil {
		log.Error("cron scheduler: tick", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			log.Info("cron scheduler: stopping")
			return
		case <-ticker.C:
			if err := schedulerTick(ctx, pool, store, runsCreator, log, cfg.AdvisoryKey); err != nil {
				log.Error("cron scheduler: tick", "err", err)
			}
		}
	}
}

// schedulerTick attempts to acquire the advisory lock, fires due crons if leader,
// then releases the lock. Non-blocking: if lock is held by another replica, skip.
func schedulerTick(ctx context.Context, pool *pgxpool.Pool, store *Store, rc RunsCreator, log *slog.Logger, key int64) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil // another replica is leader
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key) //nolint:errcheck

	due, err := store.Next(ctx)
	if err != nil {
		return err
	}
	for _, cw := range due {
		c := cw.Cron
		// next_run_date has already been atomically claimed-and-advanced by
		// store.Next() (see store.go), so the scheduler no longer computes or
		// persists it itself — that used to happen after the row's lock was
		// already released, leaving a race window across scheduler replicas.

		// Inject on_completion: "keep" when the cron's on_run_completed is "keep"
		// (cron_scheduler.py:89-90: run_payload.setdefault("on_completion", "keep")).
		// Setdefault semantics: caller's existing key wins.
		payload := injectOnCompletion(c.Payload, c.OnRunCompleted)

		ifNotExists := coreapi.CreateRunBehavior_CREATE_THREAD_IF_THREAD_NOT_EXISTS
		multitaskStrategy := multitaskStrategyFromPayload(payload)
		req := &coreapi.CreateRunRequest{
			AssistantId: &coreapi.UUID{Value: c.AssistantID},
			KwargsJson:  payload,
			// cron_scheduler.py:87-92: {**cron_metadata, **existing_metadata},
			// payload's own "metadata" key wins over the cron row's metadata.
			MetadataJson: mergeCronMetadata(c.Metadata, payload),
			// cron_scheduler.py:96-98: set_auth_ctx_for_run(..., user_id=cron["user_id"]).
			IfNotExists:       &ifNotExists,
			MultitaskStrategy: &multitaskStrategy,
		}
		if c.ThreadID != "" {
			req.ThreadId = &coreapi.UUID{Value: c.ThreadID}
		}
		if c.UserID != "" {
			req.UserId = &c.UserID
		}
		if _, err := rc.Create(ctx, req); err != nil {
			log.Error("cron scheduler: create run", "cron_id", c.CronID, "err", err)
		}
	}
	return nil
}

// injectOnCompletion applies the on_run_completed logic from cron_scheduler.py:89-90:
//
//	if on_run_completed == "keep":
//	    run_payload.setdefault("on_completion", "keep")
//
// It deserialises the JSON payload, injects the key only when absent (setdefault
// semantics: caller's existing value wins), and re-serialises.
// onRunCompleted is the raw string stored in the DB column (e.g. "keep", "delete", "").
// Returns the original payload unchanged on any JSON error.
func injectOnCompletion(payload []byte, onRunCompleted string) []byte {
	// Check both the stored string and the proto enum mapping.
	// DB column stores the enum name directly (store.go cronCols: COALESCE(on_run_completed, '')).
	isKeep := onRunCompleted == enumcronorc.CronOnRunCompleted_keep.String() // "keep"
	if !isKeep {
		return payload
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return payload // leave as-is on parse error
	}
	if m == nil {
		m = make(map[string]json.RawMessage)
	}
	// setdefault: only inject when key is absent (cron_scheduler.py:90).
	if _, exists := m["on_completion"]; !exists {
		m["on_completion"] = json.RawMessage(`"keep"`)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return payload
	}
	return out
}

// multitaskStrategyFromPayload reads the payload dict's "multitask_strategy"
// key (crons.py:_payload_dict_to_proto routes it through extra_json verbatim,
// so it round-trips as a plain string), defaulting to "enqueue" when absent
// or unrecognised (api/models/run.py:185: payload.get("multitask_strategy")
// or "enqueue").
func multitaskStrategyFromPayload(payload []byte) enummultitaskstrategy.MultitaskStrategy {
	var obj struct {
		MultitaskStrategy string `json:"multitask_strategy"`
	}
	_ = json.Unmarshal(payload, &obj) // leave obj zero-valued on parse error
	if v, ok := enummultitaskstrategy.MultitaskStrategy_value[obj.MultitaskStrategy]; ok {
		return enummultitaskstrategy.MultitaskStrategy(v)
	}
	return enummultitaskstrategy.MultitaskStrategy_enqueue
}

// mergeCronMetadata merges the cron row's own metadata with the run payload's
// "metadata" key, with the payload's keys taking precedence on conflict —
// cron_scheduler.py:32-49 get_metadata_from_payload: {**cron_metadata, **existing_metadata}.
func mergeCronMetadata(cronMetadata, payload []byte) []byte {
	merged := map[string]json.RawMessage{}
	if len(cronMetadata) > 0 {
		_ = json.Unmarshal(cronMetadata, &merged)
	}
	if merged == nil {
		merged = map[string]json.RawMessage{}
	}

	var payloadObj map[string]json.RawMessage
	_ = json.Unmarshal(payload, &payloadObj)
	if raw, ok := payloadObj["metadata"]; ok {
		var payloadMeta map[string]json.RawMessage
		if json.Unmarshal(raw, &payloadMeta) == nil {
			for k, v := range payloadMeta {
				merged[k] = v
			}
		}
	}

	out, err := json.Marshal(merged)
	if err != nil {
		return cronMetadata
	}
	return out
}
