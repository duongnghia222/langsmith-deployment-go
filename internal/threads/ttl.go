package threads

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TTLSweeperConfig controls the thread TTL sweeper loop cadence and leader lock key.
type TTLSweeperConfig struct {
	Interval    time.Duration // how often to sweep; default 60s
	AdvisoryKey int64         // pg_advisory_lock key; default 8472637262 (distinct from crons' 8472637261)
}

// TTLSweeper deletes threads whose expires_at has passed, on a fixed
// interval. Python's Threads.sweep_ttl is a stub (ops.py:1137-1148) — Go owns
// TTL enforcement exclusively (api/thread_ttl.py becomes a no-op under
// IS_POSTGRES_OR_GRPC_BACKEND). Leader election via pg_try_advisory_lock
// mirrors crons.CronScheduler so only one replica sweeps at a time.
// Returns when ctx is cancelled.
func TTLSweeper(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger, cfg TTLSweeperConfig) {
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.AdvisoryKey == 0 {
		cfg.AdvisoryKey = 8472637262
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("thread ttl sweeper: stopping")
			return
		case <-ticker.C:
			if err := ttlSweepTick(ctx, pool, log, cfg.AdvisoryKey); err != nil {
				log.Error("thread ttl sweeper: tick", "err", err)
			}
		}
	}
}

// ttlSweepTick acquires the advisory lock, then deletes expired threads in
// batches of 100 until none remain, releasing the lock when done.
func ttlSweepTick(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger, key int64) error {
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

	const batchSize = 100
	for {
		tag, err := conn.Exec(ctx, `
			DELETE FROM thread
			WHERE thread_id IN (
				SELECT thread_id FROM thread
				WHERE expires_at IS NOT NULL AND expires_at < now()
				LIMIT $1
			)`, batchSize)
		if err != nil {
			return err
		}
		n := tag.RowsAffected()
		if n > 0 {
			log.Info("thread ttl sweeper: deleted expired threads", "count", n)
		}
		if n < batchSize {
			return nil
		}
	}
}
