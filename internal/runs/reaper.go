package runs

import (
	"context"
	"log/slog"
	"time"

	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	goredis "github.com/redis/go-redis/v9"
)

// ReaperConfig controls the Reaper loop cadence.
type ReaperConfig struct {
	Interval time.Duration // how often to sweep; default 30s
}

// RunReaper sweeps expired run leases on a fixed interval. It runs on every
// replica; pg_advisory_xact_lock inside Sweep prevents double-reap.
// Returns when ctx is cancelled.
//
// (2m) After a sweep requeues runs, RPush their IDs onto the run queue so a
// worker blocked on BLPOP wakes immediately instead of waiting out its poll
// timeout — mirroring Python ops.py:1473 wake_up_worker() and the same RPush
// service.Sweep already does for the RPC-triggered path.
func RunReaper(ctx context.Context, store *Store, rdb *goredis.Client, log *slog.Logger, cfg ReaperConfig) {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("run reaper: stopping")
			return
		case <-ticker.C:
			reaped, err := store.Sweep(ctx)
			if err != nil {
				log.Error("run reaper: sweep", "err", err)
				continue
			}
			if len(reaped) > 0 {
				log.Info("run reaper: reaped expired leases", "count", len(reaped))
				if rdb != nil {
					for _, id := range reaped {
						_ = rdb.RPush(ctx, lsdstream.RunQueueKey(), id).Err()
					}
				}
			}
		}
	}
}
