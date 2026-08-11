package runs

import (
	"context"
	"log/slog"
	"time"
)

// ReaperConfig controls the Reaper loop cadence.
type ReaperConfig struct {
	Interval time.Duration // how often to sweep; default 30s
}

// RunReaper sweeps expired run leases on a fixed interval. It runs on every
// replica; pg_advisory_xact_lock inside Sweep prevents double-reap.
// Returns when ctx is cancelled.
func RunReaper(ctx context.Context, store *Store, log *slog.Logger, cfg ReaperConfig) {
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
			}
		}
	}
}
