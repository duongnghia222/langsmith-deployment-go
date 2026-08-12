package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/duongnghia222/langsmith-deployment-go/internal/admin"
	"github.com/duongnghia222/langsmith-deployment-go/internal/assistants"
	"github.com/duongnghia222/langsmith-deployment-go/internal/cache"
	"github.com/duongnghia222/langsmith-deployment-go/internal/checkpointer"
	"github.com/duongnghia222/langsmith-deployment-go/internal/config"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/db"
	"github.com/duongnghia222/langsmith-deployment-go/internal/logger"
	"github.com/duongnghia222/langsmith-deployment-go/internal/redis"
	"github.com/duongnghia222/langsmith-deployment-go/internal/runs"
	"github.com/duongnghia222/langsmith-deployment-go/internal/server"
	lsdstream "github.com/duongnghia222/langsmith-deployment-go/internal/stream"
	"github.com/duongnghia222/langsmith-deployment-go/internal/threads"
	"github.com/duongnghia222/langsmith-deployment-go/internal/tracing"
)

const version = "0.1.0"

func main() {
	cfg, err := config.Load()
	if err != nil {
		fatalf("config: %v", err)
	}
	log := logger.New(cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	shutdownTrace, err := tracing.Init(ctx, "lsd", version)
	if err != nil {
		fatalf("tracing: %v", err)
	}
	defer func() {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := shutdownTrace(shutdownCtx); err != nil {
			log.Error("tracer shutdown", "err", err)
		}
	}()

	pool, err := db.New(ctx, cfg.DatabaseURL, int32(cfg.DBPoolMaxConns))
	if err != nil {
		fatalf("db: %v", err)
	}
	defer pool.Close()

	rdb, err := redis.New(ctx, redis.Config{URL: cfg.RedisURL, PoolSize: cfg.RedisPoolSize})
	if err != nil {
		fatalf("redis: %v", err)
	}
	defer rdb.Close()
	log.Info("redis connected")
	streamer := lsdstream.NewStreamer(rdb.Client)

	if err := db.Migrate(pool, cfg.DatabaseURL); err != nil {
		fatalf("migrate: %v", err)
	}
	log.Info("migrations applied")

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		fatalf("listen %s: %v", cfg.GRPCAddr, err)
	}
	runsSvc := runs.NewServiceWithStream(pool, rdb.Client, streamer, cfg)
	cronsSvc := crons.NewService(pool)
	cpStore := checkpointer.NewStore(pool)
	cpSvc := checkpointer.NewService(cpStore)
	cacheSvc := cache.NewService(cache.NewCache(rdb.Client))
	threadsSvc := threads.NewServiceWithStream(pool, rdb.Client, streamer, cfg).WithCheckpointer(cpStore)

	srv := server.New(server.Deps{
		Admin:        admin.New(version, "1"),
		Assistants:   assistants.NewService(pool),
		Threads:      threadsSvc,
		Runs:         runsSvc,
		Crons:        cronsSvc,
		Checkpointer: cpSvc,
		Cache:        cacheSvc,
	})

	// (item 6) Wire LSD_REAPER_INTERVAL_SECONDS (cfg.ReaperInterval) into ReaperConfig,
	// and use a lease-TTL-aware store for the reaper so Sweep/Next use cfg.LeaseTTL.
	go runs.RunReaper(ctx, runs.NewStoreWithLeaseTTL(pool, int64(cfg.LeaseTTL.Seconds())), rdb.Client, log, runs.ReaperConfig{
		Interval: cfg.ReaperInterval,
	})
	go crons.CronScheduler(ctx, pool, crons.NewStore(pool), runsSvc, log, crons.SchedulerConfig{})
	go threads.TTLSweeper(ctx, pool, log, threads.TTLSweeperConfig{Interval: cfg.ThreadTTLSweepInterval})

	go func() {
		log.Info("grpc serving", "addr", cfg.GRPCAddr)
		if err := srv.Serve(lis); err != nil {
			log.Error("grpc serve", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	srv.GracefulStop()
}

func fatalf(format string, args ...any) {
	logger.New("info").Error("fatal", "msg", fmt.Sprintf(format, args...))
	os.Exit(1)
}
