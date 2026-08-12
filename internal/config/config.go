package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL            string
	DBPoolMaxConns         int
	RedisURL               string
	RedisPoolSize          int
	GRPCAddr               string
	MetricsAddr            string
	LeaseTTL               time.Duration
	HeartbeatInterval      time.Duration
	ReaperInterval         time.Duration
	NextPollInterval       time.Duration
	ThreadTTLSweepInterval time.Duration
	// CronInterval is the scheduler tick cadence. Python reference:
	// CRON_SCHEDULER_SLEEP_TIME=5 (api/config/__init__.py:427).
	CronInterval time.Duration
	LogLevel     string
	Env                    string

	// StreamCfg controls Redis Streams behaviour (R4).
	StreamMaxLen      int64
	StreamReadBlockMs int
	StreamReplayBatch int64
}

func Load() (*Config, error) {
	dburl := os.Getenv("LSD_DATABASE_URL")
	if dburl == "" {
		return nil, errors.New("LSD_DATABASE_URL is required")
	}
	redisURL := os.Getenv("LSD_REDIS_URL")
	if redisURL == "" {
		return nil, errors.New("LSD_REDIS_URL is required")
	}
	cfg := &Config{
		DatabaseURL: dburl,
		// Python reference pool: max_size=150 (storage/database.py). 50 is a
		// safer default against Postgres' default max_connections=100; raise
		// via env for production parity.
		DBPoolMaxConns: getEnvInt("LSD_DB_POOL_MAX_CONNS", 50),
		RedisURL:       redisURL,
		// Python reference: REDIS_MAX_CONNECTIONS=2000. Blocking stream reads
		// each hold a connection, so 10 starves under concurrent streaming.
		RedisPoolSize:          getEnvInt("LSD_REDIS_POOL_SIZE", 100),
		GRPCAddr:               getEnv("LSD_GRPC_ADDR", ":50051"),
		MetricsAddr:            getEnv("LSD_METRICS_ADDR", ":9090"),
		LeaseTTL:               getEnvSeconds("LSD_LEASE_TTL_SECONDS", 30*time.Second),
		HeartbeatInterval:      getEnvSeconds("LSD_HEARTBEAT_INTERVAL_SECONDS", 5*time.Second),
		ReaperInterval:         getEnvSeconds("LSD_REAPER_INTERVAL_SECONDS", 2*time.Second),
		NextPollInterval:       getEnvSeconds("LSD_NEXT_POLL_INTERVAL_SECONDS", 1*time.Second),
		ThreadTTLSweepInterval: getEnvSeconds("LSD_THREAD_TTL_SWEEP_INTERVAL_SECONDS", 60*time.Second),
		CronInterval:           getEnvSeconds("LSD_CRON_INTERVAL_SECONDS", 5*time.Second),
		LogLevel:               getEnv("LSD_LOG_LEVEL", "info"),
		Env:                    getEnv("LSD_ENV", "prod"),
		StreamMaxLen:           getEnvInt64("LSD_STREAM_MAX_LEN", 1000),
		StreamReadBlockMs:      getEnvInt("LSD_STREAM_READ_BLOCK_MS", 5000),
		StreamReplayBatch:      getEnvInt64("LSD_STREAM_REPLAY_BATCH", 100),
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func getEnvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func getEnvSeconds(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}
