package config

import (
	"testing"
	"time"
)

func TestLoad_DefaultsAndEnv(t *testing.T) {
	t.Setenv("LSD_DATABASE_URL", "postgres://x/y")
	t.Setenv("LSD_REDIS_URL", "redis://localhost:6379/0")
	// leave LSD_GRPC_ADDR unset -> default
	t.Setenv("LSD_LEASE_TTL_SECONDS", "45")
	t.Setenv("LSD_HEARTBEAT_INTERVAL_SECONDS", "10")
	t.Setenv("LSD_REAPER_INTERVAL_SECONDS", "5")
	t.Setenv("LSD_LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DatabaseURL != "postgres://x/y" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.GRPCAddr != ":50051" {
		t.Errorf("GRPCAddr default = %q, want :50051", cfg.GRPCAddr)
	}
	if cfg.LeaseTTL != 45*time.Second {
		t.Errorf("LeaseTTL = %v", cfg.LeaseTTL)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Errorf("HeartbeatInterval = %v", cfg.HeartbeatInterval)
	}
	if cfg.ReaperInterval != 5*time.Second {
		t.Errorf("ReaperInterval = %v", cfg.ReaperInterval)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
}

func TestLoad_MissingDatabaseURL(t *testing.T) {
	t.Setenv("LSD_DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when LSD_DATABASE_URL unset")
	}
}

func TestLoad_RequiresRedisURL(t *testing.T) {
	t.Setenv("LSD_DATABASE_URL", "postgres://localhost/x")
	t.Setenv("LSD_REDIS_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when LSD_REDIS_URL is unset")
	}
}

func TestLoad_DefaultsRedisPoolSize(t *testing.T) {
	t.Setenv("LSD_DATABASE_URL", "postgres://localhost/x")
	t.Setenv("LSD_REDIS_URL", "redis://localhost:6379/0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RedisPoolSize != 100 {
		t.Errorf("RedisPoolSize = %d, want 100", cfg.RedisPoolSize)
	}
	if cfg.DBPoolMaxConns != 50 {
		t.Errorf("DBPoolMaxConns = %d, want 50", cfg.DBPoolMaxConns)
	}
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Errorf("RedisURL = %q, want redis://localhost:6379/0", cfg.RedisURL)
	}
}

func TestConfig_StreamDefaults(t *testing.T) {
	// Ensure required vars are set; stream vars are absent → defaults apply.
	t.Setenv("LSD_DATABASE_URL", "postgres://test:test@localhost/test")
	t.Setenv("LSD_REDIS_URL", "redis://localhost:6379")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.StreamMaxLen != 1000 {
		t.Errorf("StreamMaxLen = %d, want 1000", cfg.StreamMaxLen)
	}
	if cfg.StreamReadBlockMs != 5000 {
		t.Errorf("StreamReadBlockMs = %d, want 5000", cfg.StreamReadBlockMs)
	}
	if cfg.StreamReplayBatch != 100 {
		t.Errorf("StreamReplayBatch = %d, want 100", cfg.StreamReplayBatch)
	}
	// HeartbeatInterval already exists; verify it still defaults correctly.
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 5s", cfg.HeartbeatInterval)
	}
}

func TestConfig_StreamOverrides(t *testing.T) {
	t.Setenv("LSD_DATABASE_URL", "postgres://test:test@localhost/test")
	t.Setenv("LSD_REDIS_URL", "redis://localhost:6379")
	t.Setenv("LSD_STREAM_MAX_LEN", "2000")
	t.Setenv("LSD_STREAM_READ_BLOCK_MS", "3000")
	t.Setenv("LSD_STREAM_REPLAY_BATCH", "50")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.StreamMaxLen != 2000 {
		t.Errorf("StreamMaxLen = %d, want 2000", cfg.StreamMaxLen)
	}
	if cfg.StreamReadBlockMs != 3000 {
		t.Errorf("StreamReadBlockMs = %d, want 3000", cfg.StreamReadBlockMs)
	}
	if cfg.StreamReplayBatch != 50 {
		t.Errorf("StreamReplayBatch = %d, want 50", cfg.StreamReplayBatch)
	}
}
