package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger configured to the given level
// ("debug" | "info" | "warn" | "error"). Unknown levels default to info.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("svc", "lsd")
}
