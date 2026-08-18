package crons

import (
	"testing"
	"time"
)

// TestComputeNextRunFrom_WidenedDialect verifies 4i's parser flags:
// Descriptor (@daily/@hourly/etc, croniter parity) and SecondOptional
// (6-field schedules, croniter parity) are both accepted, while the plain
// 5-field form used everywhere else in this package keeps working unchanged.
func TestComputeNextRunFrom_WidenedDialect(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		schedule string
		want     time.Time
	}{
		{"5-field still works", "0 12 * * *", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"@daily descriptor", "@daily", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"@hourly descriptor", "@hourly", time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)},
		{"6-field with seconds", "30 0 12 * * *", time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := computeNextRunFrom(tc.schedule, "", base)
			if err != nil {
				t.Fatalf("computeNextRunFrom(%q): %v", tc.schedule, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("computeNextRunFrom(%q) = %v, want %v", tc.schedule, got, tc.want)
			}
		})
	}
}

// TestComputeNextRunFrom_UnsupportedDialect documents the out-of-scope gap
// (4i): croniter's "L"/"#"/"7=Sunday" forms are not supported by robfig.
// ponytail: robfig lacks L/#; swap cron lib if users hit it.
func TestComputeNextRunFrom_UnsupportedDialect(t *testing.T) {
	if _, err := computeNextRunFrom("0 0 * * 7", "", time.Now()); err == nil {
		t.Skip("robfig accepted 7=Sunday; dialect gap may have narrowed, nothing to fix")
	}
}
