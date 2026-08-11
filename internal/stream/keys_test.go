package stream_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/duongnghia222/langsmith-deployment-go/internal/stream"
)

func TestKeys(t *testing.T) {
	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	threadID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "RunStreamKey",
			got:  stream.RunStreamKey(runID),
			want: "run:11111111-1111-1111-1111-111111111111:stream",
		},
		{
			name: "ThreadStreamKey",
			got:  stream.ThreadStreamKey(threadID),
			want: "thread:22222222-2222-2222-2222-222222222222:events",
		},
		{
			name: "RunControlChannel",
			got:  stream.RunControlChannel(runID),
			want: "run:11111111-1111-1111-1111-111111111111:control",
		},
		{
			name: "RunTerminalChannel",
			got:  stream.RunTerminalChannel(runID),
			want: "run:11111111-1111-1111-1111-111111111111:terminal",
		},
		{
			name: "RunQueueKey",
			got:  stream.RunQueueKey(),
			want: "run:queue",
		},
		{
			name: "CacheKey",
			got:  stream.CacheKey("user-abc", "mykey"),
			want: "cache:user-abc:mykey",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}
