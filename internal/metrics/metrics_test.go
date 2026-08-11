package metrics_test

import (
	"strings"
	"testing"

	"github.com/duongnghia222/langsmith-deployment-go/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_RegistryExposesAll(t *testing.T) {
	m := metrics.New()
	m.RunsClaimed.Inc()
	m.RunsReclaimed.Add(3)

	count := testutil.ToFloat64(m.RunsClaimed)
	if count != 1 {
		t.Errorf("RunsClaimed=%v", count)
	}
	count = testutil.ToFloat64(m.RunsReclaimed)
	if count != 3 {
		t.Errorf("RunsReclaimed=%v", count)
	}
	mfs, err := m.Reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make([]string, 0, len(mfs))
	for _, f := range mfs {
		names = append(names, f.GetName())
	}
	for _, want := range []string{"lsd_grpc_requests_total", "lsd_runs_claimed_total"} {
		if !contains(names, want) {
			t.Errorf("missing metric %q in %s", want, strings.Join(names, ","))
		}
	}
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
