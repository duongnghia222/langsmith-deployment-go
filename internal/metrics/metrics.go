package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Metrics struct {
	GRPCRequests  *prometheus.CounterVec
	GRPCDurations *prometheus.HistogramVec
	RunsClaimed   prometheus.Counter
	RunsReclaimed prometheus.Counter
	Reg           *prometheus.Registry
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		Reg: r,
		GRPCRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "lsd_grpc_requests_total"},
			[]string{"method", "code"}),
		GRPCDurations: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lsd_grpc_request_duration_seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method"}),
		RunsClaimed:   prometheus.NewCounter(prometheus.CounterOpts{Name: "lsd_runs_claimed_total"}),
		RunsReclaimed: prometheus.NewCounter(prometheus.CounterOpts{Name: "lsd_runs_reclaimed_total"}),
	}
	r.MustRegister(m.GRPCRequests, m.GRPCDurations, m.RunsClaimed, m.RunsReclaimed)
	// Pre-initialize label sets so the metric families appear in Gather() immediately.
	m.GRPCRequests.WithLabelValues("", "").Add(0)
	m.GRPCDurations.WithLabelValues("").Observe(0)
	return m
}

// UnaryInterceptor returns a gRPC interceptor that records request count and latency.
func (m *Metrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		m.GRPCRequests.WithLabelValues(info.FullMethod, code).Inc()
		m.GRPCDurations.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}
