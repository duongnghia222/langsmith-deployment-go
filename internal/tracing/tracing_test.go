package tracing_test

import (
	"context"
	"testing"

	"github.com/duongnghia222/langsmith-deployment-go/internal/tracing"
)

func TestInit_NoEndpoint_ReturnsNoopShutdown(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := tracing.Init(context.Background(), "lsd-test", "0.0.0")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown is nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("noop shutdown returned err: %v", err)
	}
}

func TestInit_WithEndpoint_BuildsTracer(t *testing.T) {
	// Use an unreachable endpoint; the OTLP/gRPC exporter is lazy and only
	// connects on first export, so Init should succeed even when nothing
	// is listening.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:14317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")

	shutdown, err := tracing.Init(context.Background(), "lsd-test", "0.0.0")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown is nil")
	}
	// Best-effort shutdown — if no spans were produced, this is fast.
	_ = shutdown(context.Background())
}
