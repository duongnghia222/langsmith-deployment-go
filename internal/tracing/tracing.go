// Package tracing initialises the global OpenTelemetry tracer used by LSD.
// Tracing is opt-in: if OTEL_EXPORTER_OTLP_ENDPOINT is unset, Init is a no-op
// and the returned shutdown function does nothing. When the endpoint is set,
// spans are exported via OTLP/gRPC.
package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ShutdownFunc flushes pending spans and releases tracer-provider resources.
type ShutdownFunc func(context.Context) error

func noop(context.Context) error { return nil }

// Init wires the global TracerProvider when OTEL_EXPORTER_OTLP_ENDPOINT is set.
// OTEL_EXPORTER_OTLP_INSECURE=true switches the OTLP/gRPC exporter to plaintext
// (useful for local collectors). Returns a no-op shutdown when tracing is off.
func Init(ctx context.Context, serviceName, version string) (ShutdownFunc, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return noop, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	if os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true" {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
