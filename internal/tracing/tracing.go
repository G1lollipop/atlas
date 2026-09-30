// Package tracing wires OpenTelemetry and carries W3C trace context across durable
// job boundaries. Job and run trace state is server-managed and contains only the
// traceparent and tracestate headers; baggage is intentionally never persisted.
package tracing

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Init configures a global OpenTelemetry TracerProvider. Without an OTLP endpoint,
// it still installs a non-exporting SDK provider so transitions can create distinct
// child SpanContexts and carry them through the database. Export remains opt-in.
func Init(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", serviceName))),
	}
	if strings.TrimSpace(otlpEndpoint) == "" {
		options = append(options, sdktrace.WithSampler(sdktrace.NeverSample()))
	} else {
		exporter, exportErr := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(otlpEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if exportErr != nil {
			return nil, fmt.Errorf("create otlp exporter: %w", exportErr)
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

// TraceContext serializes only the active, valid W3C SpanContext. It deliberately
// uses TraceContext directly rather than the global propagator, which may include
// baggage or other application-specific fields.
func TraceContext(ctx context.Context) (traceparent, tracestate string) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return "", ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}

// ContextWithTraceContext restores a persisted W3C parent while preserving the
// caller's values, deadline, and cancellation. An empty or invalid traceparent
// clears any active span so an unrelated polling span cannot become the parent.
// Extraction is explicitly TraceContext-only; baggage is not restored.
func ContextWithTraceContext(ctx context.Context, traceparent, tracestate string) context.Context {
	root := trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	if strings.TrimSpace(traceparent) == "" {
		return root
	}

	carrier := propagation.MapCarrier{"traceparent": traceparent}
	if tracestate != "" {
		carrier.Set("tracestate", tracestate)
	}
	extracted := propagation.TraceContext{}.Extract(root, carrier)
	if !trace.SpanContextFromContext(extracted).IsValid() {
		return root
	}
	return extracted
}
