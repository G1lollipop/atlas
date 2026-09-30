package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextRestoresValidParentAndDropsUnrelatedSpanAndBaggage(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracer := provider.Tracer("tracing-test")

	rootCtx, root := tracer.Start(context.Background(), "root")
	member, err := baggage.NewMember("customer", "sensitive")
	if err != nil {
		t.Fatalf("create baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("create baggage: %v", err)
	}
	withBaggage := baggage.ContextWithBaggage(rootCtx, bag)
	traceparent, tracestate := TraceContext(withBaggage)
	if traceparent == "" {
		t.Fatal("TraceContext returned an empty traceparent for a valid span")
	}
	rootSC := root.SpanContext()

	unrelatedCtx, unrelated := tracer.Start(context.Background(), "unrelated")
	defer unrelated.End()
	ctx := ContextWithTraceContext(unrelatedCtx, traceparent, tracestate)
	if baggage.FromContext(ctx).Len() != 0 {
		t.Fatal("restored trace context unexpectedly carried baggage")
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if carrier.Get("baggage") != "" {
		t.Fatalf("TraceContext injection included baggage: %q", carrier.Get("baggage"))
	}
	childCtx, child := tracer.Start(ctx, "child")
	childSC := child.SpanContext()
	child.End()
	root.End()

	if childSC.TraceID() != rootSC.TraceID() {
		t.Fatalf("child trace ID = %s, want %s", childSC.TraceID(), rootSC.TraceID())
	}
	if trace.SpanContextFromContext(childCtx).SpanID() != childSC.SpanID() {
		t.Fatal("child context lost its active span")
	}
	var childParent trace.SpanContext
	for _, ended := range recorder.Ended() {
		stub := tracetest.SpanStubFromReadOnlySpan(ended)
		if stub.Name == "child" {
			childParent = stub.Parent
		}
	}
	if childParent.SpanID() != rootSC.SpanID() {
		t.Fatalf("child parent span ID = %s, want %s", childParent.SpanID(), rootSC.SpanID())
	}
	if childParent.SpanID() == unrelated.SpanContext().SpanID() {
		t.Fatal("restored child was linked to the unrelated active span")
	}
}

func TestTraceContextInvalidParentStartsRootAndPreservesCancellation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracer := provider.Tracer("tracing-test")

	parentCtx, unrelated := tracer.Start(context.Background(), "unrelated")
	defer unrelated.End()
	ctx, cancel := context.WithCancel(parentCtx)
	restored := ContextWithTraceContext(ctx, "00-00000000000000000000000000000000-0000000000000000-01", "")
	if trace.SpanContextFromContext(restored).IsValid() {
		t.Fatal("invalid traceparent should not produce a parent SpanContext")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("unexpected cancellation before cancel: %v", err)
	}
	cancel()
	if restored.Err() == nil {
		t.Fatal("restored context did not preserve cancellation")
	}

	rootCtx, root := tracer.Start(restored, "root")
	root.End()
	if !trace.SpanContextFromContext(rootCtx).IsValid() {
		t.Fatal("root span did not produce a valid SpanContext")
	}
	for _, ended := range recorder.Ended() {
		stub := tracetest.SpanStubFromReadOnlySpan(ended)
		if stub.Name == "root" && stub.Parent.IsValid() {
			t.Fatalf("invalid persisted context linked root to %s", stub.Parent.SpanID())
		}
	}
}

func TestTraceContextReturnsEmptyForInvalidContext(t *testing.T) {
	traceparent, tracestate := TraceContext(context.Background())
	if traceparent != "" || tracestate != "" {
		t.Fatalf("TraceContext(background) = (%q, %q), want empty values", traceparent, tracestate)
	}
}
