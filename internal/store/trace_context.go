package store

import (
	"context"
	"fmt"

	"github.com/G1lollipop/atlas/internal/tracing"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startStoredSpan(ctx context.Context, traceparent, tracestate, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	parentCtx := tracing.ContextWithTraceContext(ctx, traceparent, tracestate)
	return otel.Tracer("atlas/store").Start(parentCtx, name, trace.WithAttributes(attrs...))
}

func persistedTraceContext(ctx context.Context) (traceparent, tracestate string) {
	return tracing.TraceContext(ctx)
}

func finishTransitionSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

type storedRunContext struct {
	id          string
	traceparent string
	tracestate  string
}

// transitionStoredRuns applies one lifecycle transition to every row returned by
// selectionSQL. It creates each span from that row's persisted parent and commits
// the new parent alongside the status change in one transaction.
func transitionStoredRuns(ctx context.Context, pool *pgxpool.Pool, selectionSQL, updateSQL, spanName string) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, selectionSQL)
	if err != nil {
		return 0, err
	}
	selected := make([]storedRunContext, 0)
	for rows.Next() {
		var run storedRunContext
		if err := rows.Scan(&run.id, &run.traceparent, &run.tracestate); err != nil {
			rows.Close()
			return 0, err
		}
		selected = append(selected, run)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	spans := make([]trace.Span, 0, len(selected))
	ids := make([]string, 0, len(selected))
	traceparents := make([]string, 0, len(selected))
	tracestates := make([]string, 0, len(selected))
	for _, run := range selected {
		opCtx, span := startStoredSpan(ctx, run.traceparent, run.tracestate, spanName,
			attribute.String("run.id", run.id))
		spans = append(spans, span)
		traceparent, tracestate := persistedTraceContext(opCtx)
		ids = append(ids, run.id)
		traceparents = append(traceparents, traceparent)
		tracestates = append(tracestates, tracestate)
	}

	if len(selected) > 0 {
		tag, updateErr := tx.Exec(ctx, updateSQL, ids, traceparents, tracestates)
		if updateErr != nil {
			for _, span := range spans {
				finishTransitionSpan(span, updateErr)
			}
			return 0, updateErr
		}
		if tag.RowsAffected() != int64(len(selected)) {
			updateErr = fmt.Errorf("trace transition %q selected %d runs but updated %d", spanName, len(selected), tag.RowsAffected())
			for _, span := range spans {
				finishTransitionSpan(span, updateErr)
			}
			return 0, updateErr
		}
	}

	if err := tx.Commit(ctx); err != nil {
		for _, span := range spans {
			finishTransitionSpan(span, err)
		}
		return 0, err
	}
	for _, span := range spans {
		finishTransitionSpan(span, nil)
	}
	return len(selected), nil
}
