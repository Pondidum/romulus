package storage

import (
	"context"
	"romulus/domain"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestWritingSpanContents(t *testing.T) {
	spans := createTrace()
	writer := createTestWriter(t)
	reader := createTestReader(t)

	root := spans[len(spans)-1]
	tid := root.SpanContext.TraceID()
	sid := root.SpanContext.SpanID()

	t.Log("traceid", tid)
	t.Log("spanid", sid)

	// write to storage
	err := writer.Write(t.Context(), spans)
	require.NoError(t, err)

	t.Run("readSpanContents", func(t *testing.T) {
		// read it back
		read, err := reader.readSpanContents(t.Context(), sid.String())
		require.NoError(t, err)

		require.Equal(t, "testing", read.Name)
		require.Equal(t, root.SpanContext.SpanID(), read.SpanContext.SpanID())
		require.Equal(t, root.SpanContext.TraceID(), read.SpanContext.TraceID())
		require.NotEmpty(t, root.SpanContext.SpanID())
		require.NotEmpty(t, root.SpanContext.TraceID())

		require.Len(t, read.Resource.Attributes(), len(root.Resource.Attributes()))
		require.Equal(t, attribute.Key("service.instance.id"), read.Resource.Attributes()[0].Key)
		require.Equal(t, attribute.StringValue("tests"), read.Resource.Attributes()[0].Value)

		require.Len(t, read.Attributes, len(root.Attributes))
		require.Equal(t, attribute.Key("a.bool.t"), read.Attributes[0].Key)
		require.Equal(t, attribute.BoolValue(true), read.Attributes[0].Value)
	})

	t.Run("read whole trace", func(t *testing.T) {
		read, err := reader.Trace(t.Context(), tid.String())
		require.NoError(t, err)
		require.Len(t, read, 7)
	})

	t.Run("read attributes", func(t *testing.T) {
		attr, err := reader.readAttribute(t.Context(), "a.bool.t", attribute.BOOL, sid.String())
		require.NoError(t, err)
		require.Equal(t, attribute.BoolValue(true), attr)
	})

	t.Run("find all spans by time", func(t *testing.T) {
		spans, err := reader.spanIdsForTime(t.Context(), Range{
			Start:  root.StartTime,
			Finish: root.EndTime,
		})
		require.NoError(t, err)
		require.Len(t, spans, 7)
	})

	t.Run("find spans subset by time", func(t *testing.T) {
		spans, err := reader.spanIdsForTime(t.Context(), Range{
			Start:  root.StartTime.Add(3 * time.Second),
			Finish: root.EndTime.Add(-2 * time.Second),
		})
		require.NoError(t, err)
		require.Len(t, spans, 2)
	})

	t.Run("find spans by attribute", func(t *testing.T) {
		traceIds, err := reader.Filter(t.Context(),
			Range{Start: root.StartTime, Finish: root.EndTime},
			SpanFilter{
				attribute.Bool("this.one", true),
			},
		)
		require.NoError(t, err)
		require.Len(t, traceIds, 1)
	})

	t.Run("find span by attribute, no result", func(t *testing.T) {
		traceIds, err := reader.Filter(t.Context(),
			Range{Start: root.StartTime, Finish: root.EndTime},
			SpanFilter{
				attribute.Bool("this.one", false),
			},
		)
		require.NoError(t, err)
		require.Empty(t, traceIds)
	})

	t.Run("find span by multiple attributes", func(t *testing.T) {
		traceIds, err := reader.Filter(t.Context(),
			Range{Start: root.StartTime, Finish: root.EndTime},
			SpanFilter{
				attribute.Bool("this.one", true),
				attribute.Bool("other.key", false),
			},
		)
		require.NoError(t, err)
		require.Len(t, traceIds, 1)
	})

	t.Run("find multiple spans by different attributes", func(t *testing.T) {
		traceIds, err := reader.Filter(t.Context(),
			Range{Start: root.StartTime, Finish: root.EndTime},
			SpanFilter{
				attribute.Bool("this.one", true),
			},
			SpanFilter{
				attribute.Bool("different.one", true),
			},
		)
		require.NoError(t, err)
		require.Len(t, traceIds, 1)
		require.Equal(t, tid, traceIds[0])
	})

	t.Run("find multiple spans by different attributes, no results", func(t *testing.T) {
		traceIds, err := reader.Filter(t.Context(),
			Range{Start: root.StartTime, Finish: root.EndTime},
			SpanFilter{
				attribute.Bool("this.one", true),
			},
			SpanFilter{
				attribute.Bool("different.one", false),
			},
		)
		require.NoError(t, err)
		require.Empty(t, traceIds)
	})
}

func createTrace() []domain.Span {
	start := time.Now()
	tp, exporter := createTraceProvider()
	tr := tp.Tracer("tests")

	createSpan := func(ctx context.Context, name string, attrs ...attribute.KeyValue) context.Context {
		start = start.Add(1 * time.Second)
		ctx, span := tr.Start(ctx, name, trace.WithTimestamp(start))
		span.SetAttributes(attrs...)
		span.End()
		return ctx
	}

	ctx, root := tr.Start(context.Background(), "testing", trace.WithNewRoot(), trace.WithTimestamp(start))
	root.SetAttributes(
		attribute.Bool("a.bool.t", true),
		attribute.Bool("a.bool.f", false),
		attribute.BoolSlice("a.bools", []bool{true, false, true}),
		attribute.Int("a.int", 19875),
		attribute.String("a.str", "something short"),
	)
	root.SetStatus(codes.Ok, "kaikki hyvin")

	createSpan(ctx, "child_one")

	c2 := createSpan(ctx, "child_two", attribute.Bool("this.one", true), attribute.Bool("other.key", false))
	createSpan(c2, "grand_one")
	createSpan(c2, "grand_two", attribute.Bool("different.one", true))

	c3 := createSpan(ctx, "child_three")
	createSpan(c3, "grand_three")

	root.End(trace.WithTimestamp(start))

	return exporter.GetSpans()
}
