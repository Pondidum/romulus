package storage

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestStorageWriting(t *testing.T) {

	cfg, err := config.LoadDefaultConfig(t.Context())
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	require.NotNil(t, client)

	storage := &Storage{
		s3:      client,
		dataset: "testing",
	}

	trace := createTrace()

	err = storage.Write(t.Context(), trace)
	require.NoError(t, err)

	// query it!
	traceId := trace[0].SpanContext().TraceID().String()

	t.Run("read by traceid", func(t *testing.T) {
		read, err := storage.Trace(t.Context(), traceId)
		require.NoError(t, err)
		require.Len(t, read, 7)
	})

	t.Run("find spans by time range", func(t *testing.T) {
		spans, err := storage.spanIdsForTime(t.Context(), Range{
			trace[0].StartTime().Add(-1 * time.Second),
			trace[0].EndTime().Add(1 * time.Second),
		})
		require.NoError(t, err)
		require.Len(t, spans, 7)
	})
}

func TestSpansIdsForTime(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(t.Context())
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	require.NotNil(t, client)

	storage := &Storage{
		s3:      client,
		dataset: "testing",
	}

	start := time.Now()
	for i := range uint64(10) {
		sid := make([]byte, 8)
		binary.LittleEndian.PutUint64(sid, i)
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			SpanID: trace.SpanID(sid),
		})

		storage.writeTimes(t.Context(), &fakeSpan{
			start.Add(time.Duration(i) * time.Second),
			sc,
		})
	}

	sids, err := storage.spanIdsForTime(t.Context(), Range{
		Start:  start.Add(3 * time.Second),
		Finish: start.Add(7 * time.Second),
	})
	require.NoError(t, err)
	require.Len(t, sids, 5)

}

type fakeSpan struct {
	start time.Time
	sc    trace.SpanContext
}

func (f *fakeSpan) StartTime() time.Time {
	return f.start
}

func (f *fakeSpan) SpanContext() trace.SpanContext {
	return f.sc
}

func createTrace() []sdktrace.ReadOnlySpan {
	tp, exporter := createTraceProvider()
	tr := tp.Tracer("tests")

	createSpan := func(ctx context.Context, name string) context.Context {
		ctx, span := tr.Start(ctx, name)
		span.End()
		return ctx
	}

	ctx, root := tr.Start(context.Background(), "testing", trace.WithNewRoot())
	createSpan(ctx, "child_one")
	c2 := createSpan(ctx, "child_two")
	createSpan(c2, "grand_one")
	createSpan(c2, "grand_two")
	c3 := createSpan(ctx, "child_three")
	createSpan(c3, "grand_three")

	root.End()

	return exporter.Spans
}
