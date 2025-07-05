package storage

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"romulus/domain"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestStorageWriting(t *testing.T) {

	storage := createTestStorage(t)

	trace := createTrace()

	err := storage.Write(t.Context(), trace)
	require.NoError(t, err)

	// query it!
	traceId := trace[0].SpanContext.TraceID().String()

	t.Run("read by traceid", func(t *testing.T) {
		read, err := storage.Trace(t.Context(), traceId)
		require.NoError(t, err)
		require.Len(t, read, 7)
	})

	t.Run("find spans by time range", func(t *testing.T) {
		spans, err := storage.spanIdsForTime(t.Context(), Range{
			trace[0].StartTime.Add(-1 * time.Second),
			trace[0].EndTime.Add(1 * time.Second),
		})
		require.NoError(t, err)
		require.Len(t, spans, 7)
	})
}

func TestWritingSpanContents(t *testing.T) {
	spans := createTrace()
	storage := createTestStorage(t)

	tid := spans[0].SpanContext.TraceID()
	sid := spans[len(spans)-1].SpanContext.SpanID()

	t.Log("traceid", tid)
	t.Log("spanid", sid)

	// write to storage
	err := storage.Write(t.Context(), spans)
	require.NoError(t, err)

	t.Run("readSpanContents", func(t *testing.T) {
		// read it back
		read, err := storage.readSpanContents(t.Context(), sid.String())
		require.NoError(t, err)

		span := spans[len(spans)-1]

		require.Equal(t, "testing", read.Name)
		require.Equal(t, span.SpanContext.SpanID(), read.SpanContext.SpanID())
		require.Equal(t, span.SpanContext.TraceID(), read.SpanContext.TraceID())
		require.NotEmpty(t, span.SpanContext.SpanID())
		require.NotEmpty(t, span.SpanContext.TraceID())

		require.Len(t, read.Resource.Attributes(), len(span.Resource.Attributes()))
		require.Equal(t, attribute.Key("service.instance.id"), read.Resource.Attributes()[0].Key)
		require.Equal(t, attribute.StringValue("tests"), read.Resource.Attributes()[0].Value)

		require.Len(t, read.Attributes, len(span.Attributes))
		require.Equal(t, attribute.Key("a.bool.t"), read.Attributes[0].Key)
		require.Equal(t, attribute.BoolValue(true), read.Attributes[0].Value)
	})
}

// func TestSpansIdsForTime(t *testing.T) {

// 	storage := createTestStorage(t)
// 	start := time.Now()
// 	for i := range uint64(10) {

// 		span := SpanStub{
// 			SpanContext: SpanContext{trace.NewSpanContext(trace.SpanContextConfig{
// 				TraceID: NewTraceID(),
// 				SpanID:  NewSpanID(),
// 			})},
// 			StartTime: start.Add(time.Duration(i) * time.Second),
// 		}

// 		storage.writeTimes(t.Context(), span)
// 	}

// 	sids, err := storage.spanIdsForTime(t.Context(), Range{
// 		Start:  start.Add(3 * time.Second),
// 		Finish: start.Add(7 * time.Second),
// 	})
// 	require.NoError(t, err)
// 	require.Len(t, sids, 5)

// }

func createTestStorage(t *testing.T) *Storage {
	cfg, err := config.LoadDefaultConfig(t.Context())
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	require.NotNil(t, client)

	return &Storage{
		s3:      client,
		dataset: "testing",
	}

}

func createTrace() []domain.Span {
	tp, exporter := createTraceProvider()
	tr := tp.Tracer("tests")

	createSpan := func(ctx context.Context, name string) context.Context {
		ctx, span := tr.Start(ctx, name)
		span.End()
		return ctx
	}

	ctx, root := tr.Start(context.Background(), "testing", trace.WithNewRoot())
	root.SetAttributes(
		attribute.Bool("a.bool.t", true),
		attribute.Bool("a.bool.f", false),
		attribute.BoolSlice("a.bools", []bool{true, false, true}),
		attribute.Int("a.int", 19875),
		attribute.String("a.str", "something short"),
	)
	root.SetStatus(codes.Ok, "kaikki hyvin")

	createSpan(ctx, "child_one")
	c2 := createSpan(ctx, "child_two")
	createSpan(c2, "grand_one")
	createSpan(c2, "grand_two")
	c3 := createSpan(ctx, "child_three")
	createSpan(c3, "grand_three")

	root.End()

	return exporter.GetSpans()
}

var randSource *rand.Rand

// invoked by go runtime
func init() {
	if randSource != nil {
		return
	}

	var rngSeed int64
	binary.Read(crand.Reader, binary.LittleEndian, &rngSeed)
	randSource = rand.New(rand.NewSource(rngSeed))
}

func NewTraceID() trace.TraceID {
	tid := trace.TraceID{}
	randSource.Read(tid[:])
	return tid
}

func NewSpanID() trace.SpanID {
	sid := trace.SpanID{}
	randSource.Read(sid[:])
	return sid
}
