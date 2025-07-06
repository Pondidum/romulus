package storage

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"romulus/domain"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func createTraceProvider() (*trace.TracerProvider, *InMemoryExporter) {

	exporter := NewInMemoryExporter()

	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("romulus"),
			semconv.ServiceInstanceID("tests"),
		)),
	)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, exporter
}

func NewInMemoryExporter() *InMemoryExporter {
	return new(InMemoryExporter)
}

type InMemoryExporter struct {
	mu sync.Mutex
	ss []domain.Span
}

func (imsb *InMemoryExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	imsb.mu.Lock()
	defer imsb.mu.Unlock()
	imsb.ss = append(imsb.ss, SpansFromReadOnlySpans(spans)...)
	return nil
}

func (imsb *InMemoryExporter) Shutdown(context.Context) error {
	imsb.Reset()
	return nil
}

func (imsb *InMemoryExporter) Reset() {
	imsb.mu.Lock()
	defer imsb.mu.Unlock()
	imsb.ss = nil
}

func (imsb *InMemoryExporter) GetSpans() []domain.Span {
	imsb.mu.Lock()
	defer imsb.mu.Unlock()
	ret := make([]domain.Span, len(imsb.ss))
	copy(ret, imsb.ss)
	return ret
}

func SpansFromReadOnlySpans(ro []trace.ReadOnlySpan) []domain.Span {
	if len(ro) == 0 {
		return nil
	}

	s := make([]domain.Span, len(ro))
	for i, r := range ro {
		s[i] = SpanFromReadOnlySpan(r)
	}

	return s
}

func SpanFromReadOnlySpan(ro trace.ReadOnlySpan) domain.Span {
	if ro == nil {
		return domain.Span{}
	}

	return domain.Span{
		Name:                 ro.Name(),
		SpanContext:          domain.SpanContext{ro.SpanContext()},
		Parent:               domain.SpanContext{ro.Parent()},
		SpanKind:             ro.SpanKind(),
		StartTime:            ro.StartTime(),
		EndTime:              ro.EndTime(),
		Attributes:           fromAttributes(ro.Attributes()),
		Events:               ro.Events(),
		Links:                ro.Links(),
		Status:               ro.Status(),
		DroppedAttributes:    ro.DroppedAttributes(),
		DroppedEvents:        ro.DroppedEvents(),
		DroppedLinks:         ro.DroppedLinks(),
		ChildSpanCount:       ro.ChildSpanCount(),
		Resource:             &domain.Resource{ro.Resource()},
		InstrumentationScope: ro.InstrumentationScope(),
	}
}

func fromAttributes(attrs []attribute.KeyValue) []domain.Attribute {
	wrapped := make([]domain.Attribute, len(attrs))
	for i, attr := range attrs {
		wrapped[i] = domain.Attribute{attr}
	}
	return wrapped
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

func NewTraceID() oteltrace.TraceID {
	tid := oteltrace.TraceID{}
	randSource.Read(tid[:])
	return tid
}

func NewSpanID() oteltrace.SpanID {
	sid := oteltrace.SpanID{}
	randSource.Read(sid[:])
	return sid
}
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
