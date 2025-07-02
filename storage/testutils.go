package storage

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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
	ss SpanStubs
}

func (imsb *InMemoryExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	imsb.mu.Lock()
	defer imsb.mu.Unlock()
	imsb.ss = append(imsb.ss, SpanStubsFromReadOnlySpans(spans)...)
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

func (imsb *InMemoryExporter) GetSpans() SpanStubs {
	imsb.mu.Lock()
	defer imsb.mu.Unlock()
	ret := make(SpanStubs, len(imsb.ss))
	copy(ret, imsb.ss)
	return ret
}

func SpanStubsFromReadOnlySpans(ro []trace.ReadOnlySpan) SpanStubs {
	if len(ro) == 0 {
		return nil
	}

	s := make(SpanStubs, 0, len(ro))
	for _, r := range ro {
		s = append(s, SpanStubFromReadOnlySpan(r))
	}

	return s
}

func SpanStubFromReadOnlySpan(ro trace.ReadOnlySpan) SpanStub {
	if ro == nil {
		return SpanStub{}
	}

	return SpanStub{
		Name:                 ro.Name(),
		SpanContext:          SpanContext{ro.SpanContext()},
		Parent:               SpanContext{ro.Parent()},
		SpanKind:             ro.SpanKind(),
		StartTime:            ro.StartTime(),
		EndTime:              ro.EndTime(),
		Attributes:           ro.Attributes(),
		Events:               ro.Events(),
		Links:                ro.Links(),
		Status:               ro.Status(),
		DroppedAttributes:    ro.DroppedAttributes(),
		DroppedEvents:        ro.DroppedEvents(),
		DroppedLinks:         ro.DroppedLinks(),
		ChildSpanCount:       ro.ChildSpanCount(),
		Resource:             ro.Resource(),
		InstrumentationScope: ro.InstrumentationScope(),
	}
}
