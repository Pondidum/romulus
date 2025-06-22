package storage

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"openretriever/tracing"
)

func createTraceProvider() (*trace.TracerProvider, *tracing.MemoryExporter) {
	exporter := tracing.NewMemoryExporter()

	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
		)),
	)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, exporter
}
