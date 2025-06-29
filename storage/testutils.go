package storage

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func createTraceProvider() (*trace.TracerProvider, *tracetest.InMemoryExporter) {

	exporter := tracetest.NewInMemoryExporter()

	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("openretriever"),
			semconv.ServiceInstanceID("tests"),
		)),
	)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, exporter
}
