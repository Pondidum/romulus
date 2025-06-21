package storage

import (
	"openretriever/tracing"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

	tp, exporter := createTraceProvider()

	_, span := tp.Tracer(t.Name()).Start(t.Context(), "testing")
	span.End()

	err = storage.Write(t.Context(), []trace.ReadOnlySpan{
		exporter.Spans[0],
	})
	require.NoError(t, err)

}
