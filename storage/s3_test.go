package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"openretriever/tracing"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func TestStorage(t *testing.T) {

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

type Storage struct {
	s3      *s3.Client
	dataset string
}

func (s *Storage) Write(ctx context.Context, trace []trace.ReadOnlySpan) error {
	if len(trace) == 0 {
		return nil
	}

	if err := s.writeTrace(ctx, trace[0]); err != nil {
		return err
	}

	for _, span := range trace {
		if err := s.writeSpan(ctx, span); err != nil {
			return err
		}
		if err := s.writeTimes(ctx, span); err != nil {
			return err
		}
		if err := s.writeAttributes(ctx, span); err != nil {
			return err
		}
	}

	return nil

}

// mid level api
func (s *Storage) writeTrace(ctx context.Context, span trace.ReadOnlySpan) error {
	path := path.Join(s.dataset, "traces", span.SpanContext().TraceID().String())
	content := ""

	return s.put(ctx, path, []byte(content))
}

func (s *Storage) writeSpan(ctx context.Context, span trace.ReadOnlySpan) error {
	path := path.Join(s.dataset, "spans", span.SpanContext().SpanID().String())
	content, err := json.Marshal(span)
	if err != nil {
		return err
	}

	return s.put(ctx, path, content)
}

func (s *Storage) writeTimes(ctx context.Context, span trace.ReadOnlySpan) error {
	epoch := fmt.Sprint(span.StartTime().Unix())
	path := path.Join(s.dataset, "times", epoch, span.SpanContext().SpanID().String())
	content := []byte{}

	return s.put(ctx, path, content)
}

func (s *Storage) writeAttributes(ctx context.Context, span trace.ReadOnlySpan) error {
	spanId := span.Parent().SpanID().String()
	for _, attr := range span.Attributes() {
		path := path.Join(s.dataset, "attributes", "span."+string(attr.Key), spanId)
		content := fmt.Sprint(attr.Value)

		if err := s.put(ctx, path, []byte(content)); err != nil {
			return err
		}
	}

	for _, attr := range span.Resource().Attributes() {
		path := path.Join(s.dataset, "attributes", "resource."+string(attr.Key), spanId)
		content := fmt.Sprint(attr.Value)

		if err := s.put(ctx, path, []byte(content)); err != nil {
			return err
		}
	}

	return nil
}

// low level api
func (s *Storage) put(ctx context.Context, path string, content []byte) error {
	fmt.Println("put:", path, content)
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("openretriever"),
		Key:    aws.String(path),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		return err
	}

	return nil
}
