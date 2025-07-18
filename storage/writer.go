package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"romulus/domain"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/attribute"
)

type Writer struct {
	s3      *s3.Client
	dataset string
}

func (s *Writer) Write(ctx context.Context, spans []domain.Span) error {
	if len(spans) == 0 {
		return nil
	}

	// this can be made more efficient as we iterate the spans multiple times in this method, and
	// we could also use go routines to write the spans in parallel.  Leaving this as is for now, as
	// its easier to debug sequential code, and I am not certain on the api usage yet.

	for _, span := range spans {
		sc := span.SpanContext
		sid := sc.SpanContext.SpanID().String()

		content, err := json.Marshal(span)
		if err != nil {
			return err
		}

		if err := s.put(ctx, spanContentPath(s.dataset, sid), content); err != nil {
			return err
		}

		if err := s.put(ctx, tracePath(s.dataset, sc.TraceID().String(), sid), empty); err != nil {
			return err
		}

		if err := s.put(ctx, timesPath(s.dataset, span.StartTime, sid), empty); err != nil {
			return err
		}

		if err := s.writeAttributes(ctx, span); err != nil {
			return err
		}
	}

	return nil

}

// mid level api

var empty = []byte{}

func (s *Writer) writeAttributes(ctx context.Context, span domain.Span) error {
	spanId := span.SpanContext.SpanID().String()

	writeAttr := func(attr attribute.KeyValue) error {
		key := attributePath(s.dataset, string(attr.Key), attr.Value.Type().String(), spanId)
		value, err := json.Marshal(attr.Value.AsInterface())
		if err != nil {
			return err
		}
		if err := s.put(ctx, key, value); err != nil {
			return err
		}

		return nil
	}

	// write attributes
	// write resources
	// write meta
	for _, attr := range span.Attributes {
		if err := writeAttr(attr.KeyValue); err != nil {
			return err
		}
	}

	for _, attr := range span.Resource.Attributes() {
		if err := writeAttr(attr); err != nil {
			return err
		}
	}

	if err := writeAttr(attribute.String("name", span.Name)); err != nil {
		return err
	}

	return nil
}

// low level api
func (s *Writer) put(ctx context.Context, path string, content []byte) error {
	// fmt.Println("put:", path)
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("romulus"),
		Key:    aws.String(path),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		return err
	}

	return nil
}
