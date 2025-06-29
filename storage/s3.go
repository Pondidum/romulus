package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"openretriever/util"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"

	oteltrace "go.opentelemetry.io/otel/trace"
)

type Storage struct {
	s3      *s3.Client
	dataset string
}

func (s *Storage) Trace(ctx context.Context, traceId string) ([]trace.ReadOnlySpan, error) {
	path := path.Join(s.dataset, "traces", traceId)
	list, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("openretriever"),
		Prefix: aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	spans := make([]trace.ReadOnlySpan, len(list.Contents))
	wg := errgroup.Group{}
	for i, obj := range list.Contents {
		wg.Go(func() error {
			span, err := s.readSpanContents(ctx, *obj.Key)
			if err != nil {
				return err
			}
			spans[i] = span
			return nil
		})
	}

	return spans, nil
}

type Range struct {
	Start  time.Time
	Finish time.Time
}

func (s *Storage) spanIdsForTime(ctx context.Context, timeRange Range) (map[string]bool, error) {

	start := timeRange.Start.Unix()
	finish := timeRange.Finish.Unix()
	prefix := util.CommonPrefix(fmt.Sprint(start), fmt.Sprint(finish))

	keyPath := path.Join(s.dataset, "times", prefix)

	list, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("openretriever"),
		Prefix: aws.String(keyPath),
	})
	if err != nil {
		return nil, err
	}

	spanIds := make(map[string]bool, len(list.Contents))
	for _, obj := range list.Contents {
		k := path.Base(path.Dir(*obj.Key))
		ts, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, err
		}

		if ts < start {
			continue
		}
		if ts > finish {
			break
		}

		spanIds[path.Base(*obj.Key)] = true
	}

	return spanIds, nil
}

func (s *Storage) readSpanContents(ctx context.Context, path string) (trace.ReadOnlySpan, error) {

	obj, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("openretriever"),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()

	var span trace.ReadOnlySpan
	if err := json.NewDecoder(obj.Body).Decode(&span); err != nil {
		return nil, err
	}

	return span, nil
}

func (s *Storage) Write(ctx context.Context, trace []trace.ReadOnlySpan) error {
	if len(trace) == 0 {
		return nil
	}

	// this can be made more efficient as we iterate the spans multiple times in this method, and
	// we could also use go routines to write the spans in parallel.  Leaving this as is for now, as
	// its easier to debug sequential code, and I am not certain on the api usage yet.
	if err := s.writeTrace(ctx, trace); err != nil {
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
func (s *Storage) writeTrace(ctx context.Context, trace []trace.ReadOnlySpan) error {
	traceId := trace[0].SpanContext().TraceID().String()
	content := []byte{}

	for _, span := range trace {
		spanId := span.SpanContext().SpanID().String()
		path := path.Join(s.dataset, "traces", traceId, spanId)

		if err := s.put(ctx, path, content); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) writeSpan(ctx context.Context, span trace.ReadOnlySpan) error {
	path := path.Join(s.dataset, "spans", span.SpanContext().SpanID().String())
	content, err := json.Marshal(span)
	if err != nil {
		return err
	}

	return s.put(ctx, path, content)
}

type Timed interface {
	StartTime() time.Time
	SpanContext() oteltrace.SpanContext
}

func (s *Storage) writeTimes(ctx context.Context, span Timed) error {
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
