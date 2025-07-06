package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"romulus/domain"
	"romulus/util"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

type Storage struct {
	s3      *s3.Client
	dataset string
}

func (s *Storage) Trace(ctx context.Context, traceId string) ([]*domain.Span, error) {
	prefix := tracePath(s.dataset, traceId, "")
	list, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("romulus"),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	spans := make([]*domain.Span, len(list.Contents))
	wg := errgroup.Group{}
	for i, obj := range list.Contents {
		wg.Go(func() error {
			if spans[i], err = s.readSpanContents(ctx, path.Base(*obj.Key)); err != nil {
				return err
			}
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, err
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

	keyPath := timesPrefixPath(s.dataset, prefix)

	list, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("romulus"),
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

func (s *Storage) Write(ctx context.Context, spans []domain.Span) error {
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

func (s *Storage) readSpanContents(ctx context.Context, spanId string) (*domain.Span, error) {
	key := spanContentPath(s.dataset, spanId)
	obj, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("romulus"),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("error reading key %s: %w", key, err)
	}
	defer obj.Body.Close()

	span := &domain.Span{}
	if err := json.NewDecoder(obj.Body).Decode(span); err != nil {
		return nil, err
	}

	return span, nil
}

var empty = []byte{}

func (s *Storage) writeAttributes(ctx context.Context, span domain.Span) error {
	spanId := span.SpanContext.SpanID().String()

	writeAttr := func(prefix, key, val string) error {

		path := attributePath(s.dataset, prefix+key, spanId)
		if err := s.put(ctx, path, []byte(val)); err != nil {
			return err
		}
		return nil
	}

	for _, attr := range span.Attributes {
		if err := writeAttr("span.", string(attr.Key), fmt.Sprint(attr.Value)); err != nil {
			return err
		}
	}

	for _, attr := range span.Resource.Attributes() {
		if err := writeAttr("resource.", string(attr.Key), fmt.Sprint(attr.Value)); err != nil {
			return err
		}
	}

	if err := writeAttr("span:", "name", span.Name); err != nil {
		return err
	}
	if err := writeAttr("span:", "traceid", span.SpanContext.TraceID().String()); err != nil {
		return err
	}
	if parent := span.Parent; parent.SpanID().IsValid() {
		if err := writeAttr("span:", "parentid", parent.SpanID().String()); err != nil {
			return err
		}
	}

	return nil
}

// low level api
func (s *Storage) put(ctx context.Context, path string, content []byte) error {
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
