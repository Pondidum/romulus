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
	prefix := path.Join(s.dataset, "traces", traceId)
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

	keyPath := path.Join(s.dataset, "times", prefix)

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
		if err := s.writeSpanContents(ctx, span); err != nil {
			return err
		}
		if err := s.writeTraceIndex(ctx, span); err != nil {
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
func (s *Storage) writeTraceIndex(ctx context.Context, span domain.Span) error {
	sc := span.SpanContext
	path := path.Join(s.dataset, "traces", sc.TraceID().String(), sc.SpanID().String())

	if err := s.put(ctx, path, empty); err != nil {
		return err
	}

	return nil
}

func (s *Storage) writeSpanContents(ctx context.Context, span domain.Span) error {
	path := path.Join(s.dataset, "spans", span.SpanContext.SpanID().String())
	content, err := json.Marshal(span)
	if err != nil {
		return err
	}

	return s.put(ctx, path, content)
}

func (s *Storage) readSpanContents(ctx context.Context, spanId string) (*domain.Span, error) {
	key := path.Join(s.dataset, "spans", spanId)
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

func (s *Storage) writeTimes(ctx context.Context, span domain.Span) error {
	epoch := fmt.Sprint(span.StartTime.Unix())
	path := path.Join(s.dataset, "times", epoch, span.SpanContext.SpanID().String())

	return s.put(ctx, path, empty)
}

func (s *Storage) writeAttributes(ctx context.Context, span domain.Span) error {
	spanId := span.SpanContext.SpanID().String()

	basePath := path.Join(s.dataset, "attributes")
	writeAttr := func(prefix, key, val string) error {
		path := path.Join(basePath, prefix+key, spanId)
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
