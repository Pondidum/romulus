package storage

import (
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type Reader struct {
	s3      *s3.Client
	dataset string
}

type Range struct {
	Start  time.Time
	Finish time.Time
}

type SpanFilter []attribute.KeyValue

func (s *Reader) Filter(ctx context.Context, timeRange Range, spanFilters ...SpanFilter) ([]trace.TraceID, error) {
	spans, err := s.spanIdsForTime(ctx, timeRange)
	if err != nil {
		return nil, err
	}

	traces := map[trace.TraceID]bool{}

	for i, spanFilter := range spanFilters {
		matches, err := s.filterSingle(ctx, spans, spanFilter)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			for _, match := range matches {
				traces[match.SpanContext.TraceID()] = true
			}
		} else {
			tids := map[trace.TraceID]bool{}
			for _, match := range matches {
				tid := match.SpanContext.TraceID()
				if _, found := traces[tid]; found {
					tids[tid] = true
				}
			}
			traces = tids
		}
	}

	traceIds := make([]trace.TraceID, 0, len(traces))
	for tid := range traces {
		traceIds = append(traceIds, tid)
	}

	return traceIds, nil
}

func (s *Reader) filterSingle(ctx context.Context, spans map[string]bool, spanFilter SpanFilter) ([]*domain.Span, error) {
	for _, filter := range spanFilter {

		prefix := attributePath(s.dataset, string(filter.Key), filter.Value.Type().String(), "")

		ls, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("romulus"),
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return nil, err
		}

		sids := make(map[string]bool, len(spans))
		for _, item := range ls.Contents {
			sid := path.Base(*item.Key)
			if _, found := spans[sid]; found {

				value, err := s.readAttribute(ctx, string(filter.Key), filter.Value.Type(), sid)
				if err != nil {
					return nil, err
				}

				// might need logic, for non strings? not sure.
				if value == filter.Value {
					sids[sid] = true
				}
			}
		}

		spans = sids
	}

	sids := make([]string, 0, len(spans))
	for sid := range spans {
		sids = append(sids, sid)
	}

	return s.readSpans(ctx, sids)
}

func (s *Reader) readAttribute(ctx context.Context, attrKey string, attrType attribute.Type, spanId string) (attribute.Value, error) {
	key := attributePath(s.dataset, attrKey, attrType.String(), spanId)

	obj, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("romulus"),
		Key:    aws.String(key),
	})
	if err != nil {
		return attribute.Value{}, err
	}

	defer obj.Body.Close()

	// there is probably a more efficient way to do this
	var value any
	if err := json.NewDecoder(obj.Body).Decode(&value); err != nil {
		return attribute.Value{}, err
	}

	attr := domain.ParseValue(attrType.String(), value)

	return attr, nil
}

func (s *Reader) Trace(ctx context.Context, traceId string) ([]*domain.Span, error) {
	prefix := tracePath(s.dataset, traceId, "")
	list, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("romulus"),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	spanids := make([]string, len(list.Contents))
	for i, obj := range list.Contents {
		spanids[i] = path.Base(*obj.Key)
	}
	spans, err := s.readSpans(ctx, spanids)
	if err != nil {
		return nil, err
	}
	return spans, nil
}

func (s *Reader) readSpans(ctx context.Context, spanids []string) ([]*domain.Span, error) {

	var err error
	spans := make([]*domain.Span, len(spanids))
	wg := errgroup.Group{}
	for i, sid := range spanids {
		wg.Go(func() error {
			if spans[i], err = s.readSpanContents(ctx, sid); err != nil {
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

func (s *Reader) spanIdsForTime(ctx context.Context, timeRange Range) (map[string]bool, error) {

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

func (s *Reader) readSpanContents(ctx context.Context, spanId string) (*domain.Span, error) {
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
