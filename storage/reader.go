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

type Filter struct {
	Key   string
	Value any
}

func (s *Reader) Filter(ctx context.Context, timeRange Range, filters ...Filter) ([]*domain.Span, error) {
	spans, err := s.spanIdsForTime(ctx, timeRange)
	if err != nil {
		return nil, err
	}

	for _, filter := range filters {

		sids := make(map[string]bool, len(spans))
		prefix := attributePath(s.dataset, filter.Key, "")

		ls, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("romulus"),
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return nil, err
		}

		for _, item := range ls.Contents {
			sid := path.Base(*item.Key)
			if _, found := spans[sid]; found {
				sids[sid] = true
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
