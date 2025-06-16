package tracing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func ErrorCtx(ctx context.Context, err error) error {
	span := trace.SpanFromContext(ctx)

	return Error(span, err)
}

func Errorf(s trace.Span, format string, a ...interface{}) error {
	return Error(s, fmt.Errorf(format, a...))
}

func Error(s trace.Span, err error) error {
	s.RecordError(err)
	s.SetStatus(codes.Error, err.Error())

	return err
}

func HashedString(key string, value string) attribute.KeyValue {

	sha := sha256.New()
	sha.Write([]byte(value))
	hash := sha.Sum(nil)

	return attribute.String(key, hex.EncodeToString(hash))
}
