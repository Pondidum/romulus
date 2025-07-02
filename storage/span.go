package storage

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type SpanStubs []SpanStub

type SpanStub struct {
	Name                 string
	SpanContext          SpanContext
	Parent               SpanContext
	SpanKind             trace.SpanKind
	StartTime            time.Time
	EndTime              time.Time
	Attributes           []attribute.KeyValue
	Events               []sdktrace.Event
	Links                []sdktrace.Link
	Status               sdktrace.Status
	DroppedAttributes    int
	DroppedEvents        int
	DroppedLinks         int
	ChildSpanCount       int
	Resource             *resource.Resource
	InstrumentationScope instrumentation.Scope
}

type SpanContext struct {
	trace.SpanContext
}

type SpanContextDto struct {
	TraceID    string
	SpanID     string
	TraceFlags string
	Remote     bool
}

func (sc *SpanContext) UnmarshalJSON(b []byte) error {
	var err error
	var dto SpanContextDto

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	cfg := trace.SpanContextConfig{}
	if dto.TraceID != "" && dto.TraceID != "00000000000000000000000000000000" {
		if cfg.TraceID, err = trace.TraceIDFromHex(dto.TraceID); err != nil {
			return err
		}
	}

	if dto.SpanID != "" && dto.SpanID != "0000000000000000" {
		if cfg.SpanID, err = trace.SpanIDFromHex(dto.SpanID); err != nil {
			return err
		}
	}

	h, err := hex.DecodeString(dto.TraceFlags)
	if err != nil {
		return err
	}
	if len(h) > 0 {
		cfg.TraceFlags = trace.TraceFlags(h[0])
	}

	cfg.Remote = dto.Remote

	// cfg.TraceState = ...

	sc.SpanContext = trace.NewSpanContext(cfg)
	return nil
}
