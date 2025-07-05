package domain

import (
	"time"

	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Spans []Spans

type Span struct {
	Name                 string
	SpanContext          SpanContext
	Parent               SpanContext
	SpanKind             trace.SpanKind
	StartTime            time.Time
	EndTime              time.Time
	Attributes           []Attribute
	Events               []sdktrace.Event
	Links                []sdktrace.Link
	Status               sdktrace.Status
	DroppedAttributes    int
	DroppedEvents        int
	DroppedLinks         int
	ChildSpanCount       int
	Resource             *Resource
	InstrumentationScope instrumentation.Scope
}
