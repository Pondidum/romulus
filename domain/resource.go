package domain

import (
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Resource struct {
	*resource.Resource
}

func (r *Resource) UnmarshalJSON(b []byte) error {

	var read []Attribute
	if err := json.Unmarshal(b, &read); err != nil {
		return err
	}

	attrs := make([]attribute.KeyValue, len(read))
	for i, attr := range read {
		attrs[i] = attr.KeyValue
	}

	r.Resource = resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	return nil
}
