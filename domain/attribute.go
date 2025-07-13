package domain

import (
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
)

type Attribute struct {
	attribute.KeyValue
}

func (a *Attribute) UnmarshalJSON(b []byte) error {

	type helper struct {
		Key   string
		Value struct {
			Type  string
			Value any
		}
	}

	kv := helper{}
	if err := json.Unmarshal(b, &kv); err != nil {
		return err
	}

	a.Key = attribute.Key(kv.Key)
	a.Value = ParseValue(kv.Value.Type, kv.Value.Value)

	return nil
}

func ParseValue(valType string, val any) attribute.Value {

	switch valType {
	case "BOOL":
		return attribute.BoolValue(val.(bool))

	case "BOOLSLICE":
		sl := val.([]any)
		bools := make([]bool, len(sl))
		for i, v := range sl {
			bools[i] = v.(bool)
		}
		return attribute.BoolSliceValue(bools)

	case "INT64":
		return attribute.Int64Value(int64(val.(float64)))

	case "INT64SLICE":
		sl := val.([]any)
		ints := make([]int64, len(sl))
		for i, v := range sl {
			ints[i] = int64(v.(float64))
		}
		return attribute.Int64SliceValue(ints)

	case "FLOAT64":
		return attribute.Float64Value(val.(float64))

	case "FLOAT64SLICE":
		sl := val.([]any)
		floats := make([]float64, len(sl))
		for i, v := range sl {
			floats[i] = v.(float64)
		}
		return attribute.Float64SliceValue(floats)

	case "STRING":
		return attribute.StringValue(val.(string))

	case "STRINGSLICE":
		sl := val.([]any)
		strings := make([]string, len(sl))
		for i, v := range sl {
			strings[i] = v.(string)
		}
		return attribute.StringSliceValue(strings)
	}

	return attribute.Value{}
}
