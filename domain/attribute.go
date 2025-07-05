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

	switch kv.Value.Type {
	case "BOOL":
		a.Value = attribute.BoolValue(kv.Value.Value.(bool))

	case "BOOLSLICE":
		sl := kv.Value.Value.([]any)
		bools := make([]bool, len(sl))
		for i, v := range sl {
			bools[i] = v.(bool)
		}
		a.Value = attribute.BoolSliceValue(bools)

	case "INT64":
		a.Value = attribute.Int64Value(int64(kv.Value.Value.(float64)))

	case "INT64SLICE":
		sl := kv.Value.Value.([]any)
		ints := make([]int64, len(sl))
		for i, v := range sl {
			ints[i] = int64(v.(float64))
		}
		a.Value = attribute.Int64SliceValue(ints)

	case "FLOAT64":
		a.Value = attribute.Float64Value(kv.Value.Value.(float64))

	case "FLOAT64SLICE":
		sl := kv.Value.Value.([]any)
		floats := make([]float64, len(sl))
		for i, v := range sl {
			floats[i] = v.(float64)
		}
		a.Value = attribute.Float64SliceValue(floats)

	case "STRING":
		a.Value = attribute.StringValue(kv.Value.Value.(string))

	case "STRINGSLICE":
		sl := kv.Value.Value.([]any)
		strings := make([]string, len(sl))
		for i, v := range sl {
			strings[i] = v.(string)
		}
		a.Value = attribute.StringSliceValue(strings)
	}

	return nil
}
