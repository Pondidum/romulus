package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestDeserialization(t *testing.T) {
	cases := []struct {
		Json     string
		Expected attribute.KeyValue
	}{
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "BOOL",
          "Value": true
        }
      }`,
			Expected: attribute.Bool("the key", true),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "BOOLSLICE",
          "Value": [ true, false, true ]
        }
      }`,
			Expected: attribute.BoolSlice("the key", []bool{true, false, true}),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "INT64",
          "Value": 19875
        }
      }`,
			Expected: attribute.Int64("the key", 19875),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "INT64SLICE",
          "Value": [ 19875, 264, 877 ]
        }
      }`,
			Expected: attribute.Int64Slice("the key", []int64{19875, 264, 877}),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "FLOAT64",
          "Value": 23.78
        }
      }`,
			Expected: attribute.Float64("the key", 23.78),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "FLOAT64SLICE",
          "Value": [ 1987.5, 26.4, 8.77 ]
        }
      }`,
			Expected: attribute.Float64Slice("the key", []float64{1987.5, 26.4, 8.77}),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "STRING",
          "Value": "some value"
        }
      }`,
			Expected: attribute.String("the key", "some value"),
		},
		{
			Json: `{
        "Key": "the key",
        "Value": {
          "Type": "STRINGSLICE",
          "Value": [ "some", "other", "value" ]
        }
      }`,
			Expected: attribute.StringSlice("the key", []string{"some", "other", "value"}),
		},
	}

	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			var attr Attribute
			require.NoError(t, json.Unmarshal([]byte(tc.Json), &attr))
			require.Equal(t, tc.Expected, attr.KeyValue)
		})
	}
}
