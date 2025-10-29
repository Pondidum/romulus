package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsing(t *testing.T) {

	cases := []struct {
		thing    any
		expected []Prop
	}{
		{
			thing: struct {
				Name   string
				Age    int32
				Active bool
			}{
				Name:   "test",
				Age:    43,
				Active: true,
			},
			expected: []Prop{
				&basicProp{k: "Name", v: "test", t: "string"},
				&basicProp{k: "Age", v: int32(43), t: "int32"},
				&basicProp{k: "Active", v: true, t: "bool"},
			},
		},
	}

	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			actual, err := parse(tc.thing)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}

}
