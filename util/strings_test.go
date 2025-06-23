package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommonPrefix(t *testing.T) {
	cases := [][]string{
		{"aaaa", "aaaa", "aaaa"},
		{"aaaa", "aaax", "aaa"},
		{"aaax", "aaaa", "aaa"},
		{"aaaa", "xxxx", ""},
		{"aaaa", "", ""},
		{"axax", "aaaa", "a"},
	}

	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc[2], CommonPrefix(tc[0], tc[1]))
		})
	}
}
