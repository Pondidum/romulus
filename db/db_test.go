package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsing(t *testing.T) {

	thing := thingTest{
		Name:   "test",
		Age:    43,
		Active: true,
		Config: thingTestNested{
			Enabled: false,
			Counter: 52,
		},
	}

	expected := []Prop{
		&basicProp{k: "Name", v: "test", t: "string"},
		&basicProp{k: "Age", v: int32(43), t: "int32"},
		&basicProp{k: "Active", v: true, t: "bool"},
		&basicProp{k: "Config.Enabled", v: false, t: "bool"},
		&basicProp{k: "Config.Counter", v: int64(52), t: "int64"},
	}

	actual, err := parse("", thing)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

}

func TestWriting(t *testing.T) {

	thing := thingTest{
		Name:   "test",
		Age:    43,
		Active: true,
		Config: thingTestNested{
			Enabled: false,
			Counter: 52,
		},
	}

	writer := NewMemoryStorage()
	err := Write(t.Context(), writer, "uuid", thing)
	require.NoError(t, err)

	json, _ := serializeThing(thing)
	expected := map[string][]byte{
		"objects/uuid":                      json,
		"indexes/Name/74657374/uuid":        []byte("test"),
		"indexes/Age/43/uuid":               []byte("43"),
		"indexes/Active/true/uuid":          []byte("true"),
		"indexes/Config.Enabled/false/uuid": []byte("false"),
		"indexes/Config.Counter/52/uuid":    []byte("52"),
	}
	require.Equal(t, expected, writer.store)
}

func TestIndexProps(t *testing.T) {

	common := "this is the common prefix test string "

	writer := NewMemoryStorage()

	require.NoError(t, indexProps(t.Context(), writer, "one", []Prop{
		&basicProp{k: "comment", t: "string", v: common + "one"},
	}))
	require.NoError(t, indexProps(t.Context(), writer, "two", []Prop{
		&basicProp{k: "comment", t: "string", v: common + "two"},
	}))

	expected := map[string][]byte{
		"indexes/comment/746869732069732074686520636f6d6d6f6e2070/one": []byte(common + "one"),
		"indexes/comment/746869732069732074686520636f6d6d6f6e2070/two": []byte(common + "two"),
	}

	require.Equal(t, expected, writer.store)
}

type thingTest struct {
	Name   string
	Age    int32
	Active bool
	Config struct {
		Enabled bool
		Counter int64
	}
}
type thingTestNested struct {
	Enabled bool
	Counter int64
}
