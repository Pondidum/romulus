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
