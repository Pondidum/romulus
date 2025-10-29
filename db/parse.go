package db

import (
	"fmt"
	"reflect"
)

type Prop interface {
	Key() string
	Value() string
	Type() string

	ValueDigest() string
}

func parse(thing any) ([]Prop, error) {

	t := reflect.TypeOf(thing)
	v := reflect.ValueOf(thing)
	props := make([]Prop, t.NumField())
	for i := range t.NumField() {
		field := t.Field(i)
		val := v.Field(i)

		prop := &basicProp{
			k: field.Name,
			t: field.Type.Name(),
			v: val.Interface(),
		}

		// switch field.Type.Name() {
		// case "string":
		// 	prop.v = val.String()

		// case "bool":
		// 	prop.v = val.Bool()
		// case "int", "int32", "int64":
		// 	prop.v = val.Int()
		// }

		props[i] = prop
	}

	return props, nil
}

var _ Prop = &basicProp{}

type basicProp struct {
	k string
	v any
	t string
}

func (bp *basicProp) Key() string   { return bp.k }
func (bp *basicProp) Value() string { return fmt.Sprint(bp.v) }
func (bp *basicProp) Type() string  { return bp.t }
func (bp *basicProp) ValueDigest() string {
	v := bp.Value()
	return v[0:min(len(v), 20)]
}
