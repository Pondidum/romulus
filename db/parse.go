package db

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
)

type Prop interface {
	Key() string
	Type() string
	Value() any

	SerializeValue() ([]byte, error)
	ValueDigest() string
}

func parse(prefix string, thing any) ([]Prop, error) {

	t := reflect.TypeOf(thing)
	v := reflect.ValueOf(thing)
	props := make([]Prop, 0, t.NumField())

	for i := range t.NumField() {
		field := t.Field(i)
		val := v.Field(i)

		switch field.Type.Kind() {
		case reflect.Struct:
			nested, err := parse(field.Name+".", val.Interface())
			if err != nil {
				return nil, err
			}
			props = append(props, nested...)

		default:
			props = append(props, &basicProp{
				k: prefix + field.Name,
				t: field.Type.Name(),
				v: val.Interface(),
			})
		}
	}

	return props, nil
}

var _ Prop = &basicProp{}

type basicProp struct {
	k string
	v any
	t string
}

func (bp *basicProp) Key() string  { return bp.k }
func (bp *basicProp) Value() any   { return bp.v }
func (bp *basicProp) Type() string { return bp.t }
func (bp *basicProp) SerializeValue() ([]byte, error) {

	switch val := bp.v.(type) {

	case string:
		return []byte(val), nil

	default:
		return json.Marshal(bp.v)
	}

}
func (bp *basicProp) ValueDigest() string {
	v := fmt.Sprint(bp.v)
	short := v[0:min(len(v), 20)]

	switch bp.v.(type) {

	case string:
		return hex.EncodeToString([]byte(short))

	default:
		return short
	}
}
