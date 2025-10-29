package db

import (
	"context"
	"path"
)

type StorageWriter interface {
	Put(ctx context.Context, path string, content []byte) error
}

func Write(ctx context.Context, sw StorageWriter, id string, thing any) error {

	props, err := parse(thing)
	if err != nil {
		return err
	}

	raw, err := serializeThing(thing)
	if err != nil {
		return err
	}

	if err := sw.Put(ctx, "objects/"+id, raw); err != nil {
		return err
	}

	if err := indexBlob(ctx, sw, id, props); err != nil {
		return err
	}

	return nil
}

func parse(thing any) ([]Prop, error) {
	return nil, nil
}

func serializeThing(thing any) ([]byte, error) {
	return nil, nil
}

func serializeProp(prop Prop) ([]byte, error) {
	return nil, nil
}

func indexBlob(ctx context.Context, sw StorageWriter, id string, blob []Prop) error {

	for _, prop := range blob {

		b, err := serializeProp(prop)
		if err != nil {
			return err
		}

		if err := sw.Put(ctx, path.Join("indexes", prop.Key(), prop.ValueDigest(), id), b); err != nil {
			return err
		}
	}

	return nil
}

type Prop interface {
	Key() string
	Value() string
	Type() string

	ValueDigest() string
}
