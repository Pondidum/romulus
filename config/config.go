package config

import (
	"context"
)

type Config struct {
	DatabaseFile string
}

func CreateConfig(ctx context.Context) (*Config, error) {
	return &Config{
		DatabaseFile: "dev.sqlite",
	}, nil
}
