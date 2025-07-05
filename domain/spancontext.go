package domain

import (
	"encoding/hex"
	"encoding/json"

	"go.opentelemetry.io/otel/trace"
)

type SpanContext struct {
	trace.SpanContext
}

type spanContextDto struct {
	TraceID    string
	SpanID     string
	TraceFlags string
	Remote     bool
}

func (sc *SpanContext) UnmarshalJSON(b []byte) error {
	var err error
	var dto spanContextDto

	if err := json.Unmarshal(b, &dto); err != nil {
		return err
	}

	cfg := trace.SpanContextConfig{}
	if dto.TraceID != "" && dto.TraceID != "00000000000000000000000000000000" {
		if cfg.TraceID, err = trace.TraceIDFromHex(dto.TraceID); err != nil {
			return err
		}
	}

	if dto.SpanID != "" && dto.SpanID != "0000000000000000" {
		if cfg.SpanID, err = trace.SpanIDFromHex(dto.SpanID); err != nil {
			return err
		}
	}

	h, err := hex.DecodeString(dto.TraceFlags)
	if err != nil {
		return err
	}
	if len(h) > 0 {
		cfg.TraceFlags = trace.TraceFlags(h[0])
	}

	cfg.Remote = dto.Remote

	// cfg.TraceState = ...

	sc.SpanContext = trace.NewSpanContext(cfg)
	return nil
}
