// Package jsonbutil provides helpers for marshaling and unmarshaling proto
// messages to and from JSONB column bytes via protojson.
//
// Most on-disk JSONB columns owned by LSD that carry proto-typed values
// (Assistant.config, Run.kwargs, Thread.interrupts) use protojson as their
// canonical format. Empty bytes and "{}" deserialize to a zero-value proto
// message without error. Pre-LSD rows written in Python-dict format will
// have unknown fields silently dropped (DiscardUnknown:true), so callers should
// expect a sparse proto when reading legacy rows.
//
// Cron.payload is the one exception: it's stored as the Python-shaped dict
// crons.py's _payload_proto_to_dict/_payload_dict_to_proto produce/consume
// (internal/crons/payload.go), not protojson, so storage/ops.py's raw-SQL
// fallbacks and the gRPC client agree on shape. Rows written before that
// switch are still protojson-shaped; internal/crons/service.go's
// decodePayload detects and handles that legacy shape.
package jsonbutil

import (
	"bytes"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var marshaler = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: false,
}

var unmarshaler = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

// Marshal serializes a proto message to its protojson representation.
// Returns "{}" for a nil message, never an error for that case.
func Marshal(m proto.Message) ([]byte, error) {
	if m == nil {
		return []byte(`{}`), nil
	}
	return marshaler.Marshal(m)
}

// Unmarshal parses protojson bytes into dst. Empty/nil bytes and "{}" are
// treated as a no-op (dst remains its zero value, no error).
func Unmarshal(b []byte, dst proto.Message) error {
	if len(b) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(b), []byte(`{}`)) {
		return nil
	}
	return unmarshaler.Unmarshal(b, dst)
}
