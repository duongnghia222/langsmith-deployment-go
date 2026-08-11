package jsonbutil

import (
	"testing"

	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	enumdurability "github.com/duongnghia222/langsmith-deployment-go/gen/enum_durability"
	"google.golang.org/protobuf/proto"
)

func TestMarshalNilReturnsEmptyJSONObject(t *testing.T) {
	b, err := Marshal(nil)
	if err != nil {
		t.Fatalf("Marshal(nil) error = %v", err)
	}
	if string(b) != `{}` {
		t.Fatalf("Marshal(nil) = %q, want %q", string(b), `{}`)
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	gid := "graph-abc"
	runID := "run-123"
	dur := enumdurability.Durability_ASYNC
	src := &engcommon.EngineRunnableConfig{
		Tags:       []string{"a", "b"},
		RunName:    proto.String("test-run"),
		RunId:      &runID,
		GraphId:    &gid,
		Durability: &dur,
	}

	b, err := Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	dst := &engcommon.EngineRunnableConfig{}
	if err := Unmarshal(b, dst); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Fatalf("round-trip mismatch:\nsrc=%v\ndst=%v", src, dst)
	}
}

func TestUnmarshalEmptyReturnsNoError(t *testing.T) {
	dst := &engcommon.EngineRunnableConfig{}
	if err := Unmarshal(nil, dst); err != nil {
		t.Fatalf("Unmarshal(nil) error = %v", err)
	}
	if err := Unmarshal([]byte{}, dst); err != nil {
		t.Fatalf("Unmarshal([]) error = %v", err)
	}
	if err := Unmarshal([]byte(`{}`), dst); err != nil {
		t.Fatalf("Unmarshal({}) error = %v", err)
	}
}

func TestUnmarshalLegacyPythonDictReturnsErrLegacyFormat(t *testing.T) {
	dst := &engcommon.EngineRunnableConfig{}
	if err := Unmarshal([]byte(`{"configurable":{"thread_id":"abc"},"tags":["x"]}`), dst); err != nil {
		t.Fatalf("Unmarshal legacy: %v", err)
	}
	if len(dst.Tags) != 1 || dst.Tags[0] != "x" {
		t.Fatalf("expected tags=[x], got %v", dst.Tags)
	}
}

func TestUnmarshalGarbageReturnsError(t *testing.T) {
	dst := &engcommon.EngineRunnableConfig{}
	if err := Unmarshal([]byte(`not json at all`), dst); err == nil {
		t.Fatal("expected error for invalid json, got nil")
	}
}
