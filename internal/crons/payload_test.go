package crons

import (
	"encoding/json"
	"testing"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	enummultitaskstrategy "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	"google.golang.org/protobuf/proto"
)

// TestPayloadRoundTrip_DictToProtoToDict is 4a's required round-trip test:
// dict -> proto -> dict is identity for a payload containing input, config,
// stream_mode, multitask_strategy, and on_completion (the last three are not
// CronPayload "simple keys" — crons.py:_payload_dict_to_proto routes them
// through extra_json verbatim, so this also exercises that passthrough).
//
// Comparison goes through a JSON re-marshal on both sides rather than
// reflect.DeepEqual: Go map key order is nondeterministic but
// encoding/json.Marshal sorts map keys, so two logically-identical dicts
// always produce byte-identical JSON regardless of iteration order or
// []any-vs-[]string representation differences introduced by the codec.
func TestPayloadRoundTrip_DictToProtoToDict(t *testing.T) {
	raw := []byte(`{
		"assistant_id": "a-1",
		"input": {"q": "hello"},
		"config": {
			"tags": ["tag1", "tag2"],
			"metadata": {"run_attempt": 2, "custom": "val"},
			"max_concurrency": 5,
			"configurable": {
				"thread_id": "t-1",
				"checkpoint_ns": "ns-1",
				"custom_configurable_key": "x"
			}
		},
		"stream_mode": ["values", "updates"],
		"multitask_strategy": "enqueue",
		"on_completion": "keep"
	}`)

	var want map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("json.Unmarshal(raw): %v", err)
	}

	p := payloadDictToProto(want)
	got := payloadProtoToDict(p)

	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal(want): %v", err)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal(got): %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("round trip mismatch:\nwant=%s\ngot= %s", wantJSON, gotJSON)
	}
}

// TestPayloadProtoToDict_LegacyTypedFields covers the defensive read-side
// support for typed fields that a dict-written payload never populates
// (multitask_strategy, interrupt_before/after) but a legacy or non-Go
// producer might have set directly, per crons.py:_payload_proto_to_dict.
func TestPayloadProtoToDict_LegacyTypedFields(t *testing.T) {
	ms := enummultitaskstrategy.MultitaskStrategy_enqueue
	p := &coreapi.CronPayload{
		AssistantId:       "a-1",
		MultitaskStrategy: &ms,
		InterruptBefore: &engcommon.StaticInterruptConfig{
			Config: &engcommon.StaticInterruptConfig_All{All: true},
		},
		InterruptAfter: &engcommon.StaticInterruptConfig{
			Config: &engcommon.StaticInterruptConfig_NodeNames_{
				NodeNames: &engcommon.StaticInterruptConfig_NodeNames{Names: []string{"n1", "n2"}},
			},
		},
	}

	got := payloadProtoToDict(p)
	if got["multitask_strategy"] != "enqueue" {
		t.Errorf("multitask_strategy = %v, want enqueue", got["multitask_strategy"])
	}
	if got["interrupt_before"] != "*" {
		t.Errorf("interrupt_before = %v, want *", got["interrupt_before"])
	}
	names, ok := got["interrupt_after"].([]string)
	if !ok || len(names) != 2 || names[0] != "n1" || names[1] != "n2" {
		t.Errorf("interrupt_after = %v, want [n1 n2]", got["interrupt_after"])
	}
}

// TestDecodePayload_LegacyProtojsonShape verifies pre-4a rows (protojson-shaped
// CronPayload, detectable via input_json/extra_json keys) still decode
// correctly through the legacy branch.
func TestDecodePayload_LegacyProtojsonShape(t *testing.T) {
	legacy := &coreapi.CronPayload{
		AssistantId: "a-legacy",
		InputJson:   []byte(`{"q":"hi"}`),
		ExtraJson: map[string][]byte{
			"custom": []byte(`"x"`),
		},
	}
	raw, err := jsonbutil.Marshal(legacy)
	if err != nil {
		t.Fatalf("jsonbutil.Marshal: %v", err)
	}

	got, ok := decodePayload(raw)
	if !ok {
		t.Fatalf("decodePayload: ok = false, want true")
	}
	if !proto.Equal(got, legacy) {
		t.Errorf("decodePayload legacy mismatch:\nwant=%v\ngot=%v", legacy, got)
	}
}

// TestDecodePayload_DictShape verifies a 4a-written, Python-dict-shaped row
// decodes via the new codec (not the legacy branch).
func TestDecodePayload_DictShape(t *testing.T) {
	dict := map[string]any{
		"assistant_id":  "a-dict",
		"input":         map[string]any{"q": "hi"},
		"on_completion": "keep",
	}
	raw, err := json.Marshal(dict)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	got, ok := decodePayload(raw)
	if !ok {
		t.Fatalf("decodePayload: ok = false, want true")
	}
	if got.GetAssistantId() != "a-dict" {
		t.Errorf("AssistantId = %q, want a-dict", got.GetAssistantId())
	}
	if string(got.GetInputJson()) != `{"q":"hi"}` {
		t.Errorf("InputJson = %s, want {\"q\":\"hi\"}", got.GetInputJson())
	}
	if string(got.GetExtraJson()["on_completion"]) != `"keep"` {
		t.Errorf("ExtraJson[on_completion] = %s, want \"keep\"", got.GetExtraJson()["on_completion"])
	}
}
