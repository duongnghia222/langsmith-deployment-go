package assistants

import (
	"testing"

	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	"github.com/duongnghia222/langsmith-deployment-go/internal/crons"
	"github.com/duongnghia222/langsmith-deployment-go/internal/jsonbutil"
	"google.golang.org/protobuf/proto"
)

// TestDecodeConfig_LegacyProtojsonShape_ExtraConfigurableOnly verifies fix
// round 1 finding 2: a pre-5f row holding an ordinary configurable dict
// (e.g. {"configurable": {"model": "gpt-4"}}) protojson-marshals with ONLY
// extra_configurable_json populated — no metadata_json, no extra_json — so
// the old isLegacyConfigShape (which only checked those two keys) misread
// it as dict-shaped and surfaced "extra_configurable_json" as a bogus
// top-level client key instead of nesting it under "configurable".
//
// Parallel to internal/crons/payload_test.go's TestDecodePayload_LegacyProtojsonShape.
func TestDecodeConfig_LegacyProtojsonShape_ExtraConfigurableOnly(t *testing.T) {
	legacy := &engcommon.EngineRunnableConfig{
		ExtraConfigurableJson: map[string][]byte{"model": []byte(`"gpt-4"`)},
	}
	raw, err := jsonbutil.Marshal(legacy)
	if err != nil {
		t.Fatalf("jsonbutil.Marshal: %v", err)
	}

	cfg, ok := decodeConfig(raw)
	if !ok {
		t.Fatalf("decodeConfig: ok = false, want true")
	}
	if !proto.Equal(cfg, legacy) {
		t.Errorf("decodeConfig legacy mismatch:\nwant=%v\ngot=%v", legacy, cfg)
	}

	// The client-visible dict (config_from_proto's Go port) must have
	// "configurable.model", not a top-level "extra_configurable_json" key.
	dict := crons.ConfigProtoToDict(cfg)
	configurable, _ := dict["configurable"].(map[string]any)
	if configurable == nil || configurable["model"] != "gpt-4" {
		t.Errorf("dict[configurable] = %v, want map with model=gpt-4", dict["configurable"])
	}
	for _, protojsonKey := range []string{"extra_configurable_json", "metadata_json", "extra_json"} {
		if _, ok := dict[protojsonKey]; ok {
			t.Errorf("dict has bogus top-level protojson key %q: %v", protojsonKey, dict)
		}
	}
}
