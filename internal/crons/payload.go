package crons

import (
	"encoding/json"
	"fmt"

	coreapi "github.com/duongnghia222/langsmith-deployment-go/gen/core_api"
	engcommon "github.com/duongnghia222/langsmith-deployment-go/gen/engine_common"
	enumdurability "github.com/duongnghia222/langsmith-deployment-go/gen/enum_durability"
	enummultitaskstrategy "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
	enumstreammode "github.com/duongnghia222/langsmith-deployment-go/gen/enum_stream_mode"
)

// This file ports the payload/config dict<->proto codec the Python client
// (api/grpc/ops/crons.py) uses so cron.payload is stored in the same
// Python-shaped-dict JSON that storage/ops.py's raw-SQL fallbacks and the
// gRPC client both expect (4a). Cron rows written by pre-4a LSD are
// protojson-shaped instead; decodePayload in service.go detects and handles
// that legacy shape separately.

// simplePayloadKeys mirrors crons.py:_payload_dict_to_proto's simple_keys —
// the payload keys with a typed CronPayload field. Everything else round-trips
// through ExtraJson as opaque JSON, exactly like the Python reference.
var simplePayloadKeys = map[string]bool{
	"assistant_id": true,
	"input":        true,
	"context":      true,
	"metadata":     true,
	"webhook":      true,
	"config":       true,
}

// payloadDictToProto ports crons.py:_payload_dict_to_proto (lines 64-94).
func payloadDictToProto(payload map[string]any) *coreapi.CronPayload {
	p := &coreapi.CronPayload{}
	if v, ok := payload["assistant_id"]; ok && v != nil {
		p.AssistantId = fmt.Sprint(v)
	}
	if v, ok := payload["input"]; ok && v != nil {
		if b, err := json.Marshal(v); err == nil {
			p.InputJson = b
		}
	}
	if v, ok := payload["context"]; ok && v != nil {
		if b, err := json.Marshal(v); err == nil {
			p.ContextJson = b
		}
	}
	if v, ok := payload["metadata"]; ok && v != nil {
		if b, err := json.Marshal(v); err == nil {
			p.MetadataJson = b
		}
	}
	if v, ok := payload["webhook"]; ok && v != nil {
		s := fmt.Sprint(v)
		p.Webhook = &s
	}
	if v, ok := payload["config"]; ok && v != nil {
		if cfgMap, ok := v.(map[string]any); ok {
			if pc := ConfigDictToProto(cfgMap); pc != nil {
				p.Config = pc
			}
		}
	}

	for key, val := range payload {
		if simplePayloadKeys[key] || val == nil {
			continue
		}
		if b, err := json.Marshal(val); err == nil {
			if p.ExtraJson == nil {
				p.ExtraJson = map[string][]byte{}
			}
			p.ExtraJson[key] = b
		}
	}

	return p
}

// payloadProtoToDict ports crons.py:_payload_proto_to_dict (lines 97-126).
// extra_json is applied last and wins on key collision, matching Python.
func payloadProtoToDict(p *coreapi.CronPayload) map[string]any {
	result := map[string]any{}
	if p == nil {
		return result
	}
	if p.AssistantId != "" {
		result["assistant_id"] = p.AssistantId
	}
	if len(p.InputJson) > 0 {
		var v any
		if json.Unmarshal(p.InputJson, &v) == nil {
			result["input"] = v
		}
	}
	if len(p.ContextJson) > 0 {
		var v any
		if json.Unmarshal(p.ContextJson, &v) == nil {
			result["context"] = v
		}
	}
	if len(p.MetadataJson) > 0 {
		var v any
		if json.Unmarshal(p.MetadataJson, &v) == nil {
			result["metadata"] = v
		}
	}
	if p.Webhook != nil {
		result["webhook"] = *p.Webhook
	}
	if p.Config != nil {
		result["config"] = ConfigProtoToDict(p.Config)
	}
	if p.InterruptBefore != nil {
		if v, ok := staticInterruptConfigFromProto(p.InterruptBefore); ok {
			result["interrupt_before"] = v
		}
	}
	if p.InterruptAfter != nil {
		if v, ok := staticInterruptConfigFromProto(p.InterruptAfter); ok {
			result["interrupt_after"] = v
		}
	}
	if p.MultitaskStrategy != nil {
		if name, ok := enummultitaskstrategy.MultitaskStrategy_name[int32(*p.MultitaskStrategy)]; ok {
			result["multitask_strategy"] = name
		}
	}
	for key, val := range p.ExtraJson {
		var v any
		if json.Unmarshal(val, &v) == nil {
			result[key] = v
		}
	}
	return result
}

// staticInterruptConfigFromProto ports
// api/grpc/ops/__init__.py:_static_interrupt_config_from_proto (lines
// 376-396). Returns "*" whenever the "all" oneof branch is set — regardless
// of the bool's value, matching Python's `which == "all": return "*"` — a
// node-name list when "node_names" is set, or (nil, false) otherwise.
func staticInterruptConfigFromProto(c *engcommon.StaticInterruptConfig) (any, bool) {
	if c == nil {
		return nil, false
	}
	switch c.GetConfig().(type) {
	case *engcommon.StaticInterruptConfig_All:
		return "*", true
	case *engcommon.StaticInterruptConfig_NodeNames_:
		return append([]string{}, c.GetNodeNames().GetNames()...), true
	default:
		return nil, false
	}
}

// restrictedReservedConfigurableKeys mirrors
// config.py:RESTRICTED_RESERVED_CONFIGURABLE_KEYS — LangGraph execution-only
// configurable keys carrying live runtime objects (callables, checkpointer
// handles). They're silently dropped rather than round-tripped through
// extra_configurable_json, matching config_to_proto's behavior.
var restrictedReservedConfigurableKeys = map[string]bool{
	"__pregel_send":              true,
	"__pregel_read":              true,
	"__pregel_scratchpad":        true,
	"__pregel_call":              true,
	"__pregel_checkpointer":      true,
	"__pregel_stream":            true,
	"__pregel_cache":             true,
	"__pregel_runner_submit":     true,
	"__pregel_root_stream_modes": true,
}

// knownConfigTopLevelKeys mirrors config.py:KNOWN_CONFIG_KEYS.
var knownConfigTopLevelKeys = map[string]bool{
	"metadata": true, "run_name": true, "run_id": true, "max_concurrency": true,
	"recursion_limit": true, "tags": true, "configurable": true, "callbacks": true,
}

// ConfigDictToProto is a scoped port of
// api/langgraph_grpc_common/conversion/config.py:config_to_proto, covering
// the fields a stored config template realistically carries: metadata (with
// run_attempt/run_id specials), tags, run_name, run_id (top-level or
// configurable, matching Python's `or`-fallback), max_concurrency,
// recursion_limit, a handful of special configurable keys
// (resuming/task_id/thread_id/checkpoint_*/durability/root_stream_modes/
// tracing_*), and generic extra_json/extra_configurable_json passthrough for
// everything else.
//
// Exported for reuse by internal/assistants (assistant.config is stored in
// the same Python-shaped-dict JSON as cron.payload's config field — see
// jsonbutil's package doc).
//
// ponytail: `configurable.__pregel_runtime` (LangGraph Runtime/context) and
// `configurable.__pregel_resume_map` (SerializedValue wire format) are not
// ported — they carry live execution-time objects a persisted config
// template would not realistically contain. Add a real port (see
// config.py:runtime_to_proto / conversion/value.py) if a cron payload ever
// needs one.
//
// Known asymmetry (matches config.py, not a bug): EngineRunnableConfig.graph_id
// round-trips proto→dict (into configurable["graph_id"], config.py:115-116)
// but NOT dict→proto — config.py's _inject_configurable_into_proto has no
// "graph_id" case, so a re-ingested dict's configurable.graph_id falls
// through to extra_configurable_json instead of GraphId. Ported verbatim.
// RunId has the identical asymmetry: config.py:163-164 copies it into
// configurable["run_id"] on the way out, but _inject_configurable_into_proto
// has no "run_id" case either (config.py:267-333), so re-ingesting a dict
// that carries it sets RunId AND ALSO adds extra_configurable_json["run_id"]
// — a second copy the original proto never had.
func ConfigDictToProto(cfg map[string]any) *engcommon.EngineRunnableConfig {
	if len(cfg) == 0 {
		return nil
	}
	pb := &engcommon.EngineRunnableConfig{}

	if metaRaw, ok := cfg["metadata"].(map[string]any); ok {
		for k, v := range metaRaw {
			switch k {
			case "run_attempt":
				if n, ok := toInt32(v); ok {
					pb.RunAttempt = &n
				}
			case "run_id":
				s := fmt.Sprint(v)
				pb.ServerRunId = &s
			default:
				if b, err := json.Marshal(v); err == nil {
					if pb.MetadataJson == nil {
						pb.MetadataJson = map[string][]byte{}
					}
					pb.MetadataJson[k] = b
				}
			}
		}
	}

	if s, ok := cfg["run_name"].(string); ok && s != "" {
		pb.RunName = &s
	}

	runID := ""
	if v, ok := cfg["run_id"]; ok && v != nil {
		runID = fmt.Sprint(v)
	}
	if runID == "" {
		if configurable, ok := cfg["configurable"].(map[string]any); ok {
			if v, ok := configurable["run_id"]; ok && v != nil {
				runID = fmt.Sprint(v)
			}
		}
	}
	if runID != "" {
		pb.RunId = &runID
	}

	if n, ok := toInt32(cfg["max_concurrency"]); ok && n != 0 {
		pb.MaxConcurrency = &n
	}
	if n, ok := toInt32(cfg["recursion_limit"]); ok && n != 0 {
		pb.RecursionLimit = &n
	}

	if tagsRaw, ok := cfg["tags"].([]any); ok {
		for _, t := range tagsRaw {
			pb.Tags = append(pb.Tags, fmt.Sprint(t))
		}
	}

	if configurable, ok := cfg["configurable"].(map[string]any); ok {
		injectConfigurableIntoProto(configurable, pb)
	}

	extra := map[string]any{}
	for k, v := range cfg {
		if !knownConfigTopLevelKeys[k] {
			extra[k] = v
		}
	}
	if len(extra) > 0 {
		pb.ExtraJson = make(map[string][]byte, len(extra))
		for k, v := range extra {
			if b, err := json.Marshal(v); err == nil {
				pb.ExtraJson[k] = b
			}
		}
	}

	return pb
}

// injectConfigurableIntoProto ports config.py:_inject_configurable_into_proto
// (lines 267-333).
func injectConfigurableIntoProto(configurable map[string]any, pb *engcommon.EngineRunnableConfig) {
	extra := map[string]any{}
	for key, value := range configurable {
		if value == nil {
			continue
		}
		switch key {
		case "__pregel_resuming":
			if b, ok := value.(bool); ok {
				pb.Resuming = &b
			}
		case "__pregel_task_id":
			s := fmt.Sprint(value)
			pb.TaskId = &s
		case "thread_id":
			s := fmt.Sprint(value)
			pb.ThreadId = &s
		case "checkpoint_map":
			if m, ok := value.(map[string]any); ok {
				cm := make(map[string]string, len(m))
				for k, v := range m {
					cm[k] = fmt.Sprint(v)
				}
				pb.CheckpointMap = cm
			}
		case "checkpoint_id":
			s := fmt.Sprint(value)
			pb.CheckpointId = &s
		case "checkpoint_ns":
			s := fmt.Sprint(value)
			pb.CheckpointNs = &s
		case "__pregel_durability":
			if s, ok := value.(string); ok {
				if d, ok := durabilityToProto(s); ok {
					pb.Durability = &d
				}
			}
		case "__pregel_root_stream_modes":
			if modes, ok := toStringSlice(value); ok {
				for _, m := range modes {
					if v, ok := enumstreammode.StreamMode_value[m]; ok {
						pb.RootStreamModes = append(pb.RootStreamModes, enumstreammode.StreamMode(v))
					}
				}
			}
		case "__langsmith_project__":
			s := fmt.Sprint(value)
			pb.TracingProject = &s
		case "__langsmith_example_id__":
			s := fmt.Sprint(value)
			pb.TracingExampleId = &s
		default:
			if !restrictedReservedConfigurableKeys[key] {
				extra[key] = value
			}
		}
	}
	if len(extra) > 0 {
		pb.ExtraConfigurableJson = make(map[string][]byte, len(extra))
		for k, v := range extra {
			if b, err := json.Marshal(v); err == nil {
				pb.ExtraConfigurableJson[k] = b
			}
		}
	}
}

// ConfigProtoToDict is the inverse of ConfigDictToProto, scoped to the same
// field set (config.py:config_from_proto, lines 41-76). Exported for reuse
// by internal/assistants (see ConfigDictToProto's doc comment).
func ConfigProtoToDict(pb *engcommon.EngineRunnableConfig) map[string]any {
	cfg := map[string]any{}
	if pb == nil {
		return cfg
	}

	configurable := configurableFromProto(pb)

	metadata := map[string]any{}
	for k, v := range pb.MetadataJson {
		var val any
		if json.Unmarshal(v, &val) == nil {
			metadata[k] = val
		}
	}
	if pb.RunAttempt != nil {
		metadata["run_attempt"] = *pb.RunAttempt
	}
	if pb.ServerRunId != nil {
		metadata["run_id"] = *pb.ServerRunId
	}

	if len(configurable) > 0 {
		cfg["configurable"] = configurable
	}
	if len(pb.Tags) > 0 {
		cfg["tags"] = append([]string{}, pb.Tags...)
	}
	if len(metadata) > 0 {
		cfg["metadata"] = metadata
	}
	if pb.RunName != nil {
		cfg["run_name"] = *pb.RunName
	}
	if pb.MaxConcurrency != nil {
		cfg["max_concurrency"] = *pb.MaxConcurrency
	}
	if pb.RecursionLimit != nil {
		cfg["recursion_limit"] = *pb.RecursionLimit
	}
	for k, v := range pb.ExtraJson {
		var val any
		if json.Unmarshal(v, &val) == nil {
			cfg[k] = val
		}
	}
	return cfg
}

// configurableFromProto ports config.py:_configurable_from_proto (lines
// 89-170), minus the runtime/resume_map fields (see ConfigDictToProto's
// ponytail note) and __pregel_stream (a non-serializable StreamProtocol
// callable in Python with no meaningful Go/storage representation).
func configurableFromProto(pb *engcommon.EngineRunnableConfig) map[string]any {
	configurable := map[string]any{}
	if pb.Resuming != nil {
		configurable["__pregel_resuming"] = *pb.Resuming
	}
	if pb.TaskId != nil {
		configurable["__pregel_task_id"] = *pb.TaskId
	}
	if pb.ThreadId != nil {
		configurable["thread_id"] = *pb.ThreadId
	}
	if pb.CheckpointId != nil && *pb.CheckpointId != "" {
		configurable["checkpoint_id"] = *pb.CheckpointId
	}
	if pb.CheckpointNs != nil {
		configurable["checkpoint_ns"] = *pb.CheckpointNs
	}
	if pb.Durability != nil {
		if s, ok := durabilityFromProto(*pb.Durability); ok {
			configurable["__pregel_durability"] = s
		}
	}
	if pb.GraphId != nil {
		configurable["graph_id"] = *pb.GraphId
	}
	if len(pb.RootStreamModes) > 0 {
		modes := make([]string, 0, len(pb.RootStreamModes))
		for _, m := range pb.RootStreamModes {
			if name, ok := enumstreammode.StreamMode_name[int32(m)]; ok {
				modes = append(modes, name)
			}
		}
		configurable["__pregel_root_stream_modes"] = modes
	}
	if len(pb.CheckpointMap) > 0 {
		cm := make(map[string]any, len(pb.CheckpointMap))
		for k, v := range pb.CheckpointMap {
			cm[k] = v
		}
		configurable["checkpoint_map"] = cm
	}
	for k, v := range pb.ExtraConfigurableJson {
		var val any
		if json.Unmarshal(v, &val) == nil {
			configurable[k] = val
		}
	}
	if pb.RunId != nil && *pb.RunId != "" {
		configurable["run_id"] = *pb.RunId
	}
	if pb.TracingProject != nil && *pb.TracingProject != "" {
		configurable["__langsmith_project__"] = *pb.TracingProject
	}
	if pb.TracingExampleId != nil && *pb.TracingExampleId != "" {
		configurable["__langsmith_example_id__"] = *pb.TracingExampleId
	}
	return configurable
}

// durabilityToProto/durabilityFromProto port
// api/langgraph_grpc_common/conversion/durability.py. A manual mapping is
// required (not enumdurability.Durability_value) because the Go enum names
// are uppercase (ASYNC/SYNC/EXIT) while Python's durability strings are
// lowercase.
func durabilityToProto(s string) (enumdurability.Durability, bool) {
	switch s {
	case "async":
		return enumdurability.Durability_ASYNC, true
	case "sync":
		return enumdurability.Durability_SYNC, true
	case "exit":
		return enumdurability.Durability_EXIT, true
	default:
		return 0, false
	}
}

func durabilityFromProto(d enumdurability.Durability) (string, bool) {
	switch d {
	case enumdurability.Durability_ASYNC:
		return "async", true
	case enumdurability.Durability_SYNC:
		return "sync", true
	case enumdurability.Durability_EXIT:
		return "exit", true
	default:
		return "", false
	}
}

// toInt32 coerces a JSON-decoded numeric value (float64 from encoding/json,
// or a Go-native int/int32/int64 from tests constructing maps directly) to
// int32.
func toInt32(v any) (int32, bool) {
	switch n := v.(type) {
	case float64:
		return int32(n), true
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	default:
		return 0, false
	}
}

func toStringSlice(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, fmt.Sprint(item))
	}
	return out, true
}

// isLegacyPayloadShape reports whether raw stored payload JSON is protojson
// CronPayload-shaped (pre-4a) rather than the Python-dict shape written by
// payloadDictToProto. input_json/extra_json are proto field names a Python
// payload dict would never carry at the top level.
func isLegacyPayloadShape(m map[string]json.RawMessage) bool {
	_, hasInput := m["input_json"]
	_, hasExtra := m["extra_json"]
	return hasInput || hasExtra
}
