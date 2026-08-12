package crons

import (
	"encoding/json"
	"testing"

	enummultitaskstrategy "github.com/duongnghia222/langsmith-deployment-go/gen/enum_multitask_strategy"
)

func TestInjectOnCompletion(t *testing.T) {
	cases := []struct {
		name           string
		payload        string
		onRunCompleted string
		wantOnCompl    string // expected value of on_completion key; "" = key absent
		wantUnchanged  bool   // payload must be returned byte-identical
	}{
		{
			name:           "keep injects on_completion",
			payload:        `{"input":{"x":1}}`,
			onRunCompleted: "keep",
			wantOnCompl:    "keep",
		},
		{
			name:           "setdefault: caller value wins",
			payload:        `{"on_completion":"delete"}`,
			onRunCompleted: "keep",
			wantOnCompl:    "delete",
		},
		{
			name:           "non-keep leaves payload unchanged",
			payload:        `{"input":{"x":1}}`,
			onRunCompleted: "delete",
			wantUnchanged:  true,
		},
		{
			name:           "empty on_run_completed leaves payload unchanged",
			payload:        `{"input":{"x":1}}`,
			onRunCompleted: "",
			wantUnchanged:  true,
		},
		{
			name:           "invalid JSON returned as-is",
			payload:        `not-json`,
			onRunCompleted: "keep",
			wantUnchanged:  true,
		},
		{
			name:           "empty object gets on_completion",
			payload:        `{}`,
			onRunCompleted: "keep",
			wantOnCompl:    "keep",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := injectOnCompletion([]byte(tc.payload), tc.onRunCompleted)
			if tc.wantUnchanged {
				if string(got) != tc.payload {
					t.Fatalf("payload changed: got %s, want %s", got, tc.payload)
				}
				return
			}
			var m map[string]json.RawMessage
			if err := json.Unmarshal(got, &m); err != nil {
				t.Fatalf("result not valid JSON: %v", err)
			}
			var onCompl string
			if raw, ok := m["on_completion"]; ok {
				if err := json.Unmarshal(raw, &onCompl); err != nil {
					t.Fatalf("on_completion not a string: %v", err)
				}
			}
			if onCompl != tc.wantOnCompl {
				t.Fatalf("on_completion = %q, want %q", onCompl, tc.wantOnCompl)
			}
		})
	}
}

// TestMultitaskStrategyFromPayload verifies the payload's multitask_strategy
// key is read when present, and "enqueue" is the default (api/models/run.py:185).
func TestMultitaskStrategyFromPayload(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    enummultitaskstrategy.MultitaskStrategy
	}{
		{"absent defaults to enqueue", `{"input":{}}`, enummultitaskstrategy.MultitaskStrategy_enqueue},
		{"explicit reject", `{"multitask_strategy":"reject"}`, enummultitaskstrategy.MultitaskStrategy_reject},
		{"explicit rollback", `{"multitask_strategy":"rollback"}`, enummultitaskstrategy.MultitaskStrategy_rollback},
		{"unrecognised falls back to enqueue", `{"multitask_strategy":"bogus"}`, enummultitaskstrategy.MultitaskStrategy_enqueue},
		{"invalid JSON falls back to enqueue", `not-json`, enummultitaskstrategy.MultitaskStrategy_enqueue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := multitaskStrategyFromPayload([]byte(tc.payload))
			if got != tc.want {
				t.Errorf("multitaskStrategyFromPayload(%s) = %v, want %v", tc.payload, got, tc.want)
			}
		})
	}
}

// TestMergeCronMetadata verifies cron_scheduler.py:32-49's
// {**cron_metadata, **existing_metadata} merge: payload's own "metadata" key
// wins over the cron row's metadata on conflicting keys.
func TestMergeCronMetadata(t *testing.T) {
	cronMeta := []byte(`{"a":1,"b":2}`)
	payload := []byte(`{"input":{},"metadata":{"b":20,"c":3}}`)

	got := mergeCronMetadata(cronMeta, payload)

	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("result not valid JSON: %v", err)
	}
	want := map[string]any{"a": 1.0, "b": 20.0, "c": 3.0}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("merged[%q] = %v, want %v", k, m[k], v)
		}
	}
	if len(m) != len(want) {
		t.Errorf("merged = %v, want exactly %v", m, want)
	}
}

// TestMergeCronMetadata_NoPayloadMetadata verifies the cron's own metadata is
// preserved unchanged when the payload carries no "metadata" key.
func TestMergeCronMetadata_NoPayloadMetadata(t *testing.T) {
	cronMeta := []byte(`{"a":1}`)
	payload := []byte(`{"input":{}}`)

	got := mergeCronMetadata(cronMeta, payload)

	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("result not valid JSON: %v", err)
	}
	if m["a"] != 1.0 || len(m) != 1 {
		t.Errorf("merged = %v, want {a:1}", m)
	}
}
