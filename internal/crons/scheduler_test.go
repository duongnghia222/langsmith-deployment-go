package crons

import (
	"encoding/json"
	"testing"
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
