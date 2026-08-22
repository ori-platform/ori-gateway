// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestExportRequestEmitsNoExponentNotation pins the numeric forms this schema
// emits. The runtime verifies by re-serialising with CPython, and the two
// languages agree on integers and on floats of magnitude 0 or >= 1e-4, but choose
// different notation below that. exportRequest carries only strings and integers
// today, including inside Params; this test fails if a float ever arrives there,
// which is the point at which the contract follow-up applies.
func TestExportRequestEmitsNoExponentNotation(t *testing.T) {
	requests := []exportRequest{
		{
			RequestID: "req-1", ExportType: "sensor_history", DeviceID: "dev-01",
			SinceMS: 1755820000000, UntilMS: 1755830000000, Limit: 100,
			Params: map[string]any{"sensor_id": "load-current", "bucket_ms": int64(60000)},
		},
		{
			RequestID: "req-2", ExportType: "reasoning_log", DeviceID: "dev-01",
			Params: reasoningLogParams(ReasoningLogRequest{
				TierUsed: "local_slm", ActionTier: "A", ReasoningStatus: "ok", CorrelationID: "corr-1",
			}),
		},
	}
	for _, req := range requests {
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.ContainsAny(string(raw), "eE") {
			// Field names legitimately contain 'e'; only a number may not.
			var probe map[string]any
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			if err := dec.Decode(&probe); err != nil {
				t.Fatalf("decode: %v", err)
			}
			assertNoExponentNumbers(t, probe)
		}
	}
}

func assertNoExponentNumbers(t *testing.T, v any) {
	t.Helper()
	switch node := v.(type) {
	case map[string]any:
		for _, child := range node {
			assertNoExponentNumbers(t, child)
		}
	case []any:
		for _, child := range node {
			assertNoExponentNumbers(t, child)
		}
	case json.Number:
		if strings.ContainsAny(node.String(), "eE") {
			t.Errorf("number %s uses exponent notation; CPython and Go may not agree on its spelling", node)
		}
	}
}
