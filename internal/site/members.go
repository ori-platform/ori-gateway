// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// Gateway-side verdicts for a heartbeat whose member names are ambiguous. They
// are not contract verdicts: gateway-api/v2 names its members exactly, and
// encoding/json would otherwise read STATUS as status.
const (
	VerdictHeartbeatMemberCaseVariant = "runtime_heartbeat_member_case_variant"
	VerdictHeartbeatMemberDuplicate   = "runtime_heartbeat_member_duplicate"
)

// runtimeHeartbeatMembers are the runtime node heartbeat's contract members.
var runtimeHeartbeatMembers = []string{
	"device_id", "status", "last_seen_ms", "gateway_seen_ms",
	"active_triggers", "degradation_reasons", "evidence", "auth",
}

// runtimeHeartbeatEvidenceMembers are the evidence block's contract members.
var runtimeHeartbeatEvidenceMembers = []string{
	"chain_head_hash", "attestation_gap_count", "available", "action_event_type",
}

// refuseInexactHeartbeatMembers refuses, on the wire bytes, a heartbeat or
// evidence member whose name differs from a contract member only by case, and
// a contract member named twice. Other members are left to additive tolerance.
func refuseInexactHeartbeatMembers(payload []byte) error {
	return refuseInexactMembers(payload, runtimeHeartbeatMembers, "")
}

func refuseInexactMembers(raw []byte, members []string, path string) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	open, err := dec.Token()
	if err != nil {
		return fmt.Errorf("decode runtime node heartbeat: %w", err)
	}
	if open != json.Delim('{') {
		return fmt.Errorf("decode runtime node heartbeat: %sthe value is not a JSON object", path)
	}
	seen := make(map[string]struct{}, len(members))
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return fmt.Errorf("decode runtime node heartbeat: %w", err)
		}
		name, _ := keyToken.(string)
		for _, member := range members {
			if name != member && strings.EqualFold(name, member) {
				return newDegradationError(VerdictHeartbeatMemberCaseVariant,
					fmt.Sprintf("member %q differs from %q only by case", path+name, path+member))
			}
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return fmt.Errorf("decode runtime node heartbeat: %w", err)
		}
		if !slices.Contains(members, name) {
			continue
		}
		if _, dup := seen[name]; dup {
			return newDegradationError(VerdictHeartbeatMemberDuplicate,
				fmt.Sprintf("member %q is named more than once", path+name))
		}
		seen[name] = struct{}{}
		if path == "" && name == "evidence" && bytes.HasPrefix(bytes.TrimSpace(value), []byte("{")) {
			if err := refuseInexactMembers(value, runtimeHeartbeatEvidenceMembers, "evidence."); err != nil {
				return err
			}
		}
	}
	return nil
}

// decodeRuntimeHeartbeat reads every member the handler consumes by its exact
// key. A struct decode would match member names case-insensitively.
func decodeRuntimeHeartbeat(members map[string]json.RawMessage) (contracts.RuntimeNodeHeartbeat, error) {
	var beat contracts.RuntimeNodeHeartbeat
	// gateway_seen_ms is decoded for its type only and never read: the
	// gateway stamps its own receipt, so no producer-supplied value reaches
	// the receiver's clock.
	var gatewaySeenMS int64
	for _, m := range []struct {
		name string
		dst  any
	}{
		{"device_id", &beat.DeviceID},
		{"status", &beat.Status},
		{"last_seen_ms", &beat.LastSeenMS},
		{"gateway_seen_ms", &gatewaySeenMS},
		{"active_triggers", &beat.ActiveTriggers},
	} {
		if err := decodeExactMember(members, m.name, m.dst); err != nil {
			return beat, err
		}
	}
	raw, ok := members["evidence"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return beat, nil
	}
	var evidenceMembers map[string]json.RawMessage
	if err := json.Unmarshal(raw, &evidenceMembers); err != nil {
		return beat, fmt.Errorf("decode runtime node heartbeat evidence: %w", err)
	}
	evidence := &contracts.RuntimeNodeHeartbeatEvidence{}
	for _, m := range []struct {
		name string
		dst  any
	}{
		{"chain_head_hash", &evidence.ChainHeadHash},
		{"attestation_gap_count", &evidence.AttestationGapCount},
		{"available", &evidence.Available},
		{"action_event_type", &evidence.ActionEventType},
	} {
		if err := decodeExactMember(evidenceMembers, m.name, m.dst); err != nil {
			return beat, fmt.Errorf("evidence: %w", err)
		}
	}
	beat.Evidence = evidence
	return beat, nil
}

func decodeExactMember(members map[string]json.RawMessage, name string, dst any) error {
	raw, ok := members[name]
	if !ok {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode runtime node heartbeat %s: %w", name, err)
	}
	return nil
}
