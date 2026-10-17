// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"errors"
	"fmt"
	"slices"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"
)

// definedMembers are each family's defined members under gateway-api/v2's
// Payload Member Rule: the JSON example keys and Field table names of the
// sections that give the family's members, with the Envelope Members keys.
// They hold for every object of a payload, at any depth. The vendored
// payload-members-v2 corpus lists the same sets.
var definedMembers = map[string][]string{
	FamilyGatewayHealth: {
		"auth", "body_limit_bytes", "ciphertext", "enabled", "encrypted",
		"encryption", "loopback_only", "nonce", "provider", "provider_cidr_count",
		"ready", "request_timeout_ms", "runtime_target_configured", "scheme",
		"signature", "signed_at_ms", "sim_available", "source_cidrs_configured",
		"status", "timestamp_ms", "uptime_s", "webhook_bridge",
	},
	FamilyRuntimeHeartbeat: {
		"action_event_type", "active_triggers", "attestation_gap_count", "auth",
		"available", "chain_head_hash", "ciphertext", "degradation_reasons",
		"device_id", "encrypted", "encryption", "evidence", "gateway_seen_ms",
		"last_seen_ms", "nonce", "scheme", "signature", "signed_at_ms", "status",
	},
	FamilyReasoningRequest: {
		"action_tier_hint", "auth", "ciphertext", "context", "device_id",
		"encrypted", "encryption", "history", "nonce", "producer_time", "prompt",
		"received_at_ms", "request_id", "scheme", "sensor_type", "signature",
		"signed_at_ms", "timeout_ms", "timestamp", "trigger_name", "unit", "value",
	},
	FamilyReasoningResponse: {
		"action_tier", "auth", "ciphertext", "confidence", "device_id", "encrypted",
		"encryption", "error", "latency_ms", "model", "nonce", "proposed_action",
		"request_id", "scheme", "signature", "signed_at_ms", "text", "tokens_used",
	},
	FamilyEnrichmentRequest: {
		"auth", "ciphertext", "device_id", "encrypted", "encryption",
		"history_window", "nonce", "operator_message", "proposal_id",
		"proposed_action", "quality", "reading_value", "request_id",
		"safe_default_action", "scheme", "sensor_id", "sensor_type", "signature",
		"signed_at_ms", "skill_name", "timeout_ms", "timestamp_ms", "trigger_name",
		"unit", "value",
	},
	FamilyEnrichmentResponse: {
		"auth", "ciphertext", "device_id", "encrypted", "encryption", "error",
		"estimated_impact", "explanation", "latency_ms", "model", "nonce",
		"proposal_id", "provider", "recommended_operator_context", "request_id",
		"scheme", "signature", "signed_at_ms", "tokens_used",
	},
	FamilyExportRequest: {
		"action_name", "action_taken", "action_tier", "auth", "avg_value",
		"bucket_ms", "ciphertext", "contributed_to", "correlation_id", "device_id",
		"encrypted", "encryption", "end_ms", "executed", "export_type", "limit",
		"max_value", "min_value", "nonce", "page_token", "params", "producer_time",
		"quality", "reasoning_status", "received_at_ms", "record_key", "record_kind",
		"request_id", "sample_count", "scheme", "sensor_id", "sensor_type",
		"signature", "signed_at_ms", "since_ms", "start_ms", "tier", "tier_used",
		"timestamp", "unit", "until_ms", "value",
	},
	FamilyExportResponse: {
		"action_name", "action_taken", "action_tier", "auth", "avg_value",
		"bucket_ms", "ciphertext", "complete", "contributed_to", "correlation_id",
		"device_id", "encrypted", "encryption", "end_ms", "error", "executed",
		"export_type", "items", "max_value", "min_value", "next_page_token", "nonce",
		"producer_time", "quality", "reasoning_status", "received_at_ms",
		"record_key", "record_kind", "request_id", "sample_count", "scheme",
		"sensor_id", "sensor_type", "signature", "signed_at_ms", "start_ms", "tier",
		"tier_used", "timestamp", "unit", "value",
	},
	FamilyEvidenceOutbound: {
		"artifact_b64", "artifact_type", "auth", "ciphertext", "device_id",
		"encrypted", "encryption", "nonce", "scheme", "signature", "signed_at_ms",
	},
	FamilyEvidenceOutboundAck: {
		"acknowledged_at_ms", "artifact_digest", "artifact_type", "auth",
		"ciphertext", "device_id", "encrypted", "encryption", "nonce", "outcome",
		"reason", "scheme", "signature", "signed_at_ms",
	},
	FamilyEvidenceInbound: {
		"artifact", "artifact_type", "auth", "ciphertext", "custody_at_ms",
		"device_id", "encrypted", "encryption", "envelope_digest", "key_id",
		"local_seq", "mac", "nonce", "scheme", "signature", "signed_at_ms", "v",
	},
	FamilyEvidenceInboundAck: {
		"acknowledged_at_ms", "artifact_digest", "artifact_type", "auth",
		"ciphertext", "device_id", "encrypted", "encryption", "nonce", "outcome",
		"reason", "scheme", "signature", "signed_at_ms",
	},
}

// DefinedMembers returns a copy of family's defined members, or nil for a
// topic outside the table.
func DefinedMembers(family string) []string {
	return slices.Clone(definedMembers[family])
}

// Member rule refusal reasons, in the contract's order of precedence.
const (
	ReasonInvalidUnicode    = "invalid_unicode"
	ReasonMalformedJSON     = "malformed_json"
	ReasonNestingTooDeep    = "nesting_too_deep"
	ReasonTrailingData      = "trailing_data"
	ReasonDuplicateMember   = "duplicate_member"
	ReasonCaseVariantMember = "case_variant_member"
	ReasonNotAnObject       = "not_an_object"
)

var memberRuleReasons = []struct {
	err    error
	reason string
}{
	{canonicaljson.ErrInvalidUnicode, ReasonInvalidUnicode},
	{canonicaljson.ErrMalformedJSON, ReasonMalformedJSON},
	{canonicaljson.ErrNestingTooDeep, ReasonNestingTooDeep},
	{canonicaljson.ErrTrailingData, ReasonTrailingData},
	{canonicaljson.ErrDuplicateMember, ReasonDuplicateMember},
	{canonicaljson.ErrCaseVariantMember, ReasonCaseVariantMember},
	{canonicaljson.ErrNotAnObject, ReasonNotAnObject},
}

// MemberRuleError is a payload the Payload Member Rule refuses. Its message is
// the reason alone: nothing of the payload, which no reader may rely on.
type MemberRuleError struct {
	Reason string
}

func (e *MemberRuleError) Error() string { return "payload refused: " + e.Reason }

// AdmitPayload applies gateway-api/v2's Payload Member Rule to a payload of
// family, before any member of it is read, and returns a *MemberRuleError
// naming the reason it is refused. An admitted payload has one reading: a
// struct decode of it matches only members spelled exactly, as long as the
// struct names only defined members.
func AdmitPayload(family string, payload []byte) error {
	members, ok := definedMembers[family]
	if !ok {
		return fmt.Errorf("contracts: %q is not a payload family", family)
	}
	return admit(members, payload)
}

// AdmitPayloadWithMembers applies the rule with an explicit member set; nil
// defines none.
func AdmitPayloadWithMembers(members []string, payload []byte) error {
	return admit(members, payload)
}

func admit(members []string, payload []byte) error {
	_, err := canonicaljson.DecodeStrict(payload, &canonicaljson.Shape{Members: members})
	if err == nil {
		return nil
	}
	for _, r := range memberRuleReasons {
		if errors.Is(err, r.err) {
			return &MemberRuleError{Reason: r.reason}
		}
	}
	return &MemberRuleError{Reason: ReasonMalformedJSON}
}
