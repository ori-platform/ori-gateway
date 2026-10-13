// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Runtime node heartbeat `degradation_reasons`, per gateway-api/v2.
//
// The field names which runtime subsystems are degraded, so a consumer can
// tell *why* a node reports `degraded` or `critical` rather than only that it
// does. It is deliberately not `active_triggers`: an active trigger is a
// physical or automation trigger and a degraded subsystem is neither, and the
// two differ in disclosure — trigger names are site-authored and arbitrary,
// which is why only their count is projected, whereas these tokens are
// contract-owned and non-sensitive and may be projected in full.

// maxDegradationReasons is the exact bound from the contract, not a guess.
const maxDegradationReasons = 32

// healthSnapshotInvalid is the fallback heartbeat's token: it is only ever the
// sole token, and only on a critical heartbeat.
const healthSnapshotInvalid = "health_snapshot_invalid"

// degradationReasonSeverity is the closed vocabulary with each token's
// severity. An unrecognised token is refused rather than passed through,
// which is what makes a vocabulary addition order-dependent: ratify in specs,
// then gateway acceptance, then runtime emission. A runtime emitting ahead of
// its gateways would make a conforming older gateway reject the whole
// heartbeat. The umbrella firmware_liveness_degraded decides no severity.
var degradationReasonSeverity = map[string]int{
	"action_log_records_lost":              severityDegraded,
	"action_log_records_stalled":           severityDegraded,
	"alert_delivery_overdue":               severityDegraded,
	"commissioning_problem":                severityDegraded,
	"community_skills_refused":             severityDegraded,
	"config_unsigned_value_source":         severityDegraded,
	"decision_record_lost":                 severityCritical,
	"evidence_contributor_link_unresolved": severityDegraded,
	"evidence_courier_answer_unrecognised": severityDegraded,
	"evidence_posture_problem":             severityDegraded,
	"firmware_liveness_degraded":           severityHealthy,
	"firmware_liveness_device_expired":     severityCritical,
	"firmware_liveness_over_capacity":      severityDegraded,
	"firmware_liveness_stopped":            severityCritical,
	"firmware_liveness_tick_failures":      severityDegraded,
	"firmware_liveness_tick_stale":         severityCritical,
	healthSnapshotInvalid:                  severityCritical,
	"safety_zone_activation_refused":       severityDegraded,
	"safety_zone_active_unprotected":       severityDegraded,
	"safety_zone_trip_unresolved":          severityCritical,
	"sensor_measurement_degraded":          severityDegraded,
	"sensor_stale":                         severityDegraded,
	"sensor_unconnected":                   severityDegraded,
	"skill_optional_hooks_unavailable":     severityDegraded,
	"skill_required_hooks_unavailable":     severityCritical,
	"subsystem_starting":                   severityDegraded,
	"subsystem_state_unknown":              severityDegraded,
	"subsystem_state_unknown_critical":     severityCritical,
	"tier_c_records_degraded":              severityDegraded,
	"tier_c_recovery_failed":               severityDegraded,
	"tier_d_alert_delivery_unconfirmed":    severityCritical,
}

// Closed classification verdicts. These are internal: the runtime node
// heartbeat is a one-way MQTT publication with no rejection envelope, so
// nothing is answered on the wire. They exist so a refusal is nameable in
// logs, metrics and conformance tests — which is what makes "refused for the
// right reason" checkable rather than an untestable aspiration.
const (
	VerdictRuntimeStatusInvalid             = "runtime_status_invalid"
	VerdictDegradationReasonsNotArray       = "degradation_reasons_not_array"
	VerdictDegradationReasonsLengthInvalid  = "degradation_reasons_length_invalid"
	VerdictDegradationReasonNotString       = "degradation_reason_not_string"
	VerdictDegradationReasonsNotUnique      = "degradation_reasons_not_unique"
	VerdictDegradationReasonsNotOrdered     = "degradation_reasons_not_ordered"
	VerdictDegradationReasonUnknown         = "degradation_reason_unknown"
	VerdictDegradationReasonNotAlone        = "degradation_reason_not_alone"
	VerdictDegradationReasonsStatusMismatch = "degradation_reasons_status_mismatch"
	VerdictDegradationSeverityUnderstated   = "degradation_reasons_severity_understated"
)

// DegradationError carries the closed verdict alongside a human message.
// Callers match on Verdict rather than on error text: an error-string match
// silently starts passing when the wording changes, which is precisely the
// kind of test that ratifies a defect instead of catching it.
type DegradationError struct {
	Verdict string
	Detail  string
}

func (e *DegradationError) Error() string {
	field := "degradation_reasons"
	if e.Verdict == VerdictRuntimeStatusInvalid {
		field = "status"
	}
	return fmt.Sprintf("runtime heartbeat %s: %s (%s)", field, e.Detail, e.Verdict)
}

// DegradationVerdict returns the closed verdict for err, or "" if err is not a
// degradation classification error.
func DegradationVerdict(err error) string {
	if err == nil {
		return ""
	}
	if de, ok := errors.AsType[*DegradationError](err); ok {
		return de.Verdict
	}
	return ""
}

func newDegradationError(verdict, detail string) *DegradationError {
	return &DegradationError{Verdict: verdict, Detail: detail}
}

// validateDegradationReasons applies steps 2 to 11 of the normative order from
// gateway-api/v2 to the RAW value, before typed decoding or normalisation.
// status must already have passed step 1 (validateRuntimeStatus).
//
// Operating on json.RawMessage is not incidental. A typed []string decode
// would reject a non-array or a non-string element with a generic unmarshal
// error, making two of the contract's verdicts unreachable, and would collapse
// an absent field and a present empty array into the same nil-versus-empty
// ambiguity that the contract requires be preserved.
//
// Returns the decoded tokens on success. A nil slice means the field was
// absent, which is valid and distinct from an empty array.
func validateDegradationReasons(raw json.RawMessage, status string) ([]string, error) {
	// 2. presence — absent is valid and terminates validation.
	if raw == nil {
		return nil, nil
	}

	// 3. array type. An explicit null must be refused here rather than
	// masquerading as absence — and Go will not do it for you: unmarshalling
	// JSON null into a slice is a successful no-op that yields a nil slice,
	// which would otherwise be misclassified downstream as a zero-length
	// array and reported as a length failure.
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, newDegradationError(
			VerdictDegradationReasonsNotArray,
			"value is null, which is neither an array nor an omitted field",
		)
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return nil, newDegradationError(
			VerdictDegradationReasonsNotArray,
			"value is not an array",
		)
	}

	// 4. length 1..32. Checked before uniqueness, ordering and vocabulary:
	// the vocabulary holds fewer tokens than the bound, so any over-length
	// list necessarily also contains duplicates or unknown tokens. Without this precedence an
	// over-length list would be refused by a later check while no length
	// guard existed at all, and a test asserting only "refused" would pass.
	if len(elements) < 1 || len(elements) > maxDegradationReasons {
		return nil, newDegradationError(
			VerdictDegradationReasonsLengthInvalid,
			fmt.Sprintf("length %d is outside 1..%d (an empty array is malformed; omit the field instead)",
				len(elements), maxDegradationReasons),
		)
	}

	// 5. every element is a string. Decoded generically and type-asserted
	// rather than unmarshalled straight into a string: JSON null into a
	// string is a successful no-op in Go, so [null] would become "" and reach
	// vocabulary validation as an unknown token instead of being refused
	// here. Asserting the decoded Go type covers null, numbers, booleans,
	// objects and nested arrays uniformly, rather than special-casing the one
	// hole that happens to be known.
	tokens := make([]string, 0, len(elements))
	for i, element := range elements {
		var decoded any
		if err := json.Unmarshal(element, &decoded); err != nil {
			return nil, newDegradationError(
				VerdictDegradationReasonNotString,
				fmt.Sprintf("element %d is not valid JSON", i),
			)
		}
		token, ok := decoded.(string)
		if !ok {
			return nil, newDegradationError(
				VerdictDegradationReasonNotString,
				fmt.Sprintf("element %d is %s, not a string", i, jsonTypeName(decoded)),
			)
		}
		tokens = append(tokens, token)
	}

	// 6. unique. Separate from ordering because they are different defects;
	// one shared verdict would leave a conformance failure ambiguous about
	// which rule an implementation is missing.
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if _, dup := seen[token]; dup {
			return nil, newDegradationError(
				VerdictDegradationReasonsNotUnique,
				fmt.Sprintf("token %q appears more than once", token),
			)
		}
		seen[token] = struct{}{}
	}

	// 7. ordered by the bytes of their UTF-8 encoding.
	for i := 1; i < len(tokens); i++ {
		if tokens[i-1] > tokens[i] {
			return nil, newDegradationError(
				VerdictDegradationReasonsNotOrdered,
				fmt.Sprintf("token %q precedes %q", tokens[i-1], tokens[i]),
			)
		}
	}

	// 8. closed vocabulary.
	for _, token := range tokens {
		if _, ok := degradationReasonSeverity[token]; !ok {
			return nil, newDegradationError(
				VerdictDegradationReasonUnknown,
				fmt.Sprintf("token %q is not in the gateway-api/v2 vocabulary", token),
			)
		}
	}

	// 9. health_snapshot_invalid is the fallback heartbeat's token: alone,
	// and only on a critical heartbeat.
	if _, present := seen[healthSnapshotInvalid]; present &&
		(len(tokens) != 1 || status != NodeStatusCritical) {
		return nil, newDegradationError(
			VerdictDegradationReasonNotAlone,
			fmt.Sprintf("%s must be the only token on a critical heartbeat (status %q, %d tokens)",
				healthSnapshotInvalid, status, len(tokens)),
		)
	}

	// 10. non-empty reasons require status degraded or critical. A receiver
	// seeing reasons with a healthy status would otherwise have to choose
	// which of two contradicting fields to believe.
	if status != NodeStatusDegraded && status != NodeStatusCritical {
		return nil, newDegradationError(
			VerdictDegradationReasonsStatusMismatch,
			fmt.Sprintf("reasons present with status %q", status),
		)
	}

	// 11. status is at least the most severe token's severity. A status above
	// its tokens is accepted.
	statusSev, _ := statusSeverity(status)
	for _, token := range tokens {
		if degradationReasonSeverity[token] > statusSev {
			return nil, newDegradationError(
				VerdictDegradationSeverityUnderstated,
				fmt.Sprintf("token %q is more severe than status %q", token, status),
			)
		}
	}

	return tokens, nil
}

// jsonTypeName names a decoded JSON value's type for a refusal message.
func jsonTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	default:
		return "not a string"
	}
}

// Node status severities, ordered so that a larger value is more severe.
const (
	severityHealthy = iota
	severityDegraded
	severityCritical
)

// statusSeverity returns the severity of a runtime node status, and false for
// a value that is not one.
func statusSeverity(status string) (int, bool) {
	switch status {
	case NodeStatusHealthy:
		return severityHealthy, true
	case NodeStatusDegraded:
		return severityDegraded, true
	case NodeStatusCritical:
		return severityCritical, true
	default:
		return 0, false
	}
}

// validateRuntimeStatus is step 1: status is the string healthy, degraded or
// critical. It runs before every reasons check, because no reason can be
// judged against a status that is not one.
func validateRuntimeStatus(raw json.RawMessage) (string, error) {
	var status string
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &status) != nil {
		return "", newDegradationError(VerdictRuntimeStatusInvalid, "status is absent or not a string")
	}
	if _, ok := statusSeverity(status); !ok {
		return "", newDegradationError(
			VerdictRuntimeStatusInvalid,
			fmt.Sprintf("status %q is not healthy, degraded or critical", status),
		)
	}
	return status, nil
}
