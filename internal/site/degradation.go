// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Runtime node heartbeat `degradation_reasons`, per gateway-api/v1.
//
// The field names which runtime subsystems are degraded, so a consumer can
// tell *why* a node reports `degraded` rather than only that it does. It is
// deliberately not `active_triggers`: an active trigger is a physical or
// automation trigger and a degraded subsystem is neither, and the two differ
// in disclosure — trigger names are site-authored and arbitrary, which is why
// only their count is projected, whereas these tokens are contract-owned and
// non-sensitive and may be projected in full.

// maxDegradationReasons is the exact bound from the contract, not a guess.
const maxDegradationReasons = 16

// degradationReasonVocabulary is closed. An unrecognised token is refused
// rather than passed through, which is what makes a vocabulary addition
// order-dependent: ratify in specs, then gateway acceptance, then runtime
// emission. A runtime emitting ahead of its gateways would make a conforming
// older gateway reject the whole heartbeat.
var degradationReasonVocabulary = map[string]struct{}{
	"firmware_liveness_degraded": {},
}

// Closed classification verdicts. These are internal: the runtime node
// heartbeat is a one-way MQTT publication with no rejection envelope, so
// nothing is answered on the wire. They exist so a refusal is nameable in
// logs, metrics and conformance tests — which is what makes "refused for the
// right reason" checkable rather than an untestable aspiration.
const (
	VerdictDegradationReasonsNotArray       = "degradation_reasons_not_array"
	VerdictDegradationReasonsLengthInvalid  = "degradation_reasons_length_invalid"
	VerdictDegradationReasonNotString       = "degradation_reason_not_string"
	VerdictDegradationReasonsNotUnique      = "degradation_reasons_not_unique"
	VerdictDegradationReasonsNotOrdered     = "degradation_reasons_not_ordered"
	VerdictDegradationReasonUnknown         = "degradation_reason_unknown"
	VerdictDegradationReasonsStatusMismatch = "degradation_reasons_status_mismatch"
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
	return fmt.Sprintf("runtime heartbeat degradation_reasons: %s (%s)", e.Detail, e.Verdict)
}

// DegradationVerdict returns the closed verdict for err, or "" if err is not a
// degradation classification error.
func DegradationVerdict(err error) string {
	if err == nil {
		return ""
	}
	var de *DegradationError
	if errors.As(err, &de) {
		return de.Verdict
	}
	return ""
}

func newDegradationError(verdict, detail string) *DegradationError {
	return &DegradationError{Verdict: verdict, Detail: detail}
}

// validateDegradationReasons applies the normative order from gateway-api/v1
// to the RAW value, before typed decoding or normalisation.
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
	// 1. presence — absent is valid and terminates validation.
	if raw == nil {
		return nil, nil
	}

	// 2. array type. An explicit null must be refused here rather than
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

	// 3. length 1..16. Checked before uniqueness, ordering and vocabulary:
	// the v1 vocabulary holds one token, so any over-length list necessarily
	// also contains duplicates or unknown tokens. Without this precedence an
	// over-length list would be refused by a later check while no length
	// guard existed at all, and a test asserting only "refused" would pass.
	if len(elements) < 1 || len(elements) > maxDegradationReasons {
		return nil, newDegradationError(
			VerdictDegradationReasonsLengthInvalid,
			fmt.Sprintf("length %d is outside 1..%d (an empty array is malformed; omit the field instead)",
				len(elements), maxDegradationReasons),
		)
	}

	// 4. every element is a string. Decoded generically and type-asserted
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

	// 5. unique. Separate from ordering because they are different defects;
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

	// 6. lexicographically ordered.
	for i := 1; i < len(tokens); i++ {
		if tokens[i-1] > tokens[i] {
			return nil, newDegradationError(
				VerdictDegradationReasonsNotOrdered,
				fmt.Sprintf("token %q precedes %q", tokens[i-1], tokens[i]),
			)
		}
	}

	// 7. closed vocabulary.
	for _, token := range tokens {
		if _, ok := degradationReasonVocabulary[token]; !ok {
			return nil, newDegradationError(
				VerdictDegradationReasonUnknown,
				fmt.Sprintf("token %q is not in the v1 vocabulary", token),
			)
		}
	}

	// 8. non-empty reasons require status degraded. A receiver seeing reasons
	// with a healthy status would otherwise have to choose which of two
	// contradicting fields to believe, and implementations would choose
	// differently.
	if status != NodeStatusDegraded {
		return nil, newDegradationError(
			VerdictDegradationReasonsStatusMismatch,
			fmt.Sprintf("reasons present with status %q", status),
		)
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

// rawStatusString returns the heartbeat status when it is a JSON string, and
// "" otherwise. A malformed status is not "degraded", so reasons carried
// alongside one are refused by the status-implication check; the typed decode
// rejects the malformed status separately on its own terms.
func rawStatusString(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var status string
	if err := json.Unmarshal(raw, &status); err != nil {
		return ""
	}
	return status
}
