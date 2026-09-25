// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import "net/http"

// The refusal policy for the gateway-to-authority hop is the "Refusal policy"
// table of ori-specs evidence-transport/v1.md, exercised by
// evidence-transport/vectors/refusal-policy.json. Nothing about this hop is
// taken from gateway-api/v1, whose refusal and retirement semantics govern the
// runtime-gateway return path only.

// unrecognisedRefusalReason is recorded for any reason the refusal's status
// does not admit. It is the only value a far-end reason outside a status's
// closed list can become.
const unrecognisedRefusalReason = "unrecognised"

// pendingRegistrationConflict is the one 409 reason that does not hold: the
// authority retains a different registration pending under the same
// commissioning digest, and the refusal lasts only while that one is pending.
const pendingRegistrationConflict = "pending_registration_conflict"

// byteRefusalReasons are the 422 reasons that hold: identical bytes cannot
// repair them.
// 422 malformed is invalid authority output — malformed bytes are a 400 — and
// holds as unrecognised.
var byteRefusalReasons = map[string]bool{
	"bad_authenticator":             true,
	"binding_mismatch":              true,
	"commissioning_digest_mismatch": true,
}

// receiverStateRefusalReasons are the 422 reasons that name receiver-held
// state: retained, and retried on the next handoff only, never on a timer.
// wrong_purpose, retired_key, unknown_sequence and non_contiguous_range are
// return-path reasons, not reasons on this hop; at 422 they hold as
// unrecognised.
var receiverStateRefusalReasons = map[string]bool{
	"unrecognised_version": true,
	"unknown_key":          true,
}

// statusRefusalReasons is each status's closed reason list. A reason is
// admitted only under the status the table gives it: 400 unknown_key is invalid
// server output, not a receiver-state refusal.
var statusRefusalReasons = map[int]map[string]bool{
	http.StatusBadRequest:          {"malformed": true},
	http.StatusUnauthorized:        {"malformed": true, "unknown_key": true, "bad_authenticator": true, "stale": true, "replay": true},
	http.StatusForbidden:           {"not_authorized": true},
	http.StatusConflict:            {"conflict": true, pendingRegistrationConflict: true},
	http.StatusUnprocessableEntity: unionReasons(byteRefusalReasons, receiverStateRefusalReasons),
	http.StatusTooManyRequests:     {"rate_limited": true, "pending_registration_limit": true},
	http.StatusServiceUnavailable:  {"unavailable": true},
}

func unionReasons(sets ...map[string]bool) map[string]bool {
	out := make(map[string]bool)
	for _, set := range sets {
		for reason := range set {
			out[reason] = true
		}
	}
	return out
}

// recordedRefusalReason is the reason the gateway records, logs and reports:
// the response's reason when its status admits it, otherwise "unrecognised".
// A reason is chosen by the far end, so an invalid pair must never look like a
// valid one, and a free-form value — a hostname, a token — never reaches a log.
// Status 0 is a channel that reports no status; it is held to the union of
// every status's list.
func recordedRefusalReason(status int, reason string) string {
	if status == 0 {
		for _, reasons := range statusRefusalReasons {
			if reasons[reason] {
				return reason
			}
		}
		return unrecognisedRefusalReason
	}
	if statusRefusalReasons[status][reason] {
		return reason
	}
	return unrecognisedRefusalReason
}

// heldReason reports whether a hold may record reason for status: a reason at
// which the status holds, or unrecognised. A handoff reason such as
// 422 unknown_key never holds. Status 0 is a channel that reports none.
func heldReason(status int, reason string) bool {
	if reason == unrecognisedRefusalReason {
		return true
	}
	if status == 0 {
		return reason != "" && recordedRefusalReason(0, reason) == reason
	}
	return statusRefusalReasons[status][reason] && refusalPolicy(status, reason) == refusalHold
}

// refusalClass is what the courier does with a well-formed refusal.
type refusalClass int

const (
	// refusalHold holds the head: never re-sent by the running process,
	// never discarded.
	refusalHold refusalClass = iota
	// refusalHandoff keeps the head and retries it on the next runtime
	// handoff, never on a timer.
	refusalHandoff
	// refusalBackoff keeps the head and retries it after a bounded
	// exponential back-off, never sooner than a Retry-After the response
	// carried. A handoff does not bring the retry forward.
	refusalBackoff
)

// refusalPolicy is the only place a refusal's status and reason become an
// action, and it is the evidence-transport/v1 refusal policy table. The
// response's retriable flag is not an input: it is never used to select a
// well-formed refusal's class.
//
//   - 400 holds, whatever the reason.
//   - 401 and 403 are handoff-only, whatever the reason.
//   - 409 pending_registration_conflict backs off; every other 409 reason,
//     including one 409 does not admit, holds.
//   - 422 is decided by reason: a receiver-state reason is handoff-only; a
//     byte reason, and any reason 422 does not admit, holds.
//   - 429 and 503 back off, whatever the reason; a 429 waits no less than any
//     parseable Retry-After, and a 429 rate_limited or
//     pending_registration_limit without one is an unrecognised outcome,
//     decided by the channel before this table.
//
// In the registration lane, every hold outcome is an archival (see
// deliveryLane.archive).
//
// A reason outside its status's list is recorded as unrecognised and takes the
// status's fail-closed action above. Any other status is an unrecognised
// outcome: the channel reports it before asking, and the worker backs it off,
// never retiring or holding it. Should one reach this function it holds, which
// fails closed.
func refusalPolicy(status int, reason string) refusalClass {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return refusalHandoff
	case http.StatusBadRequest:
		return refusalHold
	case http.StatusConflict:
		if reason == pendingRegistrationConflict {
			return refusalBackoff
		}
		return refusalHold
	case http.StatusUnprocessableEntity:
		if receiverStateRefusalReasons[reason] {
			return refusalHandoff
		}
		return refusalHold
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return refusalBackoff
	default:
		return refusalHold
	}
}
