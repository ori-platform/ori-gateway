// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

// signedAckWith signs, with the runtime's secret, the rig's applied decision
// on its head after mutate has changed its members, so only the shape is
// wrong.
func (r *returnRig) signedAckWith(mutate func(map[string]any)) []byte {
	r.t.Helper()
	var members map[string]any
	if err := json.Unmarshal(r.decision(rigDevice, "applied", "", nil), &members); err != nil {
		r.t.Fatal(err)
	}
	delete(members, "auth")
	mutate(members)
	auth, err := mqttauth.Sign(members, contracts.EvidenceInboundAckMessageType, rigDevice, "", r.now().UnixMilli(), rigSecret)
	if err != nil {
		r.t.Fatal(err)
	}
	members["auth"] = auth
	payload, err := json.Marshal(members)
	if err != nil {
		r.t.Fatal(err)
	}
	return payload
}

// TestAnIllTypedAcknowledgementIsNoDecision sends correctly signed applied
// decisions on the queued head with each member absent, null or of another
// JSON type. Each is refused as malformed: the head neither retires nor is
// held, and is carried again on the retry interval as if unacknowledged.
func TestAnIllTypedAcknowledgementIsNoDecision(t *testing.T) {
	values := map[string]any{
		"null": nil, "number": 42, "object": map[string]any{}, "array": []any{}, "boolean": false, "string": "42",
	}
	type shape struct {
		member, form string
	}
	var shapes []shape
	// The acknowledgement's members as gateway-evidence-carriage/v1 lists
	// them, independently of the list the handler checks.
	members := []string{"device_id", "artifact_type", "artifact_digest", "outcome", "reason", "acknowledged_at_ms"}
	if want := members[:5]; !slices.Equal(inboundAckStrings, want) {
		t.Fatalf("the handler requires strings %v, want %v", inboundAckStrings, want)
	}
	for _, member := range members {
		shapes = append(shapes, shape{member, "absent"})
		for form := range values {
			isString := member != "acknowledged_at_ms"
			if (isString && form == "string") || (!isString && form == "number") {
				continue
			}
			shapes = append(shapes, shape{member, form})
		}
	}
	for _, s := range shapes {
		t.Run(s.member+"/"+s.form, func(t *testing.T) {
			t.Parallel()
			r := newReturnRig(t)
			payload := r.signedAckWith(func(m map[string]any) {
				if s.form == "absent" {
					delete(m, s.member)
				} else {
					m[s.member] = values[s.form]
				}
			})
			err := r.handle(rigTopic, payload)
			if err == nil || errors.Is(err, ErrUnrecognisedInboundReason) {
				t.Fatalf("HandleAck = %v, want the acknowledgement refused", err)
			}
			r.headKept()
			if r.heldID() != "" {
				t.Fatal("an ill-typed acknowledgement held the head")
			}
			r.carriedAgain(r.interval(), r.artifact)
		})
	}
}

// TestAnAcknowledgementWithANullReasonSpelledWithSpacesIsNoDecision sends the
// null reason with whitespace around it, which the signature does not cover.
func TestAnAcknowledgementWithANullReasonSpelledWithSpacesIsNoDecision(t *testing.T) {
	r := newReturnRig(t)
	payload := r.signedAckWith(func(m map[string]any) { m["reason"] = nil })
	spaced := bytes.Replace(payload, []byte(`"reason":null`), []byte("\"reason\" :\n\t null "), 1)
	if bytes.Equal(spaced, payload) {
		t.Fatal("the payload has no null reason")
	}
	if err := r.handle(rigTopic, spaced); err == nil {
		t.Fatal("a null reason spelled with spaces was accepted")
	}
	r.headKept()
	r.carriedAgain(r.interval(), r.artifact)
}

// TestAWellTypedAppliedDecisionStillRetires is the control: the same signed
// construction with every member well typed retires the head.
func TestAWellTypedAppliedDecisionStillRetires(t *testing.T) {
	r := newReturnRig(t)
	if err := r.handle(rigTopic, r.signedAckWith(func(map[string]any) {})); err != nil {
		t.Fatal(err)
	}
	if r.sink.Len() != 0 {
		t.Fatal("a well-typed applied decision did not retire the head")
	}
}
