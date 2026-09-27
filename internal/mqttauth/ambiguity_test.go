// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// TestVerifierRefusesAmbiguousMembers splices ambiguity into a payload whose
// authenticator is correct for the members a map decode keeps: a member named
// twice or by a case variant, at the top level, in a nested object, or in the
// auth envelope. Without the refusal each verifies, and a later struct decode
// that keeps the last duplicate or ignores case reads something its signer
// did not write.
func TestVerifierRefusesAmbiguousMembers(t *testing.T) {
	now := time.UnixMilli(1787000000000)
	type row struct {
		// signed is what a map decode of the payload keeps, and what is signed.
		signed map[string]any
		// raw renders the payload given the exact auth members.
		raw func(scheme, signedAt, signature string) string
	}
	exact := func(scheme, signedAt, signature string) string { return scheme + "," + signedAt + "," + signature }
	body := func(auth string) string {
		return `{"device_id":"dev-01","status":"healthy","meta":{"a":1},"auth":{` + auth + `}}`
	}
	plain := map[string]any{"device_id": "dev-01", "status": "healthy", "meta": map[string]any{"a": 1}}
	rows := map[string]row{
		"the exact payload verifies": {plain, func(s, a, g string) string { return body(exact(s, a, g)) }},
		"a top-level member named twice": {plain, func(s, a, g string) string {
			return strings.Replace(body(exact(s, a, g)), `"status":"healthy"`, `"status":"degraded","status":"healthy"`, 1)
		}},
		"a top-level case collision": {
			map[string]any{"device_id": "dev-01", "status": "healthy", "Status": "degraded", "meta": map[string]any{"a": 1}},
			func(s, a, g string) string {
				return strings.Replace(body(exact(s, a, g)), `"status":"healthy"`, `"status":"healthy","Status":"degraded"`, 1)
			}},
		"a nested member named twice": {plain, func(s, a, g string) string {
			return strings.Replace(body(exact(s, a, g)), `{"a":1}`, `{"a":2,"a":1}`, 1)
		}},
		"scheme named twice":       {plain, func(s, a, g string) string { return body(`"scheme":"other",` + exact(s, a, g)) }},
		"signed_at_ms named twice": {plain, func(s, a, g string) string { return body(`"signed_at_ms":1,` + exact(s, a, g)) }},
		"signature named twice":    {plain, func(s, a, g string) string { return body(`"signature":"hmac-sha256:00",` + exact(s, a, g)) }},
		"a case-variant scheme":    {plain, func(s, a, g string) string { return body(exact(strings.Replace(s, `"scheme"`, `"Scheme"`, 1), a, g)) }},
		"a case-variant signed_at_ms": {plain, func(s, a, g string) string {
			return body(exact(s, strings.Replace(a, `"signed_at_ms"`, `"Signed_At_MS"`, 1), g))
		}},
		"a case-variant signature": {plain, func(s, a, g string) string {
			return body(exact(s, a, strings.Replace(g, `"signature"`, `"SIGNATURE"`, 1)))
		}},
		"a case-variant beside it": {plain, func(s, a, g string) string { return body(`"Signature":"hmac-sha256:00",` + exact(s, a, g)) }},
	}
	for name, tc := range rows {
		t.Run(name, func(t *testing.T) {
			auth, err := Sign(tc.signed, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
			if err != nil {
				t.Fatal(err)
			}
			raw := tc.raw(fmt.Sprintf(`"scheme":%q`, auth.Scheme), fmt.Sprintf(`"signed_at_ms":%d`, auth.SignedAtMS), fmt.Sprintf(`"signature":%q`, auth.Signature))
			verifier, err := NewVerifier(Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			_, err = verifier.VerifyJSON([]byte(raw), contracts.RuntimeHeartbeatMessageType, "dev-01", "")
			if want := name == "the exact payload verifies"; want != (err == nil) {
				t.Fatalf("VerifyJSON = %v", err)
			}
		})
	}
}
