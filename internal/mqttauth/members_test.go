// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// TestDecryptedPlaintextIsHeldToTheMemberRule refuses an export plaintext
// carrying a case variant of a member the export response defines, before
// any member of it is read.
func TestDecryptedPlaintextIsHeldToTheMemberRule(t *testing.T) {
	encryptor, err := NewEncryptor("site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := map[string]any{
		"request_id": "req-1", "device_id": "dev-01", "export_type": "action_log",
		"items": []any{}, "COMPLETE": true,
	}
	encrypted, err := encryptor.Encrypt(plain, contracts.ExportResponseMessageType, []byte("123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	var refused *contracts.MemberRuleError
	if _, err := encryptor.Decrypt(encrypted, contracts.ExportResponseMessageType, "dev-01", "req-1"); !errors.As(err, &refused) ||
		refused.Reason != contracts.ReasonCaseVariantMember {
		t.Fatalf("Decrypt = %v, want refused as %s", err, contracts.ReasonCaseVariantMember)
	}
}

// TestVerifierFoldsNamesOnlyByTheClosedFold admits a signed payload whose
// names differ only outside the closed fold (σ and ς, ß and ẞ), which the
// member rule compares exactly, and refuses one whose names are equal under
// it.
func TestVerifierFoldsNamesOnlyByTheClosedFold(t *testing.T) {
	now := time.UnixMilli(1_787_000_000_000)
	verifier, err := NewVerifier(Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	signed := func(payload map[string]any, raw string) []byte {
		auth, err := Sign(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
		if err != nil {
			t.Fatal(err)
		}
		return []byte(`{` + raw + `,"auth":{"scheme":"` + auth.Scheme + `","signed_at_ms":` +
			strconv.FormatInt(auth.SignedAtMS, 10) + `,"signature":"` + auth.Signature + `"}}`)
	}
	exact := signed(map[string]any{"device_id": "dev-01", "σ": 1, "ς": 2, "ß": 3, "ẞ": 4},
		`"device_id":"dev-01","σ":1,"ς":2,"ß":3,"ẞ":4`)
	if _, err := verifier.VerifyJSON(exact, contracts.RuntimeHeartbeatMessageType, "dev-01", ""); err != nil {
		t.Fatalf("names the closed fold keeps apart were refused: %v", err)
	}
	folded := signed(map[string]any{"device_id": "dev-01", "kelvin": 1, "Kelvin": 2},
		`"device_id":"dev-01","kelvin":1,"Kelvin":2`)
	if _, err := verifier.VerifyJSON(folded, contracts.RuntimeHeartbeatMessageType, "dev-01", ""); err == nil {
		t.Fatal("names equal under the closed fold were admitted")
	}
}
