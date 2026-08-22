// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type custodyVectorFile struct {
	DomainASCII      string `json:"domain_ascii"`
	GatewaySecretHex string `json:"gateway_secret_hex"`
	Cases            []struct {
		Name          string `json:"name"`
		Authenticator string `json:"authenticator"`
		Expected      string `json:"expected"`
		CanonicalHex  string `json:"canonical_hex"`
		Artifact      struct {
			V              int    `json:"v"`
			DeviceID       string `json:"device_id"`
			LocalSeq       int64  `json:"local_seq"`
			EnvelopeDigest string `json:"envelope_digest"`
			CustodyAtMS    int64  `json:"custody_at_ms"`
			KeyID          string `json:"key_id"`
			MAC            string `json:"mac"`
		} `json:"artifact"`
	} `json:"cases"`
}

func loadCustodyVectors(t *testing.T) custodyVectorFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/custody-acknowledgement.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var doc custodyVectorFile
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("vector file carries no cases")
	}
	return doc
}

// TestCustodyMatchesContractVectors is the conformance test. The vector file is
// the arbiter of these bytes, so the producer is checked against it rather than
// against a locally-computed expectation.
func TestCustodyMatchesContractVectors(t *testing.T) {
	doc := loadCustodyVectors(t)
	if doc.DomainASCII != CustodyDomain {
		t.Fatalf("domain drift: contract says %q, package says %q", doc.DomainASCII, CustodyDomain)
	}
	secret, err := hex.DecodeString(doc.GatewaySecretHex)
	if err != nil {
		t.Fatalf("decode gateway secret: %v", err)
	}

	checked := 0
	for _, c := range doc.Cases {
		// A case whose authenticator is deliberately invalid cannot be reproduced
		// by a correct producer; it exists to exercise a verifier.
		if c.Authenticator != "valid" {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			signer, err := NewCustodySigner(c.Artifact.KeyID, string(secret))
			if err != nil {
				t.Fatalf("signer: %v", err)
			}
			ack, signed, err := signer.Acknowledge(
				c.Artifact.DeviceID, c.Artifact.LocalSeq,
				c.Artifact.EnvelopeDigest, c.Artifact.CustodyAtMS)
			if err != nil {
				t.Fatalf("acknowledge: %v", err)
			}

			wantCanonical, err := hex.DecodeString(c.CanonicalHex)
			if err != nil {
				t.Fatalf("decode canonical_hex: %v", err)
			}
			gotBody := signed[len(CustodyDomain)+1:]
			if string(gotBody) != string(wantCanonical) {
				t.Errorf("canonical bytes differ\n got %q\nwant %q", gotBody, wantCanonical)
			}
			if signed[len(CustodyDomain)] != 0 {
				t.Error("the domain must be terminated by a single NUL byte")
			}
			if ack.MAC != c.Artifact.MAC {
				t.Errorf("mac differs\n got %s\nwant %s", ack.MAC, c.Artifact.MAC)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatal("no reproducible vector case was exercised")
	}
}

// TestCustodyRejectionVectorsAreNotReproducible guards the conformance test
// above. A rejection case that a correct producer happens to reproduce would be
// testing nothing, which is how two rejection vectors were once silently erased.
func TestCustodyRejectionVectorsAreNotReproducible(t *testing.T) {
	doc := loadCustodyVectors(t)
	secret, _ := hex.DecodeString(doc.GatewaySecretHex)
	seen := 0
	for _, c := range doc.Cases {
		if c.Expected != "reject" || c.Authenticator != "invalid" {
			continue
		}
		seen++
		signer, err := NewCustodySigner(c.Artifact.KeyID, string(secret))
		if err != nil {
			continue
		}
		ack, _, err := signer.Acknowledge(
			c.Artifact.DeviceID, c.Artifact.LocalSeq,
			c.Artifact.EnvelopeDigest, c.Artifact.CustodyAtMS)
		if err == nil && ack.MAC == c.Artifact.MAC {
			t.Errorf("%s carries a MAC a correct producer reproduces; it cannot test a rejection", c.Name)
		}
	}
	if seen == 0 {
		t.Fatal("no invalid-authenticator vector present; the guard tests nothing")
	}
}

// TestCustodySignerRefusesIncompleteInput fails closed rather than emitting an
// artifact a receiver would reject or, worse, accept with a wrong binding.
func TestCustodySignerRefusesIncompleteInput(t *testing.T) {
	signer, err := NewCustodySigner("gw-secret-1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	validDigest := "sha256:" + strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name                string
		deviceID, digest    string
		localSeq, custodyAt int64
	}{
		{"empty device", "", validDigest, 1, 1787000000900},
		{"zero custody_at_ms", "dev-01", validDigest, 1, 0},
		{"local_seq beyond D-011", "dev-01", validDigest, 9007199254740992, 1787000000900},

		// Delivery sequencing starts at 1. Zero is the checkpoint's "before any
		// envelope" state, so custody of envelope zero claims something that does
		// not exist.
		{"zero local_seq", "dev-01", validDigest, 0, 1787000000900},
		{"negative local_seq", "dev-01", validDigest, -1, 1787000000900},

		// The gateway never opens the envelope, so it cannot recompute the digest.
		// Signing is the last point a malformed one can be caught.
		{"digest without prefix", "dev-01", strings.Repeat("a", 64), 1, 1787000000900},
		{"digest too short", "dev-01", "sha256:abc", 1, 1787000000900},
		{"digest one char short", "dev-01", "sha256:" + strings.Repeat("a", 63), 1, 1787000000900},
		{"digest one char long", "dev-01", "sha256:" + strings.Repeat("a", 65), 1, 1787000000900},
		{"digest uppercase hex", "dev-01", "sha256:" + strings.Repeat("A", 64), 1, 1787000000900},
		{"digest non-hex", "dev-01", "sha256:" + strings.Repeat("z", 64), 1, 1787000000900},
		{"digest empty body", "dev-01", "sha256:", 1, 1787000000900},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := signer.Acknowledge(tc.deviceID, tc.localSeq, tc.digest, tc.custodyAt); err == nil {
				t.Error("expected refusal")
			}
		})
	}
	if _, err := NewCustodySigner("", "secret"); err == nil {
		t.Error("expected refusal for an empty key_id")
	}
	if _, err := NewCustodySigner("gw-secret-1", "  "); err == nil {
		t.Error("expected refusal for an empty shared secret")
	}
}

// TestCustodyPreimageExcludesTheAuthenticator states the rule directly: an
// artifact never authenticates its own authenticator.
func TestCustodyPreimageExcludesTheAuthenticator(t *testing.T) {
	signer, _ := NewCustodySigner("gw-secret-1", "secret")
	ack, signed, err := signer.Acknowledge("dev-01", 7, "sha256:"+strings.Repeat("ab", 32), 1787000000900)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(signed), "\"mac\"") {
		t.Error("the preimage must exclude the mac field")
	}
	if !strings.HasPrefix(ack.MAC, macPrefix) {
		t.Errorf("mac must carry its algorithm prefix, got %q", ack.MAC)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(ack.MAC, macPrefix)); err != nil {
		t.Errorf("mac must be lowercase hex: %v", err)
	}
	if got := strings.TrimPrefix(ack.MAC, macPrefix); got != strings.ToLower(got) {
		t.Error("mac hex must be lowercase")
	}
}

// TestVendoredCustodyVectorsMatchManifest keeps the vendored corpus honest.
func TestVendoredCustodyVectorsMatchManifest(t *testing.T) {
	raw, err := os.ReadFile("testdata/MANIFEST.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m struct {
		SourceRepository string            `json:"source_repository"`
		SourceCommit     string            `json:"source_commit"`
		Files            map[string]string `json:"files"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.SourceCommit == "" || m.SourceRepository == "" || len(m.Files) == 0 {
		t.Fatal("manifest must record the upstream repository, commit, and files")
	}
	for name, want := range m.Files {
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		sum := sha256.Sum256(body)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s has been edited locally; re-vendor from %s@%s instead",
				name, m.SourceRepository, m.SourceCommit)
		}
	}
}

// TestCustodyAcceptsAWellFormedDigest is the positive half of the digest rule,
// so the validator cannot regress into refusing everything and still pass.
func TestCustodyAcceptsAWellFormedDigest(t *testing.T) {
	signer, err := NewCustodySigner("gw-secret-1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{
		"sha256:" + strings.Repeat("0", 64),
		"sha256:" + strings.Repeat("f", 64),
		"sha256:405837b420ee850d8e0503cc81e0d6a3f2b1c4e59a7d8306b2c1f4e5a6d7b8c9",
	} {
		if _, _, err := signer.Acknowledge("dev-01", 1, digest, 1787000000900); err != nil {
			t.Errorf("well-formed digest refused: %s: %v", digest, err)
		}
	}
	// The lowest sequence a real envelope can carry must be accepted.
	if _, _, err := signer.Acknowledge("dev-01", 1, "sha256:"+strings.Repeat("ab", 32), 1787000000900); err != nil {
		t.Errorf("local_seq 1 refused: %v", err)
	}
}
