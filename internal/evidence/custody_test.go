// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type custodyVectorFile struct {
	DomainASCII string `json:"domain_ascii"`
	// Three generations are published. The active one signs the valid case; the
	// others exist so a rotation window and a retired tombstone can be
	// exercised, and so a case naming an identifier no secret derives can be
	// shown to be unproducible.
	GatewaySecretHex         string `json:"gateway_secret_hex"`
	PreviousGatewaySecretHex string `json:"previous_gateway_secret_hex"`
	RetiredGatewaySecretHex  string `json:"retired_gateway_secret_hex"`
	Cases                    []struct {
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

// custodySecrets returns every published secret keyed by the identifier it
// derives to. Selecting a producer this way rather than assuming one secret is
// what the derived identifier now requires: the cases are signed under three
// generations, and a producer holding only the active one can reproduce none
// of the others.
func custodySecrets(t *testing.T, doc custodyVectorFile) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, hexSecret := range []string{
		doc.GatewaySecretHex,
		doc.PreviousGatewaySecretHex,
		doc.RetiredGatewaySecretHex,
	} {
		if hexSecret == "" {
			continue
		}
		raw, err := hex.DecodeString(hexSecret)
		if err != nil {
			t.Fatalf("decode secret: %v", err)
		}
		keyID, err := DeriveCustodyKeyID(string(raw))
		if err != nil {
			t.Fatalf("derive: %v", err)
		}
		out[keyID] = string(raw)
	}
	if len(out) == 0 {
		t.Fatal("the vector file publishes no secrets")
	}
	return out
}

// TestCustodyMatchesContractVectors is the conformance test. The vector file is
// the arbiter of these bytes, so the producer is checked against it rather than
// against a locally-computed expectation.
//
// A case is reproducible exactly when some published secret derives the
// identifier it names. That is not a convenience for the test -- it is the
// property the derived identifier creates, and checking it both ways is what
// proves a conforming producer cannot emit the adversarial cases at all.
func TestCustodyMatchesContractVectors(t *testing.T) {
	doc := loadCustodyVectors(t)
	if doc.DomainASCII != CustodyDomain {
		t.Fatalf("domain drift: contract says %q, package says %q", doc.DomainASCII, CustodyDomain)
	}
	secrets := custodySecrets(t, doc)

	reproduced, refused := 0, 0
	for _, c := range doc.Cases {
		secret, derivable := secrets[c.Artifact.KeyID]

		// An identifier no held secret derives to cannot be produced here, and
		// a producer that could emit one would be naming a generation it does
		// not hold.
		if !derivable {
			refused++
			t.Run(c.Name+"_is_not_producible", func(t *testing.T) {
				for keyID, held := range secrets {
					signer, err := NewCustodySigner(held)
					if err != nil {
						t.Fatalf("signer: %v", err)
					}
					if signer.KeyID() == c.Artifact.KeyID {
						t.Fatalf("secret for %s derives the case's identifier after all", keyID)
					}
				}
			})
			continue
		}

		// A case whose authenticator is deliberately invalid cannot be
		// reproduced by a correct producer; it exists to exercise a verifier.
		if c.Authenticator != "valid" {
			continue
		}

		reproduced++
		t.Run(c.Name, func(t *testing.T) {
			signer, err := NewCustodySigner(secret)
			if err != nil {
				t.Fatalf("signer: %v", err)
			}
			// Derived, not supplied. Passing the vector's own identifier in
			// would have made this assertion circular.
			if signer.KeyID() != c.Artifact.KeyID {
				t.Fatalf("derived key_id %s, vector names %s", signer.KeyID(), c.Artifact.KeyID)
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
			if ack.MAC == c.Artifact.MAC {
				return
			}

			// A conforming producer always writes the identifier it derives, so
			// no producer configuration can emit a body naming one generation
			// while authenticated under another. Differing bytes therefore mean
			// either that -- the case that separates selection from trial
			// verification on the verifying side -- or a defect here.
			//
			// Telling them apart is a verifier's question, not a producer's:
			// recompute the MAC over the vector's own canonical bytes under
			// each held secret. If one matches, the artifact is internally
			// consistent and unproducible by construction.
			signedUnder := ""
			for keyID, held := range secrets {
				if keyID == c.Artifact.KeyID {
					continue
				}
				mac := hmac.New(sha256.New, []byte(held))
				mac.Write([]byte(CustodyDomain))
				mac.Write([]byte{0})
				mac.Write(wantCanonical)
				if "hmac-sha256:"+hex.EncodeToString(mac.Sum(nil)) == c.Artifact.MAC {
					signedUnder = keyID
					break
				}
			}
			if signedUnder == "" {
				t.Errorf("mac differs and no other held generation produces it\n got %s\nwant %s",
					ack.MAC, c.Artifact.MAC)
				return
			}
			if c.Expected != "reject" {
				t.Errorf("case names %s but was signed under %s, yet expects acceptance",
					c.Artifact.KeyID, signedUnder)
			}
			t.Logf("names %s, signed under %s: unproducible by construction, as intended",
				c.Artifact.KeyID, signedUnder)
		})
	}
	if reproduced == 0 {
		t.Fatal("no reproducible vector case was exercised")
	}
	if refused == 0 {
		t.Fatal("no non-producible case was exercised; the derived identifier proves nothing here")
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
		signer, err := NewCustodySigner(string(secret))
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
	signer, err := NewCustodySigner("secret")
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
	if _, err := NewCustodySigner("  "); err == nil {
		t.Error("expected refusal for an empty custody secret")
	}
}

// TestCustodyPreimageExcludesTheAuthenticator states the rule directly: an
// artifact never authenticates its own authenticator.
func TestCustodyPreimageExcludesTheAuthenticator(t *testing.T) {
	signer, _ := NewCustodySigner("secret")
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

// TestVendoredEvidenceVectorsMatchManifest keeps the vendored corpus honest.
func TestVendoredEvidenceVectorsMatchManifest(t *testing.T) {
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
	actual := make(map[string]bool)
	var walk func(string)
	walk = func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read fixture directory %s: %v", dir, err)
		}
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			if entry.IsDir() {
				walk(path)
				continue
			}
			if filepath.Ext(path) == ".json" && entry.Name() != "MANIFEST.json" {
				rel, err := filepath.Rel("testdata", path)
				if err != nil {
					t.Fatal(err)
				}
				actual[filepath.ToSlash(rel)] = true
			}
		}
	}
	walk("testdata")
	if len(actual) != len(m.Files) {
		t.Fatalf("manifest lists %d vectors but testdata contains %d", len(m.Files), len(actual))
	}
	for name := range actual {
		if _, ok := m.Files[name]; !ok {
			t.Errorf("vendored vector %s is not tracked by the manifest", name)
		}
	}
}

// TestCustodyAcceptsAWellFormedDigest is the positive half of the digest rule,
// so the validator cannot regress into refusing everything and still pass.
func TestCustodyAcceptsAWellFormedDigest(t *testing.T) {
	signer, err := NewCustodySigner("secret")
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

// TestCustodySecretIsNeverNormalised holds the line the contract draws: the key
// is the secret's exact UTF-8 bytes.
//
// A secret carrying a stray newline is the ordinary shape of one read from a
// file or a Docker env file. Trimming it would key from bytes the operator did
// not provision and derive an identifier and a MAC no conforming peer
// reproduces -- a divergence that surfaces as bad_authenticator on the far side
// rather than as the configuration error it is. The assertion is written so it
// still holds if a later revision decides to accept surrounding whitespace:
// what may never happen is such a secret quietly deriving the trimmed one's
// identifier.
func TestCustodySecretIsNeverNormalised(t *testing.T) {
	const base = "custody-secret"
	trimmed, err := DeriveCustodyKeyID(base)
	if err != nil {
		t.Fatalf("deriving from a well-formed secret: %v", err)
	}

	for _, variant := range []string{
		base + "\n",
		base + " ",
		" " + base,
		"\t" + base + "\t",
		"\r\n" + base,
	} {
		t.Run(strconv.Quote(variant), func(t *testing.T) {
			got, err := DeriveCustodyKeyID(variant)
			if err != nil {
				return
			}
			if got == trimmed {
				t.Fatalf("%q derived the trimmed secret's key_id; secret material was normalised", variant)
			}
		})
	}
}
