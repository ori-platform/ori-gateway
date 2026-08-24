// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package evidence implements the gateway's role in evidence-exchange/v1.
//
// The gateway is a blind courier. It signs nothing on the evidence path and
// holds no evidence-authority key: outbound artifacts arrive already signed by
// the runtime's device key and leave unchanged, and inbound authority artifacts
// are handed back unmodified.
//
// The custody acknowledgement is the single exception, and it is not a
// signature. It is an HMAC under the runtime-gateway shared secret, asserting
// only that the courier holds an envelope durably. It is deliberately not a
// delivery confirmation: nothing here says the evidence authority received
// anything.
package evidence

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/canonicaljson"
)

const (
	// CustodyDomain is the ASCII domain from evidence-exchange/v1. Authenticated
	// bytes are this domain, one NUL byte, then the canonical artifact with its
	// authenticator removed. It is distinct from any runtime-gateway MQTT message
	// type on purpose: a custody acknowledgement must not be mistakable for, or
	// forgeable from, an envelope of any other kind.
	CustodyDomain = "ori.evidence_custody_ack.v1"

	// CustodyVersion is the only artifact version this package produces. A
	// receiver rejects an unrecognised version rather than ignoring it.
	CustodyVersion = 1

	macPrefix = "hmac-sha256:"

	// maxSafeInteger bounds integers under the D-011 agreement zone, which
	// evidence-exchange/v1 adopts. Values beyond it are not representable in an
	// IEEE-754 double, so a consumer parsing into f64 would read a different
	// number than the one that was authenticated.
	maxSafeInteger = int64(9007199254740991)

	// sha256HexLen is the hex length of a SHA-256 digest.
	sha256HexLen = 64
)

// CustodyAcknowledgement is the artifact defined by evidence-exchange/v1. Field
// names and their JSON spellings are contract surface, not implementation
// detail; evidence-exchange/vectors/custody-acknowledgement.json is the arbiter.
type CustodyAcknowledgement struct {
	V              int    `json:"v"`
	DeviceID       string `json:"device_id"`
	LocalSeq       int64  `json:"local_seq"`
	EnvelopeDigest string `json:"envelope_digest"`
	CustodyAtMS    int64  `json:"custody_at_ms"`
	KeyID          string `json:"key_id"`
	MAC            string `json:"mac"`
}

// Salt and info are fixed by evidence-exchange/v1 and must not vary per device
// or per site. The identifier names a secret generation, not a device: one
// gateway holding custody for several devices under one secret issues the same
// key_id for all of them, and that is correct rather than a collision.
const (
	custodyKeyIDSalt   = "ori.evidence_custody_key_id.v1"
	custodyKeyIDInfo   = "gateway_custody"
	custodyKeyIDPrefix = "hkdf-sha256:"
	custodyKeyIDBytes  = 16
)

// DeriveCustodyKeyID returns the (gateway_custody, key_id) selector for one
// custody secret generation.
//
// Derived rather than configured, because a configured name survives a
// rotation unless an operator remembers to change it, and an identifier that
// names two different secrets cannot select between them -- which is the
// selector's whole function on the runtime side.
func DeriveCustodyKeyID(sharedSecret string) (string, error) {
	secret := strings.TrimSpace(sharedSecret)
	if secret == "" {
		return "", fmt.Errorf("evidence: custody secret must not be empty")
	}
	material, err := hkdf.Key(
		sha256.New,
		[]byte(secret),
		[]byte(custodyKeyIDSalt),
		custodyKeyIDInfo,
		custodyKeyIDBytes,
	)
	if err != nil {
		return "", fmt.Errorf("evidence: deriving custody key_id: %w", err)
	}
	return custodyKeyIDPrefix + hex.EncodeToString(material), nil
}

// CustodySigner authenticates custody acknowledgements with the current
// custody secret.
//
// It holds exactly one secret. A rotated-out secret is verify-only by
// definition here, because this type never verifies anything: it signs, and it
// signs only with the current generation. Keeping a previous secret would let
// the courier issue custody under a key the runtime has already retired.
type CustodySigner struct {
	keyID  string
	secret []byte
}

// NewCustodySigner builds a signer for one custody secret generation.
//
// The identifier is derived here and cannot be supplied. Accepting one would
// let a misconfigured or hostile value name a secret it was not derived from,
// and the runtime selects by that identifier -- so a mismatch would present as
// a failed authentication rather than as the configuration error it is.
func NewCustodySigner(sharedSecret string) (*CustodySigner, error) {
	secret := strings.TrimSpace(sharedSecret)
	if secret == "" {
		return nil, fmt.Errorf("evidence: custody shared secret must not be empty")
	}
	keyID, err := DeriveCustodyKeyID(secret)
	if err != nil {
		return nil, err
	}
	return &CustodySigner{keyID: keyID, secret: []byte(secret)}, nil
}

// KeyID names the shared-secret generation this signer authenticates under.
func (s *CustodySigner) KeyID() string {
	if s == nil {
		return ""
	}
	return s.keyID
}

// Acknowledge returns a custody acknowledgement for one sealed envelope, along
// with the exact bytes that were authenticated.
//
// It asserts durable custody only. Callers must not issue one before the
// envelope is durably held, because the runtime uses custody state to manage its
// own queue: an acknowledgement for evidence the courier has not actually
// retained would let real evidence be deprioritised.
func (s *CustodySigner) Acknowledge(deviceID string, localSeq int64, envelopeDigest string, custodyAtMS int64) (CustodyAcknowledgement, []byte, error) {
	if s == nil {
		return CustodyAcknowledgement{}, nil, fmt.Errorf("evidence: nil custody signer")
	}
	if strings.TrimSpace(deviceID) == "" {
		return CustodyAcknowledgement{}, nil, fmt.Errorf("evidence: custody device_id must not be empty")
	}
	if err := validateSHA256Digest(envelopeDigest); err != nil {
		return CustodyAcknowledgement{}, nil, err
	}
	// Delivery sequencing starts at 1. Zero is the checkpoint's "before any
	// envelope" state, so custody of envelope zero claims custody of something
	// that does not exist.
	if localSeq <= 0 {
		return CustodyAcknowledgement{}, nil, fmt.Errorf("evidence: local_seq must be positive; delivery sequencing starts at 1")
	}
	if custodyAtMS <= 0 {
		return CustodyAcknowledgement{}, nil, fmt.Errorf("evidence: custody_at_ms must be positive")
	}

	ack := CustodyAcknowledgement{
		V:              CustodyVersion,
		DeviceID:       deviceID,
		LocalSeq:       localSeq,
		EnvelopeDigest: envelopeDigest,
		CustodyAtMS:    custodyAtMS,
		KeyID:          s.keyID,
	}
	signed, err := custodyPreimage(ack)
	if err != nil {
		return CustodyAcknowledgement{}, nil, err
	}
	mac := hmac.New(sha256.New, s.secret)
	mac.Write(signed)
	ack.MAC = macPrefix + hex.EncodeToString(mac.Sum(nil))
	return ack, signed, nil
}

// custodyPreimage returns DOMAIN || NUL || canonical(artifact without mac).
func custodyPreimage(ack CustodyAcknowledgement) ([]byte, error) {
	body, err := custodyCanonicalBody(ack)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(CustodyDomain)+1+len(body))
	out = append(out, CustodyDomain...)
	out = append(out, 0)
	return append(out, body...), nil
}

// custodyCanonicalBody canonicalises the artifact with its authenticator removed.
//
// Integers are converted through d011Integer rather than handed to the writer as
// Go ints. The shared writer accepts only json.Number precisely so that a
// numeric policy decision cannot be made by accident here: this contract adopts
// the D-011 agreement zone, while the runtime-gateway MQTT transport does not.
func custodyCanonicalBody(ack CustodyAcknowledgement) ([]byte, error) {
	version, err := d011Integer(int64(ack.V), "v")
	if err != nil {
		return nil, err
	}
	localSeq, err := d011Integer(ack.LocalSeq, "local_seq")
	if err != nil {
		return nil, err
	}
	custodyAt, err := d011Integer(ack.CustodyAtMS, "custody_at_ms")
	if err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(map[string]any{
		"v":               version,
		"device_id":       ack.DeviceID,
		"local_seq":       localSeq,
		"envelope_digest": ack.EnvelopeDigest,
		"custody_at_ms":   custodyAt,
		"key_id":          ack.KeyID,
	})
}

func d011Integer(n int64, field string) (json.Number, error) {
	if n > maxSafeInteger || n < -maxSafeInteger {
		return "", fmt.Errorf("evidence: %s is outside the D-011 integer zone", field)
	}
	return json.Number(strconv.FormatInt(n, 10)), nil
}

// validateSHA256Digest requires the exact form the contract specifies: the
// algorithm prefix followed by 64 lowercase hexadecimal characters.
//
// Checking only the prefix would let a truncated, uppercase, or non-hex value be
// authenticated into an artifact that looks well-formed. The gateway cannot
// recompute the digest -- it never opens the envelope -- so this is the only
// point at which a malformed one can be caught before it is signed.
func validateSHA256Digest(digest string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) {
		return fmt.Errorf("evidence: envelope_digest must carry its algorithm prefix")
	}
	body := digest[len(prefix):]
	if len(body) != sha256HexLen {
		return fmt.Errorf("evidence: envelope_digest must be %d hex characters, got %d", sha256HexLen, len(body))
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf("evidence: envelope_digest must be lowercase hexadecimal")
	}
	return nil
}
