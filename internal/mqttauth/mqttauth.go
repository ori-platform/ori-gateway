// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/canonicaljson"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const signaturePrefix = "hmac-sha256:"

const (
	defaultMaxSkew   = 5 * time.Minute
	defaultReplayTTL = 5 * time.Minute
)

// Config controls HMAC verification for site-local runtime/gateway MQTT messages.
type Config struct {
	SharedSecret         string
	PreviousSharedSecret string
	MaxSkew              time.Duration
	ReplayTTL            time.Duration
	Now                  func() time.Time
}

// Verifier validates signed MQTT envelopes and rejects short-window replays.
type Verifier struct {
	sharedSecret         string
	previousSharedSecret string
	maxSkew              time.Duration
	replayTTL            time.Duration
	now                  func() time.Time

	mu       sync.Mutex
	seenKeys map[string]time.Time
}

// NewVerifier builds a verifier with bounded in-memory replay protection.
func NewVerifier(cfg Config) (*Verifier, error) {
	secret := strings.TrimSpace(cfg.SharedSecret)
	if secret == "" {
		return nil, fmt.Errorf("mqtt auth shared secret must not be empty")
	}
	maxSkew := cfg.MaxSkew
	if maxSkew <= 0 {
		maxSkew = defaultMaxSkew
	}
	replayTTL := cfg.ReplayTTL
	if replayTTL <= 0 {
		replayTTL = defaultReplayTTL
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	previousSecret := strings.TrimSpace(cfg.PreviousSharedSecret)
	if previousSecret != "" && hmac.Equal([]byte(previousSecret), []byte(secret)) {
		return nil, fmt.Errorf("mqtt auth previous shared secret must differ from shared secret")
	}
	return &Verifier{
		sharedSecret:         secret,
		previousSharedSecret: previousSecret,
		maxSkew:              maxSkew,
		replayTTL:            replayTTL,
		now:                  now,
		seenKeys:             map[string]time.Time{},
	}, nil
}

// Sign returns an HMAC envelope over value after removing any existing auth field.
func Sign(value any, messageType string, deviceID string, requestID string, signedAtMS int64, sharedSecret string) (contracts.HeartbeatAuth, error) {
	secret := strings.TrimSpace(sharedSecret)
	if secret == "" {
		return contracts.HeartbeatAuth{}, fmt.Errorf("mqtt auth shared secret must not be empty")
	}
	if signedAtMS <= 0 {
		return contracts.HeartbeatAuth{}, fmt.Errorf("signed_at_ms must be positive")
	}
	canonical, err := CanonicalJSONWithoutAuth(value)
	if err != nil {
		return contracts.HeartbeatAuth{}, err
	}
	return contracts.HeartbeatAuth{
		Scheme:     contracts.HeartbeatAuthScheme,
		SignedAtMS: signedAtMS,
		Signature:  signaturePrefix + computeSignature(secret, SigningInput(messageType, deviceID, requestID, signedAtMS, canonical)),
	}, nil
}

// VerifyJSON verifies a signed JSON payload and returns the unsigned payload map.
func (v *Verifier) VerifyJSON(payload []byte, messageType string, expectedDeviceID string, expectedRequestID string) (map[string]any, error) {
	if v == nil {
		return nil, fmt.Errorf("mqtt auth verifier is nil")
	}
	if err := canonicaljson.ValidateWireUnicode(payload); err != nil {
		return nil, err
	}
	var envelope map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode mqtt auth payload: %w", err)
	}
	authValue, ok := envelope["auth"]
	if !ok {
		return nil, fmt.Errorf("missing auth envelope")
	}
	authBytes, err := json.Marshal(authValue)
	if err != nil {
		return nil, fmt.Errorf("encode auth envelope: %w", err)
	}
	var auth contracts.HeartbeatAuth
	if err := json.Unmarshal(authBytes, &auth); err != nil {
		return nil, fmt.Errorf("decode auth envelope: %w", err)
	}
	if auth.Scheme != contracts.HeartbeatAuthScheme {
		return nil, fmt.Errorf("unsupported auth scheme %q", auth.Scheme)
	}
	if auth.SignedAtMS <= 0 {
		return nil, fmt.Errorf("signed_at_ms must be positive")
	}
	if !strings.HasPrefix(auth.Signature, signaturePrefix) {
		return nil, fmt.Errorf("signature must use %s prefix", signaturePrefix)
	}
	if err := validateExpectedFields(envelope, expectedDeviceID, expectedRequestID); err != nil {
		return nil, err
	}

	unsigned := cloneWithoutAuth(envelope)
	canonical, err := canonicaljson.Marshal(unsigned)
	if err != nil {
		return nil, fmt.Errorf("canonicalize mqtt auth payload: %w", err)
	}
	if !v.signatureMatches(auth.Signature, messageType, expectedDeviceID, expectedRequestID, auth.SignedAtMS, canonical) {
		return nil, fmt.Errorf("mqtt auth signature mismatch")
	}
	if err := v.checkFreshAndRecord(messageType, expectedDeviceID, expectedRequestID, auth); err != nil {
		return nil, err
	}
	return unsigned, nil
}

// CanonicalJSONWithoutAuth returns deterministic JSON for value with auth removed.
func CanonicalJSONWithoutAuth(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	delete(payload, "auth")
	return canonicaljson.Marshal(payload)
}

// SigningInput returns the newline-delimited runtime-compatible signing string.
func SigningInput(messageType string, deviceID string, requestID string, signedAtMS int64, canonicalPayload []byte) string {
	return strings.Join([]string{
		messageType,
		deviceID,
		requestID,
		fmt.Sprintf("%d", signedAtMS),
		string(canonicalPayload),
	}, "\n")
}

func (v *Verifier) signatureMatches(signature string, messageType string, deviceID string, requestID string, signedAtMS int64, canonical []byte) bool {
	want := signaturePrefix + computeSignature(v.sharedSecret, SigningInput(messageType, deviceID, requestID, signedAtMS, canonical))
	if hmac.Equal([]byte(signature), []byte(want)) {
		return true
	}
	if v.previousSharedSecret == "" {
		return false
	}
	wantPrevious := signaturePrefix + computeSignature(v.previousSharedSecret, SigningInput(messageType, deviceID, requestID, signedAtMS, canonical))
	return hmac.Equal([]byte(signature), []byte(wantPrevious))
}

func computeSignature(secret string, signingInput string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	return hex.EncodeToString(mac.Sum(nil))
}

func validateExpectedFields(payload map[string]any, expectedDeviceID string, expectedRequestID string) error {
	if expectedDeviceID != "" {
		deviceID, ok := payload["device_id"].(string)
		if !ok || deviceID != expectedDeviceID {
			return fmt.Errorf("device_id %q does not match topic device_id %q", deviceID, expectedDeviceID)
		}
	}
	if expectedRequestID != "" {
		requestID, ok := payload["request_id"].(string)
		if !ok || requestID != expectedRequestID {
			return fmt.Errorf("request_id %q does not match expected request_id %q", requestID, expectedRequestID)
		}
	}
	return nil
}

func cloneWithoutAuth(payload map[string]any) map[string]any {
	unsigned := make(map[string]any, len(payload))
	for key, value := range payload {
		if key == "auth" {
			continue
		}
		unsigned[key] = value
	}
	return unsigned
}

func replayKey(messageType string, deviceID string, requestID string, signedAtMS int64, signature string) string {
	parts := []string{messageType, deviceID, requestID, fmt.Sprintf("%d", signedAtMS), signature}
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(fmt.Sprintf("%d:", len(part)))
		b.WriteString(part)
		b.WriteString(";")
	}
	return b.String()
}

func (v *Verifier) checkFreshAndRecord(messageType string, deviceID string, requestID string, auth contracts.HeartbeatAuth) error {
	now := v.now()
	signedAt := time.UnixMilli(auth.SignedAtMS)
	if signedAt.After(now.Add(v.maxSkew)) || signedAt.Before(now.Add(-v.maxSkew)) {
		return fmt.Errorf("mqtt auth signed_at_ms outside allowed skew")
	}

	key := replayKey(messageType, deviceID, requestID, auth.SignedAtMS, auth.Signature)
	v.mu.Lock()
	defer v.mu.Unlock()
	for seenKey, expiresAt := range v.seenKeys {
		if !expiresAt.After(now) {
			delete(v.seenKeys, seenKey)
		}
	}
	if expiresAt, ok := v.seenKeys[key]; ok && expiresAt.After(now) {
		return fmt.Errorf("mqtt auth replay detected")
	}
	v.seenKeys[key] = now.Add(v.replayTTL)
	return nil
}
