// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	encryptionField   = "encryption"
	encryptionScheme  = "aes-256-gcm"
	encryptionKeyInfo = "ori.gateway-mqtt.export-encryption.v1"
	encryptionSalt    = "ori-runtime-gateway-mqtt-v1"
)

// Encryptor decrypts runtime export payloads encrypted with the shared gateway secret.
type Encryptor struct {
	key []byte
}

// NewEncryptor derives the AES-256-GCM key used for runtime export encryption.
func NewEncryptor(sharedSecret string) (*Encryptor, error) {
	secret := strings.TrimSpace(sharedSecret)
	if secret == "" {
		return nil, fmt.Errorf("mqtt encryption shared secret must not be empty")
	}
	return &Encryptor{key: hkdfSHA256([]byte(secret), []byte(encryptionSalt), []byte(encryptionKeyInfo), 32)}, nil
}

// Encrypt returns a runtime-compatible encrypted envelope. It is primarily used by tests.
func (e *Encryptor) Encrypt(value any, messageType string, nonce []byte) (map[string]any, error) {
	if e == nil {
		return nil, fmt.Errorf("mqtt encryptor is nil")
	}
	plain, err := mapWithoutAuth(value)
	if err != nil {
		return nil, err
	}
	metadata, err := encryptionMetadata(plain)
	if err != nil {
		return nil, err
	}
	if len(nonce) != 12 {
		return nil, fmt.Errorf("mqtt encryption nonce must be 12 bytes")
	}
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	canonical, err := CanonicalJSONWithoutAuth(plain)
	if err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, canonical, encryptionAAD(metadata, messageType))
	return map[string]any{
		"request_id":  metadata["request_id"],
		"device_id":   metadata["device_id"],
		"export_type": metadata["export_type"],
		"encrypted":   true,
		encryptionField: map[string]any{
			"scheme":     encryptionScheme,
			"nonce":      base64.RawURLEncoding.EncodeToString(nonce),
			"ciphertext": base64.RawURLEncoding.EncodeToString(ciphertext),
		},
	}, nil
}

// Decrypt opens a runtime-compatible encrypted envelope and returns the plaintext object.
func (e *Encryptor) Decrypt(payload map[string]any, messageType string, expectedDeviceID string, expectedRequestID string) (map[string]any, error) {
	if e == nil {
		return nil, fmt.Errorf("mqtt encryptor is nil")
	}
	if encrypted, ok := payload["encrypted"].(bool); !ok || !encrypted {
		return nil, fmt.Errorf("missing encrypted flag")
	}
	metadata, err := encryptionMetadata(payload)
	if err != nil {
		return nil, err
	}
	if metadata["device_id"] != expectedDeviceID {
		return nil, fmt.Errorf("encrypted device_id %q does not match %q", metadata["device_id"], expectedDeviceID)
	}
	if expectedRequestID != "" && metadata["request_id"] != expectedRequestID {
		return nil, fmt.Errorf("encrypted request_id %q does not match %q", metadata["request_id"], expectedRequestID)
	}
	envelope, ok := payload[encryptionField].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing encryption envelope")
	}
	if scheme, _ := envelope["scheme"].(string); scheme != encryptionScheme {
		return nil, fmt.Errorf("unsupported encryption scheme %q", scheme)
	}
	nonceText, _ := envelope["nonce"].(string)
	ciphertextText, _ := envelope["ciphertext"].(string)
	nonce, err := base64.RawURLEncoding.DecodeString(nonceText)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(ciphertextText)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, encryptionAAD(metadata, messageType))
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}
	var decoded map[string]any
	dec := json.NewDecoder(bytes.NewReader(plaintext))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode plaintext: %w", err)
	}
	decodedMetadata, err := encryptionMetadata(decoded)
	if err != nil {
		return nil, err
	}
	if decodedMetadata["request_id"] != metadata["request_id"] || decodedMetadata["device_id"] != metadata["device_id"] || decodedMetadata["export_type"] != metadata["export_type"] {
		return nil, fmt.Errorf("encrypted metadata mismatch")
	}
	return decoded, nil
}

func mapWithoutAuth(value any) (map[string]any, error) {
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
	return payload, nil
}

func encryptionMetadata(payload map[string]any) (map[string]string, error) {
	metadata := map[string]string{
		"request_id":  stringFromAny(payload["request_id"]),
		"device_id":   stringFromAny(payload["device_id"]),
		"export_type": stringFromAny(payload["export_type"]),
	}
	for key, value := range metadata {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("missing %s", key)
		}
	}
	return metadata, nil
}

func encryptionAAD(metadata map[string]string, messageType string) []byte {
	payload := map[string]string{
		"message_type": messageType,
		"request_id":   metadata["request_id"],
		"device_id":    metadata["device_id"],
		"export_type":  metadata["export_type"],
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func hkdfSHA256(secret []byte, salt []byte, info []byte, length int) []byte {
	mac := hmac.New(sha256.New, salt)
	_, _ = mac.Write(secret)
	prk := mac.Sum(nil)
	out := make([]byte, 0, length)
	var previous []byte
	counter := byte(1)
	for len(out) < length {
		mac = hmac.New(sha256.New, prk)
		_, _ = mac.Write(previous)
		_, _ = mac.Write(info)
		_, _ = mac.Write([]byte{counter})
		previous = mac.Sum(nil)
		out = append(out, previous...)
		counter++
	}
	return out[:length]
}
