// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"github.com/ori-platform/ori-gateway/internal/evidence/refusal"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"
)

const (
	ingestPath         = "/v1/evidence/artifacts"
	ingestDomain       = "ori.evidence_ingest_request.v1"
	ingestKeyIDSalt    = "ori.evidence_ingest_key_id.v1"
	ingestKeyIDInfo    = "evidence_ingest"
	ingestKeyIDPrefix  = "hkdf-sha256:"
	maxResponseBytes   = 1 << 20
	defaultHTTPTimeout = 10 * time.Second
)

type HTTPChannelOptions struct {
	Endpoint   string
	ClientID   string
	Secret     string
	HTTPClient *http.Client
	Now        func() time.Time
	Nonce      func([]byte) error
}

type HTTPChannel struct {
	endpoint *url.URL
	clientID string
	keyID    string
	secret   []byte
	client   *http.Client
	now      func() time.Time
	nonce    func([]byte) error
}

func NewHTTPChannel(opts HTTPChannelOptions) (*HTTPChannel, error) {
	endpoint, err := url.Parse(strings.TrimSpace(opts.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.Path != ingestPath || endpoint.RawQuery != "" || endpoint.User != nil {
		return nil, fmt.Errorf("evidence: channel endpoint must be an exact HTTPS ingest URL")
	}
	if !validIngestClientID(opts.ClientID) {
		return nil, fmt.Errorf("evidence: ingest client id must be 1-128 printable ASCII characters")
	}
	keyID, err := DeriveIngestKeyID(opts.Secret)
	if err != nil {
		return nil, err
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	nonce := opts.Nonce
	if nonce == nil {
		nonce = func(out []byte) error {
			_, err := rand.Read(out)
			return err
		}
	}
	return &HTTPChannel{
		endpoint: endpoint,
		clientID: opts.ClientID,
		keyID:    keyID,
		secret:   []byte(opts.Secret),
		client:   &clientCopy,
		now:      now,
		nonce:    nonce,
	}, nil
}

func DeriveIngestKeyID(secret string) (string, error) {
	if len(secret) < 32 || strings.TrimSpace(secret) != secret {
		return "", fmt.Errorf("evidence: ingest secret must be exact UTF-8 with at least 32 bytes")
	}
	material, err := hkdf.Key(
		sha256.New,
		[]byte(secret),
		[]byte(ingestKeyIDSalt),
		ingestKeyIDInfo,
		16,
	)
	if err != nil {
		return "", fmt.Errorf("evidence: derive ingest key id: %w", err)
	}
	return ingestKeyIDPrefix + hex.EncodeToString(material), nil
}

func (c *HTTPChannel) Deliver(ctx context.Context, artifact QueuedArtifact) (DeliveryResult, error) {
	if c == nil {
		return DeliveryResult{}, fmt.Errorf("evidence: nil channel")
	}
	digest := sha256.Sum256(artifact.Payload)
	artifactDigest := "sha256:" + hex.EncodeToString(digest[:])
	nonceBytes := make([]byte, 16)
	if err := c.nonce(nonceBytes); err != nil {
		return DeliveryResult{}, fmt.Errorf("evidence: generate ingest nonce: %w", err)
	}
	sentAtMS := c.now().UnixMilli()
	if sentAtMS <= 0 || sentAtMS > custody.MaxSafeInteger {
		return DeliveryResult{}, fmt.Errorf("evidence: ingest clock is outside the D-011 integer zone")
	}
	nonce := hex.EncodeToString(nonceBytes)
	preimage, err := ingestPreimage(c.clientID, c.keyID, artifact.Type, artifactDigest, sentAtMS, nonce)
	if err != nil {
		return DeliveryResult{}, err
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write(preimage)
	authorization := "Ori-Evidence-HMAC hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint.String(), bytes.NewReader(artifact.Payload))
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("evidence: construct ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Ori-Evidence-Client", c.clientID)
	req.Header.Set("X-Ori-Evidence-Key-Id", c.keyID)
	req.Header.Set("X-Ori-Evidence-Sent-At-Ms", strconv.FormatInt(sentAtMS, 10))
	req.Header.Set("X-Ori-Evidence-Nonce", nonce)
	req.Header.Set("X-Ori-Evidence-Artifact-Type", string(artifact.Type))
	req.Header.Set("X-Ori-Evidence-Artifact-Digest", artifactDigest)
	req.Header.Set("Authorization", authorization)

	resp, err := c.client.Do(req)
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("evidence: ingest request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("evidence: ingest response unavailable")
	}
	// Everything below is a response the authority did send. One that falls
	// outside the refusal policy is an unrecognised outcome: retained, backed
	// off, never retired and never held.
	if len(body) > maxResponseBytes {
		return DeliveryResult{}, unrecognised("oversized response")
	}
	mediaType, _, mediaErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return DeliveryResult{}, unrecognised("wrong content type")
	}
	var wire channelResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return DeliveryResult{}, unrecognised("malformed response body")
	}
	if wire.V != 1 {
		return DeliveryResult{}, unrecognised("unsupported response version")
	}
	if resp.StatusCode != http.StatusOK {
		return refusedDeliveryResult(resp, wire, artifactDigest)
	}
	// Only a 200 meeting every acceptance requirement retires an entry.
	if wire.Outcome != "accepted" || wire.Reason != "" || wire.Retriable || wire.ArtifactDigest != artifactDigest {
		return DeliveryResult{}, unrecognised("malformed acceptance")
	}
	authority := make([]AuthorityArtifact, 0, len(wire.AuthorityArtifacts))
	for _, returned := range wire.AuthorityArtifacts {
		kind := AuthorityArtifactType(returned.ArtifactType)
		if !validAuthorityArtifactType(kind) {
			return DeliveryResult{}, unrecognised("unknown authority artifact type")
		}
		payload, err := base64.StdEncoding.Strict().DecodeString(returned.ArtifactB64)
		if err != nil || len(payload) == 0 {
			return DeliveryResult{}, unrecognised("malformed authority artifact bytes")
		}
		authority = append(authority, AuthorityArtifact{Type: kind, Payload: payload})
	}
	return DeliveryResult{Accepted: true, AuthorityArtifacts: authority}, nil
}

func unrecognised(detail string) error {
	return fmt.Errorf("%w: %s", ErrUnrecognisedOutcome, detail)
}

// refusedDeliveryResult checks that a non-200 response is well formed for its
// status, then takes its class from refusal.Policy, the evidence-transport/v2
// refusal policy table. The reason is not part of the well-formedness check: a
// missing or unadmitted reason is recorded as unrecognised and takes the
// status's fail-closed action. The retriable flag is never used to select a
// well-formed refusal's class.
func refusedDeliveryResult(resp *http.Response, wire channelResponse, artifactDigest string) (DeliveryResult, error) {
	recorded := refusal.RecordedReason(resp.StatusCode, wire.Reason)
	result := DeliveryResult{
		Accepted: false, RefusalStatus: resp.StatusCode, RefusalReason: recorded,
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusConflict, http.StatusUnprocessableEntity:
		// A 400 may omit the digest: evidence-transport/v2 refuses malformed
		// requests before authentication, and unauthenticated responses omit
		// it. Every other refusal here is authenticated and bound to the digest.
		digestBound := wire.ArtifactDigest == artifactDigest ||
			(resp.StatusCode == http.StatusBadRequest && wire.ArtifactDigest == "")
		if wire.Outcome != "refused" || !digestBound || len(wire.AuthorityArtifacts) != 0 {
			return DeliveryResult{}, unrecognised("malformed refusal")
		}
	case http.StatusUnauthorized:
		if wire.Outcome != "refused" || wire.ArtifactDigest != "" || len(wire.AuthorityArtifacts) != 0 {
			return DeliveryResult{}, unrecognised("malformed authentication refusal")
		}
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		digestBound := wire.ArtifactDigest == artifactDigest
		preAuthUnavailable := resp.StatusCode == http.StatusServiceUnavailable && wire.ArtifactDigest == ""
		if wire.Outcome != "pending" || (!digestBound && !preAuthUnavailable) || len(wire.AuthorityArtifacts) != 0 {
			return DeliveryResult{}, unrecognised("malformed backpressure")
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter, ok := parseRetryAfter(resp.Header.Get("Retry-After"))
			// A 429 MUST carry Retry-After: a rate_limited or
			// pending_registration_limit without a positive integer one is
			// missing retry metadata, an unrecognised outcome. Any other 429
			// backs off, waiting no less than a parseable Retry-After.
			if (recorded == "rate_limited" || recorded == "pending_registration_limit") && !ok {
				return DeliveryResult{}, unrecognised("rate limit without Retry-After")
			}
			result.RetryAfter = retryAfter
		}
	default:
		return DeliveryResult{}, unrecognised("status outside the refusal policy")
	}
	class := refusal.Policy(resp.StatusCode, wire.Reason)
	result.Retriable = class == refusal.Backoff
	result.ReceiverState = class == refusal.Handoff
	return result, nil
}

// parseRetryAfter reads a Retry-After given in whole seconds.
func parseRetryAfter(value string) (time.Duration, bool) {
	seconds, err := strconv.ParseUint(strings.TrimSpace(value), 10, 31)
	if err != nil || seconds == 0 {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

type channelResponse struct {
	V                  int                        `json:"v"`
	ArtifactDigest     string                     `json:"artifact_digest"`
	Outcome            string                     `json:"outcome"`
	Reason             string                     `json:"reason"`
	Retriable          bool                       `json:"retriable"`
	AuthorityArtifacts []channelAuthorityArtifact `json:"authority_artifacts"`
}

type channelAuthorityArtifact struct {
	ArtifactType string `json:"artifact_type"`
	ArtifactB64  string `json:"artifact_b64"`
}

func ingestPreimage(clientID, keyID string, kind ArtifactType, artifactDigest string, sentAtMS int64, nonce string) ([]byte, error) {
	canonical, err := canonicaljson.Marshal(map[string]any{
		"artifact_digest": artifactDigest,
		"artifact_type":   string(kind),
		"client_id":       clientID,
		"key_id":          keyID,
		"method":          http.MethodPost,
		"nonce":           nonce,
		"path":            ingestPath,
		"sent_at_ms":      json.Number(strconv.FormatInt(sentAtMS, 10)),
	})
	if err != nil {
		return nil, fmt.Errorf("evidence: canonicalize ingest authentication: %w", err)
	}
	out := make([]byte, 0, len(ingestDomain)+1+len(canonical))
	out = append(out, ingestDomain...)
	out = append(out, 0)
	return append(out, canonical...), nil
}

func validIngestClientID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range []byte(value) {
		if char < 0x20 || char > 0x7e {
			return false
		}
	}
	return true
}
