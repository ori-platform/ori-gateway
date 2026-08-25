// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestHTTPChannelMatchesIngestAuthenticationVector(t *testing.T) {
	vector := ingestAuthenticationVector(t)
	payload := checkpointVectorWire(t)
	var captured *http.Request
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		captured = req.Clone(req.Context())
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		captured.Body = io.NopCloser(strings.NewReader(string(body)))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(
				`{"artifact_digest":"` + vector.ArtifactDigest + `","authority_artifacts":[],"outcome":"accepted","reason":"","retriable":false,"v":1}`,
			)),
		}, nil
	})}
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint:   "https://authority.invalid/v1/evidence/artifacts",
		ClientID:   vector.Request.ClientID,
		Secret:     vector.Secret,
		HTTPClient: client,
		Now:        func() time.Time { return time.UnixMilli(vector.Request.SentAtMS) },
		Nonce: func(out []byte) error {
			nonce, err := hex.DecodeString(vector.Request.Nonce)
			if err != nil {
				return err
			}
			copy(out, nonce)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := channel.Deliver(context.Background(), QueuedArtifact{
		Type: ArtifactCheckpoint, Payload: payload,
	})
	if err != nil || !result.Accepted {
		t.Fatalf("deliver = %#v, %v", result, err)
	}
	if captured.Header.Get("X-Ori-Evidence-Key-Id") != vector.KeyID {
		t.Fatalf("key id = %q", captured.Header.Get("X-Ori-Evidence-Key-Id"))
	}
	if captured.Header.Get("X-Ori-Evidence-Artifact-Digest") != vector.ArtifactDigest {
		t.Fatalf("digest = %q", captured.Header.Get("X-Ori-Evidence-Artifact-Digest"))
	}
	if captured.Header.Get("Authorization") != vector.ExpectedAuthorization {
		t.Fatalf("authorization = %q", captured.Header.Get("Authorization"))
	}
	got, err := io.ReadAll(captured.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("HTTP channel changed the queued artifact bytes")
	}
}

func TestHTTPChannelRefusesNonHTTPSOrWrongPath(t *testing.T) {
	for _, endpoint := range []string{
		"http://authority.invalid/v1/evidence/artifacts",
		"https://authority.invalid/ingest",
		"https://authority.invalid/v1/evidence/artifacts?device=d",
	} {
		if _, err := NewHTTPChannel(HTTPChannelOptions{
			Endpoint: endpoint,
			ClientID: "gateway-test-01",
			Secret:   "published-test-evidence-ingest-secret-with-256-bit-entropy-do-not-use",
		}); err == nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}

func TestHTTPChannelRequiresContractualBackpressureAndHonorsRetryAfter(t *testing.T) {
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: "https://authority.invalid/v1/evidence/artifacts", ClientID: "gateway-test-01",
		Secret: "published-test-evidence-ingest-secret-with-256-bit-entropy-do-not-use",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			digest := req.Header.Get("X-Ori-Evidence-Artifact-Digest")
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{"17"}},
				Body:       io.NopCloser(strings.NewReader(`{"artifact_digest":"` + digest + `","authority_artifacts":[],"outcome":"pending","reason":"rate_limited","retriable":true,"v":1}`)),
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := channel.Deliver(context.Background(), QueuedArtifact{Type: ArtifactCheckpoint, Payload: checkpointVectorWire(t)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted || !result.Retriable || result.RetryAfter != 17*time.Second {
		t.Fatalf("backpressure result = %#v", result)
	}
}

func TestHTTPChannelRejectsNonContractualStatusBodyPair(t *testing.T) {
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: "https://authority.invalid/v1/evidence/artifacts", ClientID: "gateway-test-01",
		Secret: "published-test-evidence-ingest-secret-with-256-bit-entropy-do-not-use",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			digest := req.Header.Get("X-Ori-Evidence-Artifact-Digest")
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"artifact_digest":"` + digest + `","authority_artifacts":[],"outcome":"pending","reason":"busy","retriable":true,"v":1}`))}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.Deliver(context.Background(), QueuedArtifact{Type: ArtifactCheckpoint, Payload: checkpointVectorWire(t)}); err == nil {
		t.Fatal("uncontracted status/body pair was accepted")
	}
}

func TestHTTPChannelNeverFollowsRedirectWithEvidenceCredentials(t *testing.T) {
	var calls atomic.Int32
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: "https://authority.invalid/v1/evidence/artifacts", ClientID: "gateway-test-01",
		Secret: "published-test-evidence-ingest-secret-with-256-bit-entropy-do-not-use",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{
				StatusCode: http.StatusTemporaryRedirect,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "Location": []string{"https://other.invalid/v1/evidence/artifacts"}},
				Body:       io.NopCloser(strings.NewReader(`{"artifact_digest":"","authority_artifacts":[],"outcome":"refused","reason":"redirect","retriable":false,"v":1}`)),
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.Deliver(context.Background(), QueuedArtifact{Type: ArtifactCheckpoint, Payload: checkpointVectorWire(t)}); err == nil {
		t.Fatal("redirect was treated as an authority response")
	}
	if calls.Load() != 1 {
		t.Fatalf("evidence request followed redirect: %d HTTP requests", calls.Load())
	}
}

func checkpointVectorWire(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/gateway-api/vectors/outbound-evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Cases []struct {
			Name               string `json:"name"`
			DecodedArtifactHex string `json:"decoded_artifact_hex"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range vector.Cases {
		if testCase.Name == "checkpoint_carriage" {
			wire, err := hex.DecodeString(testCase.DecodedArtifactHex)
			if err != nil {
				t.Fatal(err)
			}
			checkpointRaw, err := os.ReadFile("testdata/evidence-exchange/vectors/checkpoint.json")
			if err != nil {
				t.Fatal(err)
			}
			var checkpoints struct {
				Cases []struct {
					Name     string         `json:"name"`
					Artifact map[string]any `json:"artifact"`
				} `json:"cases"`
			}
			if err := json.Unmarshal(checkpointRaw, &checkpoints); err != nil {
				t.Fatal(err)
			}
			for _, checkpoint := range checkpoints.Cases {
				if checkpoint.Name == "valid" {
					canonical, err := json.Marshal(checkpoint.Artifact)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(canonical, wire) {
						t.Fatal("gateway carriage bytes drifted from the evidence-exchange checkpoint")
					}
					return wire
				}
			}
			t.Fatal("checkpoint vector has no valid case")
			return wire
		}
	}
	t.Fatal("outbound evidence vector has no checkpoint_carriage case")
	return nil
}

func ingestAuthenticationVector(t *testing.T) struct {
	Secret                string `json:"secret"`
	KeyID                 string `json:"key_id"`
	ArtifactDigest        string `json:"artifact_digest"`
	ExpectedAuthorization string `json:"expected_authorization"`
	Request               struct {
		ClientID string `json:"client_id"`
		SentAtMS int64  `json:"sent_at_ms"`
		Nonce    string `json:"nonce"`
	} `json:"request"`
} {
	t.Helper()
	raw, err := os.ReadFile("testdata/evidence-transport/vectors/ingest-auth.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Secret                string `json:"secret"`
		KeyID                 string `json:"key_id"`
		ArtifactDigest        string `json:"artifact_digest"`
		ExpectedAuthorization string `json:"expected_authorization"`
		Request               struct {
			ClientID string `json:"client_id"`
			SentAtMS int64  `json:"sent_at_ms"`
			Nonce    string `json:"nonce"`
		} `json:"request"`
	}
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	return vector
}
