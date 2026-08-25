// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestHTTPChannelMatchesIngestAuthenticationVector(t *testing.T) {
	const (
		secret = "published-test-evidence-ingest-secret-with-256-bit-entropy-do-not-use"
		digest = "sha256:42f21f21565c4a7a98b228eac9708ce9f5ff1bf2889ee4affa4ed85cb7045b07"
		auth   = "Ori-Evidence-HMAC hmac-sha256:37552fdbc8a2e079e2f75fcc684153ce637459f295974c6ad259c3a70ff7e096"
	)
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
				`{"artifact_digest":"` + digest + `","authority_artifacts":[],"outcome":"accepted","reason":"","retriable":false,"v":1}`,
			)),
		}, nil
	})}
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint:   "https://authority.invalid/v1/evidence/artifacts",
		ClientID:   "gateway-test-01",
		Secret:     secret,
		HTTPClient: client,
		Now:        func() time.Time { return time.UnixMilli(1787000003100) },
		Nonce: func(out []byte) error {
			copy(out, []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff})
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
	if captured.Header.Get("X-Ori-Evidence-Key-Id") != "hkdf-sha256:169ab7dc3fdf30297687df61205d72ac" {
		t.Fatalf("key id = %q", captured.Header.Get("X-Ori-Evidence-Key-Id"))
	}
	if captured.Header.Get("X-Ori-Evidence-Artifact-Digest") != digest {
		t.Fatalf("digest = %q", captured.Header.Get("X-Ori-Evidence-Artifact-Digest"))
	}
	if captured.Header.Get("Authorization") != auth {
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
	// Complete canonical wire bytes from ori-specs checkpoint.json valid.
	return []byte(`{"anchor_epoch_id":"sha256:7f1b65b8b24f69807441d80c8901207cf22657a9630b12901418a99efbd36f0a","boot_id":7,"device_id":"energy-monitor-ikeja-01","high_water_seq":13,"issued_at_ms":1787000900000,"key_id":"sha256:63a611374f09754a7bca6c60fd51cf2ae05a6eb8f7c58fbbe7ff5906123784a0","signature":"ed25519:UF/Npaw1X02L5V5HH3dM4NbHt3rIRn7LAVzQS7UMQF974WrVuZNIm+owkhOZPfVA8/4gg7Xsk3+NNwYj8Ed6Bw==","v":1}`)
}
