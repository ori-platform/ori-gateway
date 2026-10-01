// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build evidence_integration

package courier

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// upstreamAnswer is one request the proxy forwarded: what the authority
// answered, and whether the proxy rewrote it before the courier saw it.
type upstreamAnswer struct {
	status    int
	body      []byte
	rewritten bool
}

// rewritingProxy fronts the authority with TLS and forwards every request
// unchanged. While rewrite is set, it rewrites each 200 the authority sends
// before the courier reads it; every other status passes through.
type rewritingProxy struct {
	mu      sync.Mutex
	rewrite func(header http.Header, body []byte) []byte
	answers []upstreamAnswer
}

func (p *rewritingProxy) set(rewrite func(http.Header, []byte) []byte) {
	p.mu.Lock()
	p.rewrite = rewrite
	p.mu.Unlock()
}

func (p *rewritingProxy) seen() []upstreamAnswer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]upstreamAnswer(nil), p.answers...)
}

func (p *rewritingProxy) handler(bind string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req, _ := http.NewRequest(r.Method, "http://"+bind+r.URL.Path, bytes.NewReader(body))
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "upstream unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		header := resp.Header.Clone()
		header.Del("Content-Length")
		out := respBody
		p.mu.Lock()
		rewrite := p.rewrite
		answer := upstreamAnswer{status: resp.StatusCode, body: respBody}
		if rewrite != nil && resp.StatusCode == http.StatusOK {
			out = rewrite(header, append([]byte(nil), respBody...))
			answer.rewritten = true
		}
		p.answers = append(p.answers, answer)
		p.mu.Unlock()
		for k, v := range header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(out)
	})
}

// countingSink records staged authority artifacts under a lock, so a test can
// read it while the worker runs.
type countingSink struct {
	mu     sync.Mutex
	stored []AuthorityArtifact
}

func (s *countingSink) Store(_ context.Context, artifact AuthorityArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stored = append(s.stored, AuthorityArtifact{
		Type: artifact.Type, DeviceID: artifact.DeviceID, Payload: append([]byte(nil), artifact.Payload...),
	})
	return nil
}

func (s *countingSink) snapshot() []AuthorityArtifact {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AuthorityArtifact(nil), s.stored...)
}

// withMember rewrites one top-level member of a JSON object, or removes it
// when value is nil.
func withMember(t *testing.T, body []byte, name string, value any) []byte {
	t.Helper()
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Errorf("the authority's 200 is not a JSON object: %v", err)
		return body
	}
	if value == nil {
		delete(doc, name)
	} else {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		doc[name] = raw
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// withReturnedArtifact adds a well-formed epoch confirmation for the device to
// the response's authority artifacts, so the response carries something that
// staging it would forward.
func withReturnedArtifact(t *testing.T, body []byte, device string) []byte {
	t.Helper()
	return withMember(t, body, "authority_artifacts", []map[string]string{{
		"artifact_type": string(AuthorityEpochConfirmation),
		"artifact_b64":  base64.StdEncoding.EncodeToString(validEpochConfirmationBytes(device)),
	}})
}

type genuineAcceptance struct {
	Outcome            string `json:"outcome"`
	ArtifactDigest     string `json:"artifact_digest"`
	AuthorityArtifacts []struct {
		ArtifactType string `json:"artifact_type"`
		ArtifactB64  string `json:"artifact_b64"`
	} `json:"authority_artifacts"`
}

// TestAgainstTheEvidenceAuthorityMalformedAcceptance sends a real artifact to
// the real authority, which accepts it, and rewrites that genuine 200 before
// the courier reads it. Each rewrite makes the 200 something other than a clean
// acceptance, an unrecognised outcome under evidence-transport/v2; a rewrite
// that keeps a JSON object also adds a returned artifact. The entry must stay
// queued, never retired, held or resent before its back-off; nothing the
// response carried is staged; and delivery reports degraded, recorded
// unrecognised. The proxy then stops rewriting, and the back-off's retry must
// retire the entry on the authority's genuine answer, once, staging exactly
// what that answer carried.
func TestAgainstTheEvidenceAuthorityMalformedAcceptance(t *testing.T) {
	bind, registration := startEvidenceAuthority(t)
	key := LaneKey{Device: recordDevice(registration), Lane: LaneOf(ArtifactAnchorRegistration)}
	submitted := payloadDigest(registration)

	proxy := &rewritingProxy{}
	server := httptest.NewTLSServer(proxy.handler(bind))
	t.Cleanup(server.Close)

	const backoff = 400 * time.Millisecond
	for _, tc := range []struct {
		name    string
		rewrite func(t *testing.T, header http.Header, body []byte) []byte
	}{
		{"truncated JSON", func(_ *testing.T, _ http.Header, body []byte) []byte {
			return body[:len(body)/2]
		}},
		{"valid JSON without outcome", func(t *testing.T, _ http.Header, body []byte) []byte {
			return withMember(t, withReturnedArtifact(t, body, key.Device), "outcome", nil)
		}},
		{"an outcome outside the vocabulary", func(t *testing.T, _ http.Header, body []byte) []byte {
			return withMember(t, withReturnedArtifact(t, body, key.Device), "outcome", "delivered")
		}},
		{"another artifact's digest", func(t *testing.T, _ http.Header, body []byte) []byte {
			return withMember(t, withReturnedArtifact(t, body, key.Device),
				"artifact_digest", payloadDigest([]byte("not the submitted artifact")))
		}},
		{"a 200 carrying a reason", func(t *testing.T, _ http.Header, body []byte) []byte {
			return withMember(t, withReturnedArtifact(t, body, key.Device), "reason", "conflict")
		}},
		{"a body over the response bound", func(_ *testing.T, _ http.Header, body []byte) []byte {
			// Trailing whitespace keeps the JSON valid, so only the bound
			// can refuse it.
			return append(body, bytes.Repeat([]byte(" "), maxResponseBytes)...)
		}},
		{"a media type other than JSON", func(_ *testing.T, header http.Header, body []byte) []byte {
			header.Set("Content-Type", "text/plain; charset=utf-8")
			return body
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy.mu.Lock()
			proxy.answers = nil
			proxy.mu.Unlock()
			proxy.set(func(header http.Header, body []byte) []byte { return tc.rewrite(t, header, body) })
			t.Cleanup(func() { proxy.set(nil) })

			q := openTestQueue(t, 10, 1<<24)
			courier, err := NewCourier(q, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := courier.Admit(ArtifactAnchorRegistration, registration); err != nil {
				t.Fatalf("admission refused: %v", err)
			}
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: server.URL + ingestPath, ClientID: integrationClientID, Secret: integrationSecret,
				HTTPClient: server.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			sink := &countingSink{}
			logs := &syncBuffer{}
			worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
				RetryInterval: 25 * time.Millisecond, BackoffBase: backoff, MaxBackoff: backoff,
				BlockedReminderInterval: time.Hour, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			running := startWorker(t, worker)

			// The rewritten answer. An outcome is a retirement, a stall or a hold.
			waitFor(t, "an outcome for the rewritten answer", func() bool {
				return len(proxy.seen()) >= 1 && (q.Len() == 0 ||
					len(logs.records(t, stallMessage)) > 0 || len(logs.records(t, blockedMessage)) > 0)
			})
			if q.Len() == 0 {
				t.Fatalf("a malformed 200 retired the entry; %d authority artifacts staged", len(sink.snapshot()))
			}
			first := proxy.seen()
			if len(first) != 1 || !first[0].rewritten || first[0].status != http.StatusOK {
				t.Fatalf("want one rewritten 200, the proxy saw %d answers", len(first))
			}
			var genuine genuineAcceptance
			if err := json.Unmarshal(first[0].body, &genuine); err != nil || genuine.Outcome != "accepted" ||
				genuine.ArtifactDigest != submitted {
				t.Fatalf("the authority did not accept the artifact: %s", first[0].body)
			}
			entry, _ := laneEntry(worker, key)
			status := worker.Status()
			if entry.State != DeviceBackingOff || entry.Held != nil {
				t.Errorf("lane state %q (held %v), want backing_off and not held", entry.State, entry.Held != nil)
			}
			if !status.Degraded || status.Blocked || status.LastError != "malformed_channel_response" {
				t.Errorf("status degraded=%v blocked=%v last_error=%q, want degraded, not blocked, malformed_channel_response",
					status.Degraded, status.Blocked, status.LastError)
			}
			if stored := sink.snapshot(); len(stored) != 0 {
				t.Errorf("%d authority artifacts staged from a malformed 200", len(stored))
			}
			stalls := logs.records(t, stallMessage)
			if len(stalls) != 1 || stalls[0]["reason"] != "unrecognised" ||
				stalls[0]["failure"] != "malformed_channel_response" || stalls[0]["retry"] != "backoff" {
				t.Errorf("stall records %v, want one unrecognised malformed_channel_response back-off", stalls)
			}
			if blocked := logs.records(t, blockedMessage); len(blocked) != 0 {
				t.Errorf("a malformed 200 held the entry: %v", blocked)
			}
			waitFor(t, "a back-off to be scheduled", func() bool { return !worker.nextAttemptIn(key).IsZero() })
			// No immediate resend: well inside the back-off, nothing more was sent.
			time.Sleep(backoff / 4)
			if n := len(proxy.seen()); n != 1 {
				t.Fatalf("the malformed 200 was resent before its back-off: %d requests", n)
			}

			// The authority's genuine answer, on the back-off's retry.
			proxy.set(nil)
			waitFor(t, "the genuine answer to retire the entry", func() bool { return q.Len() == 0 })
			time.Sleep(4 * 25 * time.Millisecond)
			running.stop()
			answers := proxy.seen()
			if len(answers) != 2 || answers[1].rewritten || answers[1].status != http.StatusOK {
				t.Fatalf("want exactly one genuine retry after the malformed 200, the proxy saw %d answers", len(answers))
			}
			var retried genuineAcceptance
			if err := json.Unmarshal(answers[1].body, &retried); err != nil || retried.Outcome != "accepted" ||
				retried.ArtifactDigest != submitted {
				t.Fatalf("the retry was not a genuine acceptance: %s", answers[1].body)
			}
			stored := sink.snapshot()
			if len(stored) != len(retried.AuthorityArtifacts) {
				t.Fatalf("staged %d authority artifacts, the genuine answer carried %d", len(stored), len(retried.AuthorityArtifacts))
			}
			for i, returned := range retried.AuthorityArtifacts {
				payload, err := base64.StdEncoding.DecodeString(returned.ArtifactB64)
				if err != nil || !bytes.Equal(stored[i].Payload, payload) || string(stored[i].Type) != returned.ArtifactType {
					t.Errorf("staged artifact %d is not the genuine answer's", i)
				}
			}
			if _, listed := laneEntry(worker, key); listed {
				t.Error("the lane is still reported after the genuine acceptance")
			}
			if status := worker.Status(); status.Degraded || status.LastError != "" {
				t.Errorf("status degraded=%v last_error=%q after the genuine acceptance", status.Degraded, status.LastError)
			}
			if resumed := logs.records(t, resumedMessage); len(resumed) != 1 {
				t.Errorf("%d resumed records, want 1", len(resumed))
			}
			t.Logf("the authority accepted twice, carrying %d authority artifacts; the courier retired once",
				len(retried.AuthorityArtifacts))
		})
	}
}
