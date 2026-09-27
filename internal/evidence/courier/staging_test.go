// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// stagingSink is an authority sink whose durable write can be made to fail
// and then recover while the worker runs.
type stagingSink struct {
	mu     sync.Mutex
	fail   bool
	stored []AuthorityArtifact
}

func (s *stagingSink) Store(_ context.Context, artifact AuthorityArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("the staging store refused the write")
	}
	s.stored = append(s.stored, artifact)
	return nil
}

func (s *stagingSink) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *stagingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stored)
}

// TestAnAcceptanceIsNotRetiredUntilItsReturnedArtifactsAreStaged drives the
// courier's delivery path: a 200 whose returned receipt cannot be durably
// staged keeps the envelope queued, backs off with the outcome recorded
// unrecognised, and reports the device as not clean; nothing about it reads as
// success. Once staging recovers, the retry stages the receipt and only then
// retires the envelope.
func TestAnAcceptanceIsNotRetiredUntilItsReturnedArtifactsAreStaged(t *testing.T) {
	const device = "site-a-edge-01"
	payload := deviceEnvelope(device, 1)
	digest := payloadDigest(payload)
	receipt := validReceiptBytes(device, 1, 1)
	accepted := "accepted"
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return vectorHTTPResponse(http.StatusOK, "json", &accepted, "", true, false, "", digest, receipt, nil), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := openTestQueue(t, 10, 1<<24)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, payload); err != nil {
		t.Fatal(err)
	}
	sink := &stagingSink{fail: true}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
		RetryInterval: 200 * time.Millisecond, BlockedReminderInterval: time.Hour,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, worker)
	waitFor(t, "the staging failure", func() bool { return worker.Status().LastError != "" })

	if q.Len() != 1 {
		t.Fatal("an acceptance whose receipt was not staged retired the envelope")
	}
	status := worker.Status()
	if !status.Degraded || status.Pending != 1 {
		t.Fatalf("status reads as clean after a staging failure: %+v", status)
	}
	entry, ok := laneState(worker, device)
	if !ok || entry.State != DeviceBackingOff {
		t.Fatalf("device state %+v, want backing off", entry)
	}
	if sink.count() != 0 {
		t.Fatal("a failed staging write recorded the artifact")
	}

	sink.setFail(false)
	waitFor(t, "retirement after staging recovers", func() bool { return q.Len() == 0 })
	running.stop()
	if sink.count() != 1 {
		t.Fatalf("%d artifacts staged, want the one receipt", sink.count())
	}
	if status := worker.Status(); status.Degraded || status.Pending != 0 || len(status.Devices) != 0 {
		t.Fatalf("status after the staged retry: %+v", status)
	}
}

// targetedPins are the targeted cases beside the corpus that this gateway does
// not yet satisfy.
var targetedPins = map[string]vectorPin{
	"a 422 unknown_key carrying a return set stages it, then waits for a handoff": {
		Requirement: "missing_requirement:refusal-return-set",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "state backing_off, 0 staged, 1 queued; want waiting_handoff, 1 staged, 1 queued",
	},
}

// TestARefusalStagesItsReturnSetBeforeItsPolicyApplies covers a rule the
// refusal-policy corpus does not: evidence-transport/v2 carries authority
// artifacts on any response and requires every one to be durably staged
// before the response is acted on. A 422 unknown_key carrying a receipt must
// stage it, then keep the envelope for the next handoff.
func TestARefusalStagesItsReturnSetBeforeItsPolicyApplies(t *testing.T) {
	const name = "a 422 unknown_key carrying a return set stages it, then waits for a handoff"
	checkPinsExist(t, targetedPins, []string{name})
	const device = "site-a-edge-01"
	payload := deviceEnvelope(device, 1)
	digest := payloadDigest(payload)
	refused := "refused"
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return vectorHTTPResponse(http.StatusUnprocessableEntity, "json", &refused, "unknown_key", true, false,
				"", digest, nil, validReceiptBytes(device, 1, 1)), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := openTestQueue(t, 10, 1<<24)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, payload); err != nil {
		t.Fatal(err)
	}
	sink := &stagingSink{}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
		RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, worker)
	waitFor(t, "the refusal", func() bool { return worker.Status().LastError != "" })
	running.stop()
	entry, _ := laneState(worker, device)
	observed := ""
	if entry.State != DeviceWaitingHandoff || sink.count() != 1 || q.Len() != 1 {
		observed = fmt.Sprintf("state %s, %d staged, %d queued; want waiting_handoff, 1 staged, 1 queued",
			entry.State, sink.count(), q.Len())
	}
	checkPin(t, targetedPins, name, observed)
}

// TestADispositionTheCourierCannotCarryDiscardsNothing holds the boundary a
// device's gateway-evidence-carriage/v1 declaration relies on until
// evidence_disposition carriage lands: a response returning a disposition,
// whether it accepts or refuses, is an unrecognised outcome. The envelope
// stays queued in its lane and backs off; nothing is staged, retired or
// routed anywhere else.
func TestADispositionTheCourierCannotCarryDiscardsNothing(t *testing.T) {
	const device = "site-a-edge-01"
	disposition := fmt.Appendf(nil, `{"v":1,"device_id":%q,"scope":"epoch","stop":true,"signature":"ed25519:opaque"}`, device)
	for _, outcome := range []string{"accepted", "refused"} {
		t.Run(outcome, func(t *testing.T) {
			payload := deviceEnvelope(device, 1)
			digest := payloadDigest(payload)
			status := http.StatusOK
			if outcome == "refused" {
				status = http.StatusUnprocessableEntity
			}
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					body := fmt.Sprintf(`{"v":1,"outcome":%q,"reason":"unknown_key","retriable":false,"artifact_digest":%q,"authority_artifacts":[{"artifact_type":"evidence_disposition","artifact_b64":%q}]}`,
						outcome, digest, base64Std(disposition))
					if outcome == "accepted" {
						body = fmt.Sprintf(`{"v":1,"outcome":"accepted","reason":"","retriable":false,"artifact_digest":%q,"authority_artifacts":[{"artifact_type":"evidence_disposition","artifact_b64":%q},{"artifact_type":"delivery_receipt","artifact_b64":%q}]}`,
							digest, base64Std(disposition), base64Std(validReceiptBytes(device, 1, 1)))
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}},
						Body: io.NopCloser(strings.NewReader(body))}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			q := openTestQueue(t, 10, 1<<24)
			if _, err := q.Enqueue(ArtifactDeliveryEnvelope, payload); err != nil {
				t.Fatal(err)
			}
			sink := &stagingSink{}
			worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
				RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
			})
			if err != nil {
				t.Fatal(err)
			}
			running := startWorker(t, worker)
			waitFor(t, "the response answered", func() bool { return worker.Status().LastError != "" })
			running.stop()
			entry, ok := laneState(worker, device)
			if q.Len() != 1 || sink.count() != 0 || !ok || entry.State != DeviceBackingOff {
				t.Fatalf("queued %d, staged %d, state %+v; want the envelope kept, backing off, nothing staged",
					q.Len(), sink.count(), entry)
			}
			if got := worker.Status().LastError; got != "malformed_channel_response" {
				t.Fatalf("last_error = %s, want malformed_channel_response", got)
			}
		})
	}
}
