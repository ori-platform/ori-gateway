// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type outcomeClass string

const (
	classAccepted outcomeClass = "accepted"
	classHold     outcomeClass = "terminal hold"
	classHandoff  outcomeClass = "handoff only"
	classBackoff  outcomeClass = "back-off"
	classInterval outcomeClass = "interval retry"
)

// policyRows is the documented policy: every status the contracts name, and one
// they do not, with admitted and unadmitted reasons. The retriable flag is
// crossed with every row by the test, and must never change the class.
var policyRows = []struct {
	status int
	reason string
	want   outcomeClass
}{
	{http.StatusOK, "", classAccepted},
	{http.StatusBadRequest, "malformed", classHold},
	{http.StatusBadRequest, "unknown_key", classHold},
	{http.StatusBadRequest, "authority.internal", classHold},
	{http.StatusUnauthorized, "stale", classHandoff},
	{http.StatusForbidden, "not_authorized", classHandoff},
	{http.StatusConflict, "conflict", classHold},
	{http.StatusConflict, "unknown_key", classHold},
	{http.StatusUnprocessableEntity, "bad_authenticator", classHold},
	{http.StatusUnprocessableEntity, "unknown_key", classHandoff},
	{http.StatusUnprocessableEntity, "authority.internal", classHold},
	{http.StatusTooManyRequests, "rate_limited", classBackoff},
	{http.StatusServiceUnavailable, "unavailable", classBackoff},
	{http.StatusInternalServerError, "busy", classInterval},
	{http.StatusTeapot, "conflict", classInterval},
}

func policyResponse(status int, reason string, retriable bool, digest string) *http.Response {
	body := map[string]any{"v": 1, "reason": reason, "retriable": retriable, "authority_artifacts": []any{}}
	switch {
	case status == http.StatusOK:
		body["outcome"], body["reason"], body["artifact_digest"] = "accepted", "", digest
	case status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable || status >= 500:
		body["outcome"], body["artifact_digest"] = "pending", digest
	case status == http.StatusUnauthorized:
		body["outcome"] = "refused"
	default:
		body["outcome"], body["artifact_digest"] = "refused", digest
	}
	raw, _ := json.Marshal(body)
	header := http.Header{"Content-Type": []string{"application/json"}}
	if status == http.StatusTooManyRequests {
		header.Set("Retry-After", "3600")
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(string(raw)))}
}

func classify(result DeliveryResult, err error) outcomeClass {
	switch {
	case err != nil:
		return classInterval
	case result.Accepted:
		return classAccepted
	case result.Retriable && result.ReceiverState:
		return "contradictory"
	case result.Retriable:
		return classBackoff
	case result.ReceiverState:
		return classHandoff
	default:
		return classHold
	}
}

// workerFailure is what the worker's status must report for each class.
var workerFailure = map[outcomeClass]string{
	classHold:     "channel_permanent_refusal",
	classHandoff:  "channel_receiver_state_refusal",
	classBackoff:  "channel_refused",
	classInterval: "malformed_channel_response",
}

func TestRefusalPolicyIsOneDecisionIndependentOfTheRetriableFlag(t *testing.T) {
	payload := checkpointBytes(1)
	digest := payloadDigest(payload)
	for _, row := range policyRows {
		for _, retriable := range []bool{false, true} {
			name := fmt.Sprintf("%d_%s_retriable_%v", row.status, row.reason, retriable)
			t.Run(name, func(t *testing.T) {
				want := row.want
				if row.status == http.StatusOK && retriable {
					// An acceptance marked retriable is malformed and never
					// retires the artifact.
					want = classInterval
				}
				var calls atomic.Int32
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return policyResponse(row.status, row.reason, retriable, digest), nil
				})}
				channel, err := NewHTTPChannel(HTTPChannelOptions{
					Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
					Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: client,
				})
				if err != nil {
					t.Fatal(err)
				}
				got := classify(channel.Deliver(context.Background(), QueuedArtifact{Type: ArtifactCheckpoint, Payload: payload}))
				if got != want {
					t.Fatalf("channel classified %d %q retriable=%v as %s, policy says %s", row.status, row.reason, retriable, got, want)
				}

				// The same row through the worker: the status it reports, and
				// whether the timer retries it.
				calls.Store(0)
				q := openTestQueue(t, 10, 1<<20)
				if _, err := q.Enqueue(ArtifactCheckpoint, payload); err != nil {
					t.Fatal(err)
				}
				worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
					RetryInterval: time.Millisecond, BlockedReminderInterval: time.Millisecond,
					Logger: slog.New(slog.DiscardHandler),
				})
				if err != nil {
					t.Fatal(err)
				}
				running := startWorker(t, worker)
				if want == classAccepted {
					waitFor(t, "delivery", func() bool { return q.Len() == 0 })
					return
				}
				waitFor(t, "a failure", func() bool { return worker.Status().LastError != "" })
				time.Sleep(40 * time.Millisecond)
				running.stop()
				status := worker.Status()
				if status.LastError != workerFailure[want] || status.Blocked != (want == classHold) {
					t.Fatalf("worker reported %q blocked=%v for %s", status.LastError, status.Blocked, want)
				}
				attempts := calls.Load()
				switch want {
				case classHold, classHandoff:
					if attempts != 1 {
						t.Fatalf("%s was attempted %d times by the timer, want 1", want, attempts)
					}
				case classBackoff:
					if row.status == http.StatusTooManyRequests && attempts != 1 {
						t.Fatalf("Retry-After ignored: %d attempts", attempts)
					}
				case classInterval:
					if attempts < 3 {
						t.Fatalf("interval retry made %d attempts", attempts)
					}
				}
			})
		}
	}
}
