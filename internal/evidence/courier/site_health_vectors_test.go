// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

// siteHealthVectors is the gateway-evidence-carriage/v1 site-health corpus,
// decoded strictly: a section or field the corpus gains that this harness does
// not model fails the test. The projection cases are the oracle's, in the
// command package, which reads them from the same file.
type siteHealthVectors struct {
	Contract       string            `json:"contract"`
	Note           string            `json:"note"`
	Cases          []json.RawMessage `json:"cases"`
	LastErrorCases []struct {
		Name      string `json:"name"`
		Status    int    `json:"status"`
		Reason    string `json:"reason"`
		LastError string `json:"last_error"`
		Valid     bool   `json:"valid"`
	} `json:"last_error_cases"`
	IncidentSequences []struct {
		Name  string `json:"name"`
		Steps []struct {
			Event         string `json:"event"`
			DeviceID      string `json:"device_id"`
			AnchorEpochID string `json:"anchor_epoch_id"`
			ArtifactType  string `json:"artifact_type"`
			RefusalStatus int    `json:"refusal_status"`
			Reason        string `json:"reason"`
			Digest        string `json:"digest"`
			AtMS          int64  `json:"at_ms"`
			// Key is an acknowledge step's incident coalescing key.
			Key    []json.RawMessage `json:"key"`
			Expect json.RawMessage   `json:"expect"`
		} `json:"steps"`
	} `json:"incident_sequences"`
}

func loadSiteHealthVectors(t *testing.T) siteHealthVectors {
	t.Helper()
	raw, err := specvectors.Read("gateway-evidence-carriage/vectors/site-health-evidence-delivery.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc siteHealthVectors
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("decode the site-health corpus: %v", err)
	}
	return doc
}

// lastErrorPins are the last_error cases this courier does not yet satisfy.
var lastErrorPins = map[string]vectorPin{
	"a 503 retention_capacity_unavailable is recorded as authority_retention_capacity_unavailable": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "recorded channel_refused, want authority_retention_capacity_unavailable",
	},
	"a 507 retention_capacity_exhausted is recorded as channel_capacity_exhausted": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "recorded malformed_channel_response, want channel_capacity_exhausted",
	},
	"a generic 503 unavailable is recorded as channel_unavailable": {
		Requirement: "missing_requirement:last-error-vocabulary",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "recorded channel_refused, want channel_unavailable",
	},
}

// TestLastErrorVectors drives each refusal through the courier and reads the
// last_error it records: a conforming case must record exactly its value, and
// a non-conforming case's value must never be recorded for that refusal.
func TestLastErrorVectors(t *testing.T) {
	doc := loadSiteHealthVectors(t)
	names := make([]string, 0, len(doc.LastErrorCases))
	for _, tc := range doc.LastErrorCases {
		names = append(names, tc.Name)
	}
	checkPinsExist(t, lastErrorPins, names)
	const device = "site-a-edge-01"
	for _, tc := range doc.LastErrorCases {
		t.Run(tc.Name, func(t *testing.T) {
			outcome := "refused"
			if tc.Status == http.StatusServiceUnavailable {
				outcome = "pending"
			}
			payload := deviceCheckpoint(device, 1)
			digest := payloadDigest(payload)
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return vectorHTTPResponse(tc.Status, "json", &outcome, tc.Reason, true, false, "", digest, nil, nil), nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			q := openTestQueue(t, 10, 1<<24)
			if _, err := q.Enqueue(ArtifactCheckpoint, payload); err != nil {
				t.Fatal(err)
			}
			worker, err := NewDeliveryWorker(q, channel, &stagingSink{}, DeliveryWorkerOptions{
				RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
			})
			if err != nil {
				t.Fatal(err)
			}
			running := startWorker(t, worker)
			waitFor(t, "the refusal recorded", func() bool { return worker.Status().LastError != "" })
			running.stop()
			recorded := worker.Status().LastError
			observed := ""
			switch {
			case tc.Valid && recorded != tc.LastError:
				observed = fmt.Sprintf("recorded %s, want %s", recorded, tc.LastError)
			case !tc.Valid && recorded == tc.LastError:
				observed = fmt.Sprintf("recorded the non-conforming %s", recorded)
			}
			checkPin(t, lastErrorPins, tc.Name, observed)
		})
	}
}

// incidentSequencePins are the incident sequences this courier does not yet
// satisfy.
var incidentSequencePins = map[string]vectorPin{
	"79 distinct refusals coalesce into one incident with count 79, every member enumerable": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
	"a different epoch, type or class never coalesces": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
	"a new refusal after acknowledgement reopens the incident with its first fields unchanged": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
	"acknowledgement changes alert visibility only": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
	"an identical retry never increments the count or moves the latest fields": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
	"restart preserves aggregation": {
		Requirement: "missing_requirement:incidents",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/103",
		Observed:    "no incident reported; the refusal was recorded malformed_channel_response and 1 artifact stays queued",
	},
}

// TestIncidentSequences drives each sequence's first archival through the
// courier: a retained refusal of an artifact of that device, type, status and
// reason, then reads whether the courier reports an incident for it.
func TestIncidentSequences(t *testing.T) {
	doc := loadSiteHealthVectors(t)
	names := make([]string, 0, len(doc.IncidentSequences))
	for _, seq := range doc.IncidentSequences {
		names = append(names, seq.Name)
	}
	checkPinsExist(t, incidentSequencePins, names)
	for _, seq := range doc.IncidentSequences {
		t.Run(seq.Name, func(t *testing.T) {
			if len(seq.Steps) == 0 || seq.Steps[0].Event != "archive" {
				t.Fatalf("a sequence begins with an archival")
			}
			step := seq.Steps[0]
			payload := deviceCheckpoint(step.DeviceID, 1)
			kind := ArtifactCheckpoint
			switch step.ArtifactType {
			case string(ArtifactCheckpoint):
			case string(ArtifactDeliveryEnvelope):
				payload, kind = deviceEnvelope(step.DeviceID, 1), ArtifactDeliveryEnvelope
			case string(ArtifactAnchorRegistration):
				payload, kind = deviceRegistration(step.DeviceID, 1), ArtifactAnchorRegistration
			default:
				t.Fatalf("artifact type %q the harness cannot express", step.ArtifactType)
			}
			digest := payloadDigest(payload)
			retained := "refused_retained"
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return vectorHTTPResponse(step.RefusalStatus, "json", &retained, step.Reason, true, false, "", digest, nil, nil), nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			q := openTestQueue(t, 10, 1<<24)
			if _, err := q.Enqueue(kind, payload); err != nil {
				t.Fatal(err)
			}
			worker, err := NewDeliveryWorker(q, channel, &stagingSink{}, DeliveryWorkerOptions{
				RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
			})
			if err != nil {
				t.Fatal(err)
			}
			running := startWorker(t, worker)
			waitFor(t, "the retained refusal answered", func() bool { return worker.Status().LastError != "" })
			running.stop()
			// DeliveryStatus has no incidents: the courier cannot report one.
			observed := fmt.Sprintf("no incident reported; the refusal was recorded %s and %d artifact stays queued",
				worker.Status().LastError, q.Len())
			checkPin(t, incidentSequencePins, seq.Name, observed)
		})
	}
}
