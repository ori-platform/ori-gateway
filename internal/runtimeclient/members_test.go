// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/contracts/membertest"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

func TestExportPayloadStructsNameOnlyDefinedMembers(t *testing.T) {
	for _, c := range []struct {
		family string
		value  any
	}{
		{contracts.FamilyExportRequest, exportRequest{}},
		{contracts.FamilyExportResponse, exportResponse{}},
	} {
		if undefined := membertest.UndefinedNames(c.family, c.value); len(undefined) != 0 {
			t.Errorf("%s: %T names %v, which the family does not define", c.family, c.value, undefined)
		}
	}
}

// TestEveryExportRequestTheClientPublishesIsAdmitted publishes each export the
// client offers, every parameter set, signed, and holds each request to the
// member rule: a producer never emits a payload the rule refuses.
func TestEveryExportRequestTheClientPublishesIsAdmitted(t *testing.T) {
	b := newFakeBroker()
	client, err := NewMQTTClient(b, WithMessageAuth("current-secret", "", time.Now))
	if err != nil {
		t.Fatal(err)
	}
	var published [][]byte
	b.publishHook = func(_ string, payload []byte) { published = append(published, payload) }
	window := BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10}
	// Nothing answers, so each export publishes its first request and ends at
	// its own short deadline.
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	_, _ = client.Health(short(), HealthRequest{DeviceID: "edge-1"})
	_, _ = client.SensorHistory(short(), SensorHistoryRequest{BoundedWindow: window, SensorID: "current-main", BucketMS: 3_600_000})
	_, _ = client.ActionLog(short(), ActionLogRequest{BoundedWindow: window})
	_, _ = client.TierCDecisionLog(short(), TierCDecisionLogRequest{BoundedWindow: window})
	_, _ = client.ReasoningLog(short(), ReasoningLogRequest{BoundedWindow: window, TierUsed: "gateway", ActionTier: "C", ReasoningStatus: "complete", CorrelationID: "corr-1"})
	if len(published) != 5 {
		t.Fatalf("%d export requests published, want 5", len(published))
	}
	for _, payload := range published {
		if err := contracts.AdmitPayload(contracts.FamilyExportRequest, payload); err != nil {
			t.Errorf("published %s: %v", payload, err)
		}
	}
}

// TestARefusedExportPlaintextChangesNothing answers an encrypted export with
// a plaintext the member rule refuses, for every reason, then with a clean
// one. The refused plaintext is logged with its reason, records no replay key
// and leaves the request pending, so the clean response completes the export.
func TestARefusedExportPlaintextChangesNothing(t *testing.T) {
	plaintexts := map[string][]byte{
		contracts.ReasonInvalidUnicode:    []byte("{\"request_id\":\"r\",\"device_id\":\"edge-1\",\"export_type\":\"action_log\",\"x\":\"\xff\"}"),
		contracts.ReasonMalformedJSON:     []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log",`),
		contracts.ReasonNestingTooDeep:    []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log","x":` + strings.Repeat("[", 32) + strings.Repeat("]", 32) + `}`),
		contracts.ReasonTrailingData:      []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log"} x`),
		contracts.ReasonDuplicateMember:   []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log","items":[],"items":[]}`),
		contracts.ReasonCaseVariantMember: []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log","COMPLETE":true}`),
		contracts.ReasonNotAnObject:       []byte(`[1]`),
	}
	for reason, bad := range plaintexts {
		t.Run(reason, func(t *testing.T) {
			now := time.UnixMilli(1787000003000)
			b := newFakeBroker()
			logs := &lockedBuffer{}
			c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) { return "r", nil }),
				WithMessageAuth("current-secret", "", func() time.Time { return now }),
				WithMessageEncryption("current-secret"), WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
			if err != nil {
				t.Fatal(err)
			}
			var replayAfterBad, pendingAfterBad int
			b.publishHook = func(_ string, payload []byte) {
				var req exportRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Error(err)
					return
				}
				b.respondRaw(req.DeviceID, req.RequestID, sealedExport(t, bad, now, 0))
				replayAfterBad = c.verifier.ReplayEntries()
				c.mu.Lock()
				pendingAfterBad = len(c.pending) - len(c.pending["r"].ch)
				c.mu.Unlock()
				clean := []byte(`{"request_id":"r","device_id":"edge-1","export_type":"action_log","items":[],"next_page_token":"","complete":true,"error":null}`)
				b.respondRaw(req.DeviceID, req.RequestID, sealedExport(t, clean, now, 1))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := c.ActionLog(ctx, ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}}); err != nil {
				t.Fatalf("export = %v, want completed by the clean response", err)
			}
			if replayAfterBad != 0 || pendingAfterBad != 1 {
				t.Fatalf("after the refused plaintext: %d replay entries, %d pending and unanswered; want 0 and 1", replayAfterBad, pendingAfterBad)
			}
			if !strings.Contains(logs.String(), "reason="+reason) {
				t.Fatalf("refusal not logged with %s:\n%s", reason, logs.String())
			}
		})
	}
}

// sealedExport encrypts plaintext as the runtime does, under the export
// response envelope, and signs the outer payload.
func sealedExport(t *testing.T, plaintext []byte, now time.Time, n byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	nonce[0] += n
	encryptor, err := mqttauth.NewEncryptor("current-secret")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := encryptor.Seal(plaintext, map[string]string{"request_id": "r", "device_id": "edge-1", "export_type": "action_log"},
		contracts.ExportResponseMessageType, nonce)
	if err != nil {
		t.Fatal(err)
	}
	return signedExportPayload(t, sealed, "current-secret", now.Add(time.Duration(n)*time.Millisecond))
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
