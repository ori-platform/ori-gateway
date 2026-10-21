// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/reporting"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
	"github.com/ori-platform/ori-gateway/internal/site"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const healthTransitionSecret = "current-secret"

type healthTransitionCase struct {
	Name              string          `json:"name"`
	Note              string          `json:"note"`
	RequestedDeviceID string          `json:"requested_device_id"`
	Latched           bool            `json:"latched"`
	Item              json.RawMessage `json:"item"`
	Expect            struct {
		Severity     string `json:"severity"`
		LatchedAfter bool   `json:"latched_after"`
	} `json:"expect"`
}

// exportRuntime is a runtime answering every export request the client
// publishes, as its runtime does: each response signed under the shared
// secret for the device and request the request named, a sensitive export's
// encrypted when the site encrypts, and the health export carrying item.
type exportRuntime struct {
	t        *testing.T
	now      time.Time
	encrypt  bool
	mu       sync.Mutex
	handlers map[string]broker.MessageHandler
	item     any
}

func (r *exportRuntime) Subscribe(_ context.Context, topic string, _ byte, handler broker.MessageHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic] = handler
	return nil
}

func (r *exportRuntime) Publish(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
	var req struct {
		RequestID  string `json:"request_id"`
		DeviceID   string `json:"device_id"`
		ExportType string `json:"export_type"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: healthTransitionSecret, Now: func() time.Time { return r.now }})
	if err != nil {
		return err
	}
	if _, err := verifier.VerifyJSON(payload, contracts.ExportRequestMessageType, req.DeviceID, req.RequestID); err != nil {
		r.t.Errorf("export request not signed: %v", err)
		return err
	}
	items := []any{}
	if req.ExportType == "health" {
		r.mu.Lock()
		items = []any{r.item}
		r.mu.Unlock()
	}
	resp := map[string]any{
		"request_id": req.RequestID, "device_id": req.DeviceID, "export_type": req.ExportType,
		"items": items, "next_page_token": "", "complete": true, "error": nil,
	}
	if req.ExportType != "health" && r.encrypt {
		encryptor, err := mqttauth.NewEncryptor(healthTransitionSecret)
		if err != nil {
			return err
		}
		nonce := make([]byte, 12)
		_, _ = rand.Read(nonce)
		if resp, err = encryptor.Encrypt(resp, contracts.ExportResponseMessageType, nonce); err != nil {
			return err
		}
	}
	auth, err := mqttauth.Sign(resp, contracts.ExportResponseMessageType, req.DeviceID, req.RequestID, r.now.UnixMilli(), healthTransitionSecret)
	if err != nil {
		return err
	}
	resp["auth"] = auth
	encoded, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	topic, err := contracts.ExportResponseTopicFilter(req.DeviceID)
	if err != nil {
		return err
	}
	r.mu.Lock()
	handler := r.handlers[topic]
	r.mu.Unlock()
	handler("ori/"+req.DeviceID+"/export/response/"+req.RequestID, encoded)
	return nil
}

func decodeVectorItem(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var item any
	if err := decoder.Decode(&item); err != nil {
		t.Fatal(err)
	}
	return item
}

// TestHealthTransitionReachesSiteHealthAndTheWeeklyReport reads every case of
// the gateway-api/v2 health transition corpus, and each producer baseline,
// through the runtime export client the gateway builds from its secrets, with
// message encryption off and on: a verified health export read by
// runtimeclient.MQTTClient.Health, then into the site posture fetch and the
// site health projection, and into the weekly report's input. Each consumer
// reads the case's severity: site health raises the node, never lowers it,
// critical to critical and anything but ok to degraded, and the weekly
// report's runtime health carries it as read.
func TestHealthTransitionReachesSiteHealthAndTheWeeklyReport(t *testing.T) {
	raw, err := specvectors.Read("gateway-api/vectors/health-transition-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Baselines string                 `json:"baselines"`
		Cases     []healthTransitionCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	rawBaselines, err := specvectors.Read("gateway-api/vectors/" + corpus.Baselines)
	if err != nil {
		t.Fatal(err)
	}
	var baselines map[string]json.RawMessage
	if err := json.Unmarshal(rawBaselines, &baselines); err != nil {
		t.Fatal(err)
	}
	delete(baselines, "comment")
	cases := corpus.Cases
	for name, item := range baselines {
		baseline := healthTransitionCase{Name: "baseline_" + name, RequestedDeviceID: "dev-01", Item: item}
		baseline.Expect.Severity = runtimeclient.HealthStatusOK
		cases = append(cases, baseline)
	}
	if len(corpus.Cases) == 0 || len(baselines) != 2 {
		t.Fatalf("corpus has %d cases and %d baselines", len(corpus.Cases), len(baselines))
	}
	marker := map[string]any{"projection": "runtime-health/v4/remote", "status": "ok"}
	now := time.UnixMilli(1_787_000_003_000)
	nodeStatus := map[string]string{
		runtimeclient.HealthStatusOK:       site.SiteStatusHealthy,
		runtimeclient.HealthStatusDegraded: site.SiteStatusDegraded,
		runtimeclient.HealthStatusUnknown:  site.SiteStatusDegraded,
		runtimeclient.HealthStatusCritical: site.SiteStatusCritical,
	}

	for _, encrypt := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("encrypted=%t/%s", encrypt, tc.Name), func(t *testing.T) {
				opts, err := runtimeClientOptionsFromSecrets(config.GatewayEncryptionConfig{Enabled: encrypt},
					gatewayAuthSecrets{Enabled: true, CurrentSecret: healthTransitionSecret}, func() time.Time { return now })
				if err != nil {
					t.Fatal(err)
				}
				runtime := &exportRuntime{t: t, now: now, encrypt: encrypt, handlers: map[string]broker.MessageHandler{}}
				logger := slog.New(slog.NewTextHandler(io.Discard, nil))
				rc, err := runtimeclient.NewMQTTClient(runtime, append(opts, runtimeclient.WithLogger(logger), instantExportPacing())...)
				if err != nil {
					t.Fatal(err)
				}
				if tc.Latched {
					runtime.item = marker
					if _, err := rc.Health(context.Background(), runtimeclient.HealthRequest{DeviceID: tc.RequestedDeviceID}); err != nil {
						t.Fatal(err)
					}
				}
				runtime.item = decodeVectorItem(t, tc.Item)

				registry := site.NewRegistry()
				registry.Upsert(site.NodeHeartbeat{DeviceID: tc.RequestedDeviceID, Status: site.NodeStatusHealthy, LastSeenMS: now.UnixMilli()})
				fetchAndUpsertPosture(context.Background(), rc, registry, tc.RequestedDeviceID, logger)
				projection := site.NewProjector(registry, site.ProjectOptions{
					ExpectedDeviceIDs: []string{tc.RequestedDeviceID},
					NodeTTLMS:         60_000,
				}).Project(now, site.GatewayView{Status: site.SiteStatusHealthy})
				if len(projection.Nodes) != 1 || projection.Nodes[0].Posture == nil {
					t.Fatalf("no posture projected: %#v", projection.Nodes)
				}
				node := projection.Nodes[0]
				if node.Posture.ExportSeverity != tc.Expect.Severity {
					t.Fatalf("%s (%s): site posture severity %q, want %q", tc.Name, tc.Note, node.Posture.ExportSeverity, tc.Expect.Severity)
				}
				if want := nodeStatus[tc.Expect.Severity]; node.Status != want || projection.Status != want {
					t.Fatalf("%s: node %q and site %q, want %q", tc.Name, node.Status, projection.Status, want)
				}
				encoded, err := json.Marshal(projection)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(encoded), "health_socket_path") || strings.Contains(string(encoded), "degradation_reasons\":[\"") {
					t.Fatalf("site health carries export content: %s", encoded)
				}

				provider := &fakeReportingProvider{}
				generator, err := reporting.NewWeeklyReportGenerator(rc, provider, reporting.WithNow(func() time.Time { return now }))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := generator.Generate(context.Background(), reporting.WeeklyReportRequest{
					DeviceID: tc.RequestedDeviceID, CustomerName: "Site A Ltd", SiteName: "Site A",
					Timezone: "Africa/Lagos", SensorIDs: []string{"current-main"},
				}); err != nil {
					t.Fatal(err)
				}
				if len(provider.inputs) != 1 || provider.inputs[0].Health.Status != tc.Expect.Severity {
					t.Fatalf("%s: weekly report input %#v, want severity %q", tc.Name, provider.inputs, tc.Expect.Severity)
				}
			})
		}
	}
}
