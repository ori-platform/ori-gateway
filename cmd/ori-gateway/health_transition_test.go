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
	"path/filepath"
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
	// item is what the health export carries, or, when fail is set, a
	// runtime error instead. answer switches both at once, starting a new
	// generation; served counts the health exports answered in the current
	// generation, so an export that read the previous answer never counts.
	item       any
	fail       bool
	generation int
	served     int
}

// answer sets what every health export answered from now on carries: item,
// or a runtime error when fail is set.
func (r *exportRuntime) answer(item any, fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.item, r.fail = item, fail
	r.generation++
	r.served = 0
}

// servedSinceAnswer reports how many health exports were answered with the
// current answer.
func (r *exportRuntime) servedSinceAnswer() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.served
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
	resp := map[string]any{
		"request_id": req.RequestID, "device_id": req.DeviceID, "export_type": req.ExportType,
		"items": []any{}, "next_page_token": "", "complete": true, "error": nil,
	}
	if req.ExportType == "health" {
		r.mu.Lock()
		item, fail, generation := r.item, r.fail, r.generation
		r.mu.Unlock()
		resp["items"] = []any{item}
		if fail {
			resp["items"], resp["error"] = []any{}, "health unavailable"
		}
		// Counted once the response has been handed to the client, and only
		// while its answer is still the current one.
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.generation == generation {
				r.served++
			}
		}()
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
					runtime.answer(marker, false)
					if _, err := rc.Health(context.Background(), runtimeclient.HealthRequest{DeviceID: tc.RequestedDeviceID}); err != nil {
						t.Fatal(err)
					}
				}
				runtime.answer(decodeVectorItem(t, tc.Item), false)

				registry, _ := newTestReceiptRegistry(now.UnixMilli())
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

// TestACriticalExportSurvivesHeartbeatsAndAFailedFetch runs a node through
// the real heartbeat handler and the real export client: a heartbeat, an
// export that reads critical, another heartbeat, and a fetch that fails. The
// node and the site stay critical throughout. Then a heartbeat, a critical
// export, a heartbeat and a later export that reads ok: the raise clears.
func TestACriticalExportSurvivesHeartbeatsAndAFailedFetch(t *testing.T) {
	raw, err := specvectors.Read("gateway-api/vectors/health-transition-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Cases []healthTransitionCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	items := map[string]json.RawMessage{}
	for _, tc := range corpus.Cases {
		items[tc.Name] = tc.Item
	}
	critical, healthy := items["later_critical_true"], items["producer_later_exact"]
	if critical == nil || healthy == nil {
		t.Fatal("corpus lacks the critical or the healthy case")
	}

	now := time.UnixMilli(1_787_000_003_000)
	secrets := gatewayAuthSecrets{Enabled: true, CurrentSecret: healthTransitionSecret}
	registry, _ := newTestReceiptRegistry(now.UnixMilli())
	heartbeats, err := runtimeHeartbeatHandlerFromSecrets(secrets, registry, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signedAt := now.UnixMilli()
	beat := func() {
		t.Helper()
		signedAt++
		hb := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: site.NodeStatusHealthy, LastSeenMS: now.UnixMilli(), ActiveTriggers: []string{}}
		auth, err := mqttauth.Sign(hb, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAt, healthTransitionSecret)
		if err != nil {
			t.Fatal(err)
		}
		hb.Auth = &auth
		if err := heartbeats.Handle("ori/dev-01/runtime/heartbeat", mustJSON(t, hb)); err != nil {
			t.Fatalf("heartbeat refused: %v", err)
		}
	}
	opts, err := runtimeClientOptionsFromSecrets(config.GatewayEncryptionConfig{}, secrets, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	runtime := &exportRuntime{t: t, now: now, handlers: map[string]broker.MessageHandler{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rc, err := runtimeclient.NewMQTTClient(runtime, append(opts, runtimeclient.WithLogger(logger), instantExportPacing())...)
	if err != nil {
		t.Fatal(err)
	}
	fetch := func(item json.RawMessage, fail bool) {
		t.Helper()
		var decoded any
		if item != nil {
			decoded = decodeVectorItem(t, item)
		}
		runtime.answer(decoded, fail)
		fetchAndUpsertPosture(context.Background(), rc, registry, "dev-01", logger)
	}
	expect := func(step, want string) {
		t.Helper()
		h := site.NewProjector(registry, site.ProjectOptions{ExpectedDeviceIDs: []string{"dev-01"}, NodeTTLMS: 60_000}).
			Project(now, site.GatewayView{Status: site.SiteStatusHealthy})
		if h.Nodes[0].Status != want || h.Status != want {
			t.Fatalf("%s: node %q site %q, want %q", step, h.Nodes[0].Status, h.Status, want)
		}
	}

	beat()
	expect("first heartbeat", site.SiteStatusHealthy)
	fetch(critical, false)
	expect("critical export", site.SiteStatusCritical)
	beat()
	expect("heartbeat after the critical export", site.SiteStatusCritical)
	fetch(nil, true)
	expect("failed fetch", site.SiteStatusCritical)
	beat()
	expect("heartbeat after the failed fetch", site.SiteStatusCritical)

	fetch(critical, false)
	beat()
	expect("heartbeat after a second critical export", site.SiteStatusCritical)
	fetch(healthy, false)
	expect("a later ok export", site.SiteStatusHealthy)
	beat()
	expect("heartbeat after the ok export", site.SiteStatusHealthy)
}

// TestSiteExportSeverityNeverReadsAnUnknownValueAsOK maps every value the
// runtime client returns, and values it never returns, into the site
// posture: only ok is ok, and a value the client never returns is unknown.
func TestSiteExportSeverityNeverReadsAnUnknownValueAsOK(t *testing.T) {
	for status, want := range map[string]string{
		runtimeclient.HealthStatusOK:       site.ExportSeverityOK,
		runtimeclient.HealthStatusDegraded: site.ExportSeverityDegraded,
		runtimeclient.HealthStatusCritical: site.ExportSeverityCritical,
		runtimeclient.HealthStatusUnknown:  site.ExportSeverityUnknown,
		"":                                 site.ExportSeverityUnknown,
		"healthy":                          site.ExportSeverityUnknown,
		"OK":                               site.ExportSeverityUnknown,
	} {
		if got := sitePostureFromHealth(runtimeclient.HealthSnapshot{Status: status}).ExportSeverity; got != want {
			t.Errorf("status %q: posture severity %q, want %q", status, got, want)
		}
	}
}

// TestEvictStaleRuntimeNodesReportsASilentCriticalExport lets a node whose
// heartbeats were healthy, but whose health export last read critical, go
// silent beside a node whose heartbeat was critical. Each is retained and
// reported once with its own line, naming only the device and the times.
func TestEvictStaleRuntimeNodesReportsASilentCriticalExport(t *testing.T) {
	registry, clock := newTestReceiptRegistry(2000)
	registry.Upsert(site.NodeHeartbeat{DeviceID: "export", Status: site.NodeStatusHealthy, LastSeenMS: 1000})
	registry.UpsertPosture("export", site.SiteNodePosture{ExportSeverity: site.ExportSeverityCritical})
	registry.Upsert(site.NodeHeartbeat{DeviceID: "beat", Status: site.NodeStatusCritical, LastSeenMS: 1000})
	clock.advance(time.Second)
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	go evictStaleRuntimeNodes(t.Context(), registry, time.Millisecond, logger)

	lines := []string{
		`level=ERROR msg="runtime node silent after a critical health export; kept critical until its export reads otherwise" device_id=export gateway_seen_ms=2000 last_seen_ms=1000`,
		`level=ERROR msg="runtime node silent after a critical heartbeat; kept critical until it reports again" device_id=beat gateway_seen_ms=2000 last_seen_ms=1000`,
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.Count(logs.String(), "level=ERROR") < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	logged := logs.String()
	for _, want := range lines {
		if n := strings.Count(logged, want+"\n"); n != 1 {
			t.Fatalf("log line %q appears %d times, want once:\n%s", want, n, logged)
		}
	}
	if n := strings.Count(logged, "\n"); n != len(lines) {
		t.Fatalf("%d log lines, want only the two retentions:\n%s", n, logged)
	}
	if len(registry.Snapshot()) != 2 {
		t.Fatalf("both critical nodes should be retained: %#v", registry.Snapshot())
	}
}

// TestARunningGatewayKeepsACriticalExportThroughAFailedFetch drives a running
// gateway: signed runtime heartbeats through its heartbeat callback, each
// starting a posture fetch through the runtime export client it builds from
// its own options. A critical export makes the node critical; a heartbeat
// whose fetch then fails leaves it critical; a later ok export clears it.
func TestARunningGatewayKeepsACriticalExportThroughAFailedFetch(t *testing.T) {
	raw, err := specvectors.Read("gateway-api/vectors/health-transition-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Cases []healthTransitionCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	items := map[string]json.RawMessage{}
	for _, tc := range corpus.Cases {
		items[tc.Name] = tc.Item
	}

	t.Setenv("GATEWAY_SECRET_CURRENT", healthTransitionSecret)
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_SECRET_CURRENT"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: "127.0.0.1:0"}
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	now := deps.now()
	registry, _ := newTestReceiptRegistry(now.UnixMilli())
	deps.newSiteRegistry = func() *site.Registry { return registry }
	runtime := &exportRuntime{t: t, now: now, handlers: map[string]broker.MessageHandler{}}
	runtime.answer(decodeVectorItem(t, items["later_critical_true"]), false)
	deps.newRuntimeClient = func(_ brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return runtimeclient.NewMQTTClient(runtime, append(opts, instantExportPacing())...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not subscribe")
	}
	var handler broker.MessageHandler
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; registry %#v", what, registry.Snapshot())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("the heartbeat callback", func() bool {
		handler = fb.handlerFor("ori/dev-01/runtime/heartbeat")
		return handler != nil
	})
	signedAt := now.UnixMilli()
	beat := func() {
		signedAt++
		hb := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: site.NodeStatusHealthy, LastSeenMS: now.UnixMilli(), ActiveTriggers: []string{}}
		auth, err := mqttauth.Sign(hb, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAt, healthTransitionSecret)
		if err != nil {
			t.Fatal(err)
		}
		hb.Auth = &auth
		handler("ori/dev-01/runtime/heartbeat", mustJSON(t, hb))
	}
	// beatUntilFetched sends heartbeats until the runtime has answered n
	// fetches with its current answer: a heartbeat while a fetch is in flight
	// starts none, and a fetch that read an earlier answer never counts.
	beatUntilFetched := func(what string, n int) {
		t.Helper()
		waitFor(what, func() bool {
			beat()
			time.Sleep(5 * time.Millisecond)
			return runtime.servedSinceAnswer() >= n
		})
	}
	severity := func() string {
		snapshot := registry.Snapshot()
		if len(snapshot) != 1 {
			return ""
		}
		return snapshot[0].ExportSeverity
	}
	expect := func(step, want string) {
		t.Helper()
		h := site.NewProjector(registry, site.ProjectOptions{ExpectedDeviceIDs: []string{"dev-01"}, NodeTTLMS: 60_000}).
			Project(now, site.GatewayView{Status: site.SiteStatusHealthy})
		if h.Nodes[0].Status != want || h.Status != want {
			t.Fatalf("%s: node %q site %q, want %q", step, h.Nodes[0].Status, h.Status, want)
		}
	}

	beatUntilFetched("a critical export", 1)
	waitFor("the critical reading", func() bool { return severity() == site.ExportSeverityCritical })
	expect("critical export", site.SiteStatusCritical)

	// A second failed fetch starts only once the first has finished, so
	// whatever the first did to the registry has landed.
	runtime.answer(nil, true)
	beatUntilFetched("two failed fetches", 2)
	expect("heartbeats and failed fetches after the critical export", site.SiteStatusCritical)

	runtime.answer(decodeVectorItem(t, items["producer_later_exact"]), false)
	beatUntilFetched("a later ok export", 1)
	waitFor("the ok reading", func() bool { return severity() == site.ExportSeverityOK })
	expect("a later ok export", site.SiteStatusHealthy)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not stop")
	}
}
