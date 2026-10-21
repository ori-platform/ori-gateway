// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const (
	healthTransitionCorpusPath    = "gateway-api/vectors/health-transition-v2.json"
	healthTransitionBaselinesPath = "gateway-api/vectors/health-transition-baselines-v2.json"
	healthTransitionSecret        = "current-secret"
)

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

type healthTransitionCorpus struct {
	Contract   string                 `json:"contract"`
	Version    int                    `json:"version"`
	Baselines  string                 `json:"baselines"`
	Severities []string               `json:"severities"`
	Cases      []healthTransitionCase `json:"cases"`
}

// loadHealthTransition reads the vendored corpus and its producer baselines.
func loadHealthTransition(t *testing.T) (healthTransitionCorpus, map[string]json.RawMessage) {
	t.Helper()
	raw, err := specvectors.Read(healthTransitionCorpusPath)
	if err != nil {
		t.Fatal(err)
	}
	var corpus healthTransitionCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != 2 || corpus.Baselines != "health-transition-baselines-v2.json" ||
		!strings.HasSuffix(corpus.Contract, "#reading-status-while-runtimes-adopt-runtime-healthv4") {
		t.Fatalf("unexpected corpus header: %q v%d baselines %q", corpus.Contract, corpus.Version, corpus.Baselines)
	}
	if want := []string{HealthStatusOK, HealthStatusDegraded, HealthStatusCritical, HealthStatusUnknown}; !reflect.DeepEqual(corpus.Severities, want) {
		t.Fatalf("corpus severities %v, want %v", corpus.Severities, want)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("corpus has no cases")
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
	if len(baselines) != 2 || baselines["v2.5.0"] == nil || baselines["e4ba3e92"] == nil {
		t.Fatalf("unexpected baselines: %d", len(baselines))
	}
	return corpus, baselines
}

// decodeItem decodes a vector item as the client decodes an export page, with
// numbers kept exact.
func decodeItem(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var item any
	if err := decoder.Decode(&item); err != nil {
		t.Fatal(err)
	}
	return item
}

// healthRuntime answers each health export request a client publishes with
// the next item it is given, as a verified response: signed under the shared
// secret, for the device and request the request named, on its response topic.
type healthRuntime struct {
	t      *testing.T
	broker *fakeBroker
	now    time.Time
	next   any
}

func newHealthRuntime(t *testing.T, now time.Time) *healthRuntime {
	r := &healthRuntime{t: t, broker: newFakeBroker(), now: now}
	r.broker.publishHook = func(_ string, payload []byte) {
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: healthTransitionSecret, Now: func() time.Time { return now }})
		if err != nil {
			t.Error(err)
			return
		}
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		if _, err := verifier.VerifyJSON(payload, contracts.ExportRequestMessageType, req.DeviceID, req.RequestID); err != nil {
			t.Errorf("export request not signed: %v", err)
			return
		}
		resp := withEnvelope(map[string]any{
			"request_id": req.RequestID, "device_id": req.DeviceID, "export_type": req.ExportType,
			"items": []any{r.next},
		})
		r.broker.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(t, resp, healthTransitionSecret, r.now))
	}
	return r
}

// health reads item through c's export path, as an answer to a health export
// requested from deviceID.
func (r *healthRuntime) health(c *MQTTClient, deviceID string, item any) HealthSnapshot {
	r.t.Helper()
	r.next = item
	snapshot, err := c.Health(context.Background(), HealthRequest{DeviceID: deviceID})
	if err != nil {
		r.t.Fatalf("Health = %v", err)
	}
	return snapshot
}

// TestHealthTransitionCorpus reads every case of the health transition corpus
// through the runtime client's verified export path: a health export signed
// under the shared secret, with message encryption off and on, answered on the
// request's response topic and read by MQTTClient.Health. Each case reads the
// severity it gives, logs a fault exactly when it reads unknown, without the
// item's content, and leaves the device's latch as it gives, which a later
// well-formed producer export then shows. Each producer baseline reads ok.
func TestHealthTransitionCorpus(t *testing.T) {
	corpus, baselines := loadHealthTransition(t)
	healthyLater := decodeItem(t, baselines["e4ba3e92"])
	marker := map[string]any{"projection": healthProjectionMarker, "status": HealthStatusOK}
	now := time.UnixMilli(1_787_000_003_000)

	for _, encrypted := range []bool{false, true} {
		newClient := func(t *testing.T, runtime *healthRuntime) (*MQTTClient, *lockedBuffer) {
			logs := &lockedBuffer{}
			opts := []MQTTClientOption{
				WithMessageAuth(healthTransitionSecret, "", func() time.Time { return now }),
				WithLogger(slog.New(slog.NewJSONHandler(logs, nil))), WithExportWait(5 * time.Second),
			}
			if encrypted {
				opts = append(opts, WithMessageEncryption(healthTransitionSecret))
			}
			c, err := NewMQTTClient(runtime.broker, opts...)
			if err != nil {
				t.Fatal(err)
			}
			return c, logs
		}
		for name, raw := range baselines {
			t.Run(fmt.Sprintf("encrypted=%t/baseline/%s", encrypted, name), func(t *testing.T) {
				runtime := newHealthRuntime(t, now)
				c, _ := newClient(t, runtime)
				if got := runtime.health(c, "dev-01", decodeItem(t, raw)).Status; got != HealthStatusOK {
					t.Fatalf("producer baseline %s reads %q, want ok", name, got)
				}
			})
		}
		for _, tc := range corpus.Cases {
			t.Run(fmt.Sprintf("encrypted=%t/%s", encrypted, tc.Name), func(t *testing.T) {
				runtime := newHealthRuntime(t, now)
				c, logs := newClient(t, runtime)
				if tc.Latched {
					if got := runtime.health(c, tc.RequestedDeviceID, marker).Status; got != HealthStatusOK {
						t.Fatalf("latching item reads %q, want ok", got)
					}
				}
				item := decodeItem(t, tc.Item)
				snapshot := runtime.health(c, tc.RequestedDeviceID, item)
				if snapshot.Status != tc.Expect.Severity {
					t.Fatalf("%s (%s): severity %q, want %q", tc.Name, tc.Note, snapshot.Status, tc.Expect.Severity)
				}
				if snapshot.DeviceID != tc.RequestedDeviceID {
					t.Fatalf("snapshot names %q, want the requested device %q", snapshot.DeviceID, tc.RequestedDeviceID)
				}

				c.warn("sentinel")
				logged := logs.once(func(s string) bool { return strings.Contains(s, "sentinel") })
				faults := faultRecords(t, logged)
				if want := map[bool]int{true: 1, false: 0}[tc.Expect.Severity == HealthStatusUnknown]; len(faults) != want {
					t.Fatalf("%d faults logged, want %d:\n%s", len(faults), want, logged)
				}
				// A fault names the device the export was requested from,
				// never the item's own device_id, which its sender writes.
				itemDevice, _ := item.(map[string]any)["device_id"].(string)
				for _, record := range faults {
					if record["device_id"] != tc.RequestedDeviceID || record["fault"] == "" {
						t.Fatalf("fault record %v, want device_id %q and a fault", record, tc.RequestedDeviceID)
					}
					for name, value := range record {
						if itemDevice != tc.RequestedDeviceID && value == itemDevice && name != "msg" {
							t.Fatalf("fault record carries the item's device_id %q in %s: %v", itemDevice, name, record)
						}
					}
				}
				for _, leaked := range itemContent(item) {
					if strings.Contains(logged, leaked) {
						t.Fatalf("log carries item content %q:\n%s", leaked, logged)
					}
				}

				// The latch shows in how the device's next well-formed
				// producer export reads.
				want := HealthStatusOK
				if tc.Expect.LatchedAfter {
					want = HealthStatusUnknown
				}
				if got := runtime.health(c, tc.RequestedDeviceID, healthyLater).Status; got != want {
					t.Fatalf("latched after = %t, but a later producer export reads %q", tc.Expect.LatchedAfter, got)
				}
			})
		}
	}
}

// faultRecords parses the client's JSON log records and returns the health
// fault records among them, in order.
func faultRecords(t *testing.T, logged string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(logged) {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record["msg"] == "runtime health export severity unreadable; reported unknown" {
			out = append(out, record)
		}
	}
	return out
}

// itemContent is what a fault log must never carry from an item: its
// health_socket_path, its degradation reasons and a status that is not a
// severity.
func itemContent(item any) []string {
	m, ok := item.(map[string]any)
	if !ok {
		return nil
	}
	var out []string
	if path, ok := m["health_socket_path"].(string); ok && path != "" {
		out = append(out, path)
	}
	if reasons, ok := m["degradation_reasons"].([]any); ok {
		for _, reason := range reasons {
			if s, ok := reason.(string); ok && s != "" {
				out = append(out, s)
			}
		}
	}
	if status, ok := m["status"].(string); ok {
		switch status {
		case HealthStatusOK, HealthStatusDegraded, HealthStatusCritical, HealthStatusUnknown:
		default:
			out = append(out, status)
		}
	}
	return out
}

// TestALatchBelongsToTheRequestedDevice reads a marked item from one device:
// another device's unmarked producer export still reads ok, and an item that
// names the latched device in its own device_id latches nothing.
func TestALatchBelongsToTheRequestedDevice(t *testing.T) {
	_, baselines := loadHealthTransition(t)
	now := time.UnixMilli(1_787_000_003_000)
	runtime := newHealthRuntime(t, now)
	c, err := NewMQTTClient(runtime.broker, WithMessageAuth(healthTransitionSecret, "", func() time.Time { return now }),
		WithExportWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	marked := map[string]any{"projection": healthProjectionMarker, "status": HealthStatusDegraded, "device_id": "dev-02"}
	if got := runtime.health(c, "dev-01", marked).Status; got != HealthStatusDegraded {
		t.Fatalf("marked item reads %q", got)
	}
	other := decodeItem(t, baselines["e4ba3e92"]).(map[string]any)
	other["device_id"] = "dev-02"
	if got := runtime.health(c, "dev-02", other).Status; got != HealthStatusOK {
		t.Fatalf("another device's producer export reads %q after dev-01 latched, want ok", got)
	}
	if got := runtime.health(c, "dev-01", decodeItem(t, baselines["e4ba3e92"])).Status; got != HealthStatusUnknown {
		t.Fatalf("the latched device's unmarked export reads %q, want unknown", got)
	}
}

// TestAFaultIsLoggedOncePerEpisode reads one device's exports on one client:
// a fault, the same fault again, a clean export, the fault again, another
// fault, and that fault again. A fault is logged when it begins, so on the
// first, fourth and fifth readings only: a repeat logs nothing, a clean
// reading ends the episode, and a different fault is a new one.
func TestAFaultIsLoggedOncePerEpisode(t *testing.T) {
	_, baselines := loadHealthTransition(t)
	now := time.UnixMilli(1_787_000_003_000)
	runtime := newHealthRuntime(t, now)
	logs := &lockedBuffer{}
	c, err := NewMQTTClient(runtime.broker, WithMessageAuth(healthTransitionSecret, "", func() time.Time { return now }),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))), WithExportWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	clean := func() map[string]any { return decodeItem(t, baselines["v2.5.0"]).(map[string]any) }
	noUptime := clean()
	delete(noUptime, "uptime_s")
	otherDevice := clean()
	otherDevice["device_id"] = "dev-02"
	steps := []struct {
		item map[string]any
		want string
		log  string
	}{
		{noUptime, HealthStatusUnknown, "member_absent:uptime_s"},
		{noUptime, HealthStatusUnknown, ""},
		{clean(), HealthStatusOK, ""},
		{noUptime, HealthStatusUnknown, "member_absent:uptime_s"},
		{otherDevice, HealthStatusUnknown, "device_mismatch"},
		{otherDevice, HealthStatusUnknown, ""},
	}
	var want []string
	for i, step := range steps {
		if got := runtime.health(c, "dev-01", step.item).Status; got != step.want {
			t.Fatalf("reading %d: %q, want %q", i+1, got, step.want)
		}
		if step.log != "" {
			want = append(want, fmt.Sprintf("%d:%s", i+1, step.log))
		}
		c.warn("sentinel", "step", i+1)
	}
	logged := logs.once(func(s string) bool { return strings.Contains(s, fmt.Sprintf(`"step":%d`, len(steps))) })
	var got []string
	step := 1
	for line := range strings.Lines(logged) {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		switch record["msg"] {
		case "sentinel":
			step++
		case "runtime health export severity unreadable; reported unknown":
			got = append(got, fmt.Sprintf("%d:%v", step, record["fault"]))
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("faults logged %v, want %v", got, want)
	}
}

// TestConcurrentReadingsShareOneReader reads health exports concurrently on
// one client, marked and unmarked items for the same and different devices.
// Run under -race, the shared reader's latch and fault state is guarded;
// every reading is a severity, and each device that served a marked item is
// latched afterwards.
func TestConcurrentReadingsShareOneReader(t *testing.T) {
	_, baselines := loadHealthTransition(t)
	now := time.UnixMilli(1_787_000_003_000)
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithMessageAuth(healthTransitionSecret, "", func() time.Time { return now }),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), WithExportWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	devices := []string{"dev-01", "dev-02", "dev-03"}
	served := map[string]*atomic.Int64{}
	for _, device := range devices {
		served[device] = &atomic.Int64{}
	}
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		item := decodeItem(t, baselines["e4ba3e92"]).(map[string]any)
		item["device_id"] = req.DeviceID
		if served[req.DeviceID].Add(1)%2 == 0 {
			item = map[string]any{"projection": healthProjectionMarker, "status": HealthStatusDegraded}
		}
		resp := withEnvelope(map[string]any{
			"request_id": req.RequestID, "device_id": req.DeviceID, "export_type": req.ExportType, "items": []any{item},
		})
		b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(t, resp, healthTransitionSecret, now))
	}
	var wg sync.WaitGroup
	for i := range 48 {
		wg.Go(func() {
			snapshot, err := c.Health(context.Background(), HealthRequest{DeviceID: devices[i%len(devices)]})
			if err != nil {
				t.Error(err)
				return
			}
			switch snapshot.Status {
			case HealthStatusOK, HealthStatusDegraded, HealthStatusUnknown:
			default:
				t.Errorf("reading %q", snapshot.Status)
			}
		})
	}
	wg.Wait()
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	for _, device := range devices {
		if !c.health.latched[device] {
			t.Errorf("%s served marked items but is not latched", device)
		}
	}
}
