// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// TestAWeeklyReportForManySensorsStaysWithinTheRuntimeExportRate generates a
// weekly report for a device with far more sensors than the runtime's export
// burst, through the real MQTT export client: every request is answered, and
// on the pacing clock the requests never exceed the 2 a second, burst 10 at
// which the runtime admits them, so none is dropped to wait out its timeout.
func TestAWeeklyReportForManySensorsStaysWithinTheRuntimeExportRate(t *testing.T) {
	var mu sync.Mutex
	at := time.Unix(1_787_000_000, 0)
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return at }
	b := &exportBroker{t: t, handlers: map[string]broker.MessageHandler{}, exportSeen: map[string]int{}, clock: now}
	client, err := runtimeclient.NewMQTTClient(b, runtimeclient.WithExportPacingClock(now,
		func(_ context.Context, d time.Duration) error { mu.Lock(); at = at.Add(d); mu.Unlock(); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	generator, err := NewWeeklyReportGenerator(client, &fakeProvider{report: ProviderReport{Text: "report"}}, WithNow(fixedNow))
	if err != nil {
		t.Fatal(err)
	}
	sensors := make([]string, 40)
	for i := range sensors {
		sensors[i] = fmt.Sprintf("sensor-%02d", i)
	}
	if _, err := generator.Generate(context.Background(), WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: sensors}); err != nil {
		t.Fatalf("a weekly report for %d sensors = %v", len(sensors), err)
	}
	if got, want := len(b.sent), len(sensors)+3; got < want {
		t.Fatalf("%d export requests, want at least %d (health, every sensor, action and Tier C logs)", got, want)
	}
	// The runtime's admission, written apart from the client's pacer: a token
	// bucket of 2 a second and burst 10.
	tokens := 10.0
	for i, sent := range b.sent {
		if i > 0 {
			tokens = min(10, tokens+sent.Sub(b.sent[i-1]).Seconds()*2)
		}
		if tokens < 1-1e-9 {
			t.Fatalf("export request %d of %d was sent beyond the runtime's rate", i, len(b.sent))
		}
		tokens--
	}
}
