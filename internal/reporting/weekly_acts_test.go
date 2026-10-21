// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// exportBroker answers runtime export requests the way the runtime does over MQTT.
type exportBroker struct {
	t          *testing.T
	mu         sync.Mutex
	handlers   map[string]broker.MessageHandler
	actionLog  []any
	exportSeen map[string]int
	// clock, when set, is read as each request is published, into sent.
	clock func() time.Time
	sent  []time.Time
}

func (b *exportBroker) Subscribe(_ context.Context, topic string, _ byte, handler broker.MessageHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = handler
	return nil
}

func (b *exportBroker) Publish(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
	var req struct {
		RequestID  string `json:"request_id"`
		ExportType string `json:"export_type"`
		DeviceID   string `json:"device_id"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		b.t.Fatal(err)
	}
	items := []any{}
	switch req.ExportType {
	case "health":
		items = []any{map[string]any{"device_id": req.DeviceID}}
	case "action_log":
		items = b.actionLog
	}
	encoded, err := json.Marshal(map[string]any{
		"request_id":      req.RequestID,
		"device_id":       req.DeviceID,
		"export_type":     req.ExportType,
		"items":           items,
		"next_page_token": "",
		"complete":        true,
		"error":           nil,
	})
	if err != nil {
		b.t.Fatal(err)
	}
	topic, err := contracts.ExportResponseTopicFilter(req.DeviceID)
	if err != nil {
		b.t.Fatal(err)
	}
	b.mu.Lock()
	handler := b.handlers[topic]
	b.exportSeen[req.ExportType]++
	if b.clock != nil {
		b.sent = append(b.sent, b.clock())
	}
	b.mu.Unlock()
	if handler == nil {
		b.t.Fatalf("no export response handler on %s", topic)
	}
	handler("ori/"+req.DeviceID+"/export/response/"+req.RequestID, encoded)
	return nil
}

func generateFromActionLogExport(t *testing.T, actionLog []any) (WeeklyReportArtifact, *fakeProvider) {
	t.Helper()
	b := &exportBroker{t: t, handlers: map[string]broker.MessageHandler{}, actionLog: actionLog, exportSeen: map[string]int{}}
	client, err := runtimeclient.NewMQTTClient(b, runtimeclient.WithExportPacingClock(instantPacingClock()))
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeProvider{report: ProviderReport{Text: "report"}}
	generator, err := NewWeeklyReportGenerator(client, provider, WithNow(fixedNow))
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.exportSeen["action_log"] != 1 {
		t.Fatalf("action_log export requests = %d, want 1", b.exportSeen["action_log"])
	}
	return artifact, provider
}

func actionRow(name string, executed bool, actionTaken any) map[string]any {
	row := map[string]any{
		"device_id":   "edge-1",
		"timestamp":   float64(1_799_900_000_000),
		"action_name": name,
		"tier":        "C",
		"executed":    executed,
	}
	if actionTaken != nil {
		row["action_taken"] = actionTaken
	}
	return row
}

// retained is the record the report must hand on for a dispatch row built by
// actionRow, written out field by field rather than through the client's mapper.
func retained(name string, executed, safeDefaultUsed bool, actionTaken string) runtimeclient.ActionLogEntry {
	return runtimeclient.ActionLogEntry{
		DeviceID:        "edge-1",
		CreatedAtMS:     1_799_900_000_000,
		ActionName:      name,
		Tier:            "C",
		SafeDefaultUsed: safeDefaultUsed,
		Success:         executed,
		ActionTaken:     actionTaken,
	}
}

// assertRetained requires the provider input to be exactly want: contributors
// dropped, every kept record unchanged and in export order.
func assertRetained(t *testing.T, provider *fakeProvider, want ...runtimeclient.ActionLogEntry) {
	t.Helper()
	got := provider.input.Actions
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("provider actions =\n%#v\nwant\n%#v", got, want)
	}
}

func providerActionTaken(provider *fakeProvider) []string {
	out := make([]string, 0, len(provider.input.Actions))
	for _, row := range provider.input.Actions {
		out = append(out, row.ActionName+"/"+row.ActionTaken)
	}
	return out
}

func TestWeeklyReportCountsAnActWithTwoContributorsOnce(t *testing.T) {
	artifact, provider := generateFromActionLogExport(t, []any{
		actionRow("trip_relay", true, "trip_relay"),
		actionRow("trip_relay", false, "coalesced"),
		actionRow("trip_relay", false, "coalesced"),
	})
	if artifact.ActionCount != 1 {
		t.Fatalf("ActionCount = %d, want 1 act", artifact.ActionCount)
	}
	if got := providerActionTaken(provider); len(got) != 1 || got[0] != "trip_relay/trip_relay" {
		t.Fatalf("provider actions = %v, want the act only", got)
	}
	assertRetained(t, provider, retained("trip_relay", true, false, "trip_relay"))
}

func TestWeeklyReportDoesNotCountLegacyContributor(t *testing.T) {
	artifact, provider := generateFromActionLogExport(t, []any{
		actionRow("trip_relay", true, "trip_relay"),
		actionRow("trip_relay", true, "coalesced"),
	})
	if artifact.ActionCount != 1 {
		t.Fatalf("ActionCount = %d, want the legacy contributor left out", artifact.ActionCount)
	}
	for _, row := range provider.input.Actions {
		if row.ActionTaken == "coalesced" {
			t.Fatalf("provider input lists a legacy contributor as an action: %#v", row)
		}
	}
	assertRetained(t, provider, retained("trip_relay", true, false, "trip_relay"))
}

func TestWeeklyReportCountsExportWithoutRecordKindByCoalescedRule(t *testing.T) {
	act := actionRow("alert_sms", true, "alert_sms")
	failed := actionRow("trip_relay", false, "log_to_dashboard")
	failed["safe_default_used"] = true
	untagged := actionRow("alert_whatsapp", true, nil)
	contributor := actionRow("alert_sms", false, "coalesced")
	legacy := actionRow("trip_relay", true, "coalesced")
	for _, row := range []map[string]any{act, failed, untagged, contributor, legacy} {
		if _, ok := row["record_kind"]; ok {
			t.Fatal("fixture must model an export without record_kind")
		}
	}
	artifact, provider := generateFromActionLogExport(t, []any{act, contributor, failed, legacy, untagged})
	if artifact.ActionCount != 3 {
		t.Fatalf("ActionCount = %d, want 3 acts", artifact.ActionCount)
	}
	want := []string{"alert_sms/alert_sms", "trip_relay/log_to_dashboard", "alert_whatsapp/"}
	got := providerActionTaken(provider)
	if len(got) != len(want) {
		t.Fatalf("provider actions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("provider actions = %v, want %v", got, want)
		}
	}
	assertRetained(t, provider,
		retained("alert_sms", true, false, "alert_sms"),
		retained("trip_relay", false, true, "log_to_dashboard"),
		retained("alert_whatsapp", true, false, ""),
	)
}

func TestWeeklyReportContributorRecognitionIsExact(t *testing.T) {
	cases := []struct {
		name        string
		actionTaken any
		executed    bool
		wantActs    int
		wantTaken   string
	}{
		{"exact contributor", "coalesced", false, 0, ""},
		{"exact legacy contributor", "coalesced", true, 0, ""},
		{"capitalised", "Coalesced", true, 1, "Coalesced"},
		{"upper case", "COALESCED", false, 1, "COALESCED"},
		{"surrounding whitespace", " coalesced ", false, 1, " coalesced "},
		{"empty", "", true, 1, ""},
		{"absent", nil, true, 1, ""},
		{"non-string", float64(0), false, 1, "0"},
		{"null", json.RawMessage("null"), true, 1, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			artifact, provider := generateFromActionLogExport(t, []any{actionRow("trip_relay", tc.executed, tc.actionTaken)})
			if artifact.ActionCount != tc.wantActs || len(provider.input.Actions) != tc.wantActs {
				t.Fatalf("ActionCount = %d, provider actions = %d, want %d", artifact.ActionCount, len(provider.input.Actions), tc.wantActs)
			}
			if tc.wantActs == 1 {
				assertRetained(t, provider, retained("trip_relay", tc.executed, false, tc.wantTaken))
			}
		})
	}
}

func TestWeeklyReportContributorOnlyAndEmptyLogsCountNoActs(t *testing.T) {
	for name, log := range map[string][]any{
		"empty":             {},
		"contributors only": {actionRow("trip_relay", false, "coalesced"), actionRow("trip_relay", true, "coalesced")},
	} {
		t.Run(name, func(t *testing.T) {
			artifact, provider := generateFromActionLogExport(t, log)
			if artifact.ActionCount != 0 || len(provider.input.Actions) != 0 {
				t.Fatalf("ActionCount = %d, provider actions = %d, want none", artifact.ActionCount, len(provider.input.Actions))
			}
		})
	}
}

func TestWeeklyReportTruncationWarningCountsRowsNotActs(t *testing.T) {
	rows := make([]runtimeclient.ActionLogEntry, runtimeclient.MaxLimit)
	for i := range rows {
		rows[i] = runtimeclient.ActionLogEntry{ActionName: "trip_relay", ActionTaken: "coalesced"}
	}
	runtime := &runtimeclient.FakeClient{
		HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"},
		ActionLogRows:  rows,
	}
	provider := &fakeProvider{report: ProviderReport{Text: "report"}}
	artifact, err := newGenerator(t, runtime, provider).Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ActionCount != 0 {
		t.Fatalf("ActionCount = %d, want 0", artifact.ActionCount)
	}
	if len(artifact.Warnings) != 1 || artifact.Warnings[0] != "action_log may be truncated at runtime export limit" {
		t.Fatalf("a full page of contributor rows must still warn of truncation, got %#v", artifact.Warnings)
	}
}
