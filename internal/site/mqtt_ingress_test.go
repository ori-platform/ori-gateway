// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// mqttIngressShape decodes a projection's gateway.mqtt_ingress and refuses any
// shape but the contract's: one member per received family, each an object of
// exactly the integers payload_too_large and volume_dropped.
func mqttIngressShape(t *testing.T, h SiteHealth) map[string]map[string]uint64 {
	t.Helper()
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Gateway map[string]json.RawMessage `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	member, ok := body.Gateway["mqtt_ingress"]
	if !ok || bytes.Equal(member, []byte("null")) {
		t.Fatalf("gateway posture lacks mqtt_ingress: %s", raw)
	}
	var families map[string]map[string]json.RawMessage
	if err := json.Unmarshal(member, &families); err != nil {
		t.Fatalf("mqtt_ingress is not an object of objects: %s", member)
	}
	var names []string
	for name := range families {
		names = append(names, name)
	}
	want := slices.Clone(contracts.ReceivedFamilies)
	slices.Sort(names)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("mqtt_ingress families %v, want %v", names, want)
	}
	counts := map[string]map[string]uint64{}
	for name, value := range families {
		if len(value) != 2 {
			t.Fatalf("%s carries %d members, want payload_too_large and volume_dropped", name, len(value))
		}
		counts[name] = map[string]uint64{}
		for _, key := range []string{"payload_too_large", "volume_dropped"} {
			var n uint64
			if err := json.Unmarshal(value[key], &n); err != nil {
				t.Fatalf("%s.%s = %s, want a non-negative integer", name, key, value[key])
			}
			counts[name][key] = n
		}
	}
	return counts
}

func TestProjectSiteHealthMQTTIngressIsExactlyTheReceivedFamilies(t *testing.T) {
	p := NewProjector(NewRegistry(), ProjectOptions{NodeTTLMS: 5000})
	gateway := healthyGateway()
	if counts := mqttIngressShape(t, p.Project(time.UnixMilli(10000), gateway)); counts[contracts.FamilyReasoningRequest]["payload_too_large"] != 0 {
		t.Fatalf("counts without refusals: %v", counts)
	}
	gateway.MQTTIngress = map[string]MQTTIngressCount{
		contracts.FamilyReasoningRequest: {PayloadTooLarge: 3, VolumeDropped: 2},
		"ori/dev-01/reasoning/request":   {PayloadTooLarge: 9},
		contracts.FamilyGatewayHealth:    {PayloadTooLarge: 9},
	}
	h := p.Project(time.UnixMilli(10000), gateway)
	counts := mqttIngressShape(t, h)
	if got := counts[contracts.FamilyReasoningRequest]; got["payload_too_large"] != 3 || got["volume_dropped"] != 2 {
		t.Fatalf("reasoning request counts = %v", got)
	}
	if h.Status != SiteStatusHealthy || h.Gateway.Status != SiteStatusHealthy {
		t.Fatalf("a refusal count degraded the site: %s", h.Status)
	}
}
