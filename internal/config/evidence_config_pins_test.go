// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"regexp"
	"testing"
)

// configPin records an evidence-config vector the loader does not yet satisfy:
// the missing requirement, its tracking issue and the exact deviation. It is
// satisfied only by that deviation, and a case that now conforms fails the
// test until its pin is removed.
type configPin struct {
	Requirement string
	Issue       string
	Observed    string
}

var configPinRequirement = regexp.MustCompile(`^missing_requirement:[a-z0-9]+(-[a-z0-9]+)*$`)

// configPinIssue is the one form a pin's tracking issue takes.
var configPinIssue = regexp.MustCompile(`^https://github\.com/ori-platform/ori-gateway/issues/[1-9][0-9]*$`)

// evidenceConfigPins are the evidence-config cases the loader does not yet
// satisfy.
var evidenceConfigPins = map[string]configPin{
	"a back-off base above five minutes raises the default bound to it": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a back-off base defaults to the delivery interval and the bound to five minutes": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a legacy device outside the routing domain beside an opted-in device": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a legacy site keeps a back-off bound below its base": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a legacy site keeps a one-item queue": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a legacy site keeps a store probe outside the range": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a mixed site declares one device and defaults the other to gateway-api/v1": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a non-ASCII device ID in the routing domain": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"a per-device stopped bound above the total": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "accepted, rule stopped expects a refusal",
	},
	"a zero stopped byte bound": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "accepted, rule stopped expects a refusal",
	},
	"an empty carriage map is the absent map": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"every default applied": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"stopped-custody bounds set per device and in total": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
	"the store probe at its lower edge": {
		Requirement: "missing_requirement:stopped-custody-bounds",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "the loader produces no stopped-custody bounds",
	},
}

func checkConfigPin(t *testing.T, name, observed string) {
	t.Helper()
	pin, pinned := evidenceConfigPins[name]
	switch {
	case !pinned && observed == "":
	case !pinned:
		t.Fatal(observed)
	case observed == "":
		t.Fatalf("%s now conforms; remove its pin (%s, %s)", name, pin.Requirement, pin.Issue)
	case observed != pin.Observed:
		t.Fatalf("pinned for %q (%s) but deviated with %q", pin.Observed, pin.Requirement, observed)
	default:
		t.Logf("expected failure, %s (%s): %s", pin.Requirement, pin.Issue, observed)
	}
}

func checkConfigPinsExist(t *testing.T, names []string) {
	t.Helper()
	have := make(map[string]bool, len(names))
	for _, name := range names {
		have[name] = true
	}
	for name, pin := range evidenceConfigPins {
		if !have[name] {
			t.Errorf("pin %q names no case in the corpus", name)
		}
		if !configPinRequirement.MatchString(pin.Requirement) || !configPinIssue.MatchString(pin.Issue) || pin.Observed == "" {
			t.Errorf("pin %q needs a missing_requirement:<id>, its tracking issue's full URL and the observed deviation", name)
		}
	}
}
