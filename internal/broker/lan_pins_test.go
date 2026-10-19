// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"regexp"
	"testing"
)

// lanPin records a payload-corpus case on a LAN family the gateway does not
// yet carry: the missing requirement, its tracking issue and the exact
// deviation. It is satisfied only by that deviation, and a case that now
// conforms fails until its pin is removed.
type lanPin struct {
	// Family is the family the pinned case or family belongs to; a pin whose
	// name now names a case of another family fails.
	Family      string
	Requirement string
	Issue       string
	Observed    string
}

var (
	lanPinRequirement = regexp.MustCompile(`^missing_requirement:[a-z0-9]+(-[a-z0-9]+)*$`)
	lanPinIssue       = regexp.MustCompile(`^https://github\.com/ori-platform/ori-gateway/issues/[1-9][0-9]*$`)
)

// lanFamilyPins are the corpus families absent from the gateway's member
// table, keyed by family.
var lanFamilyPins = map[string]lanPin{
	"ori/{device_id}/lan/disclosure/request":               {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "not in the gateway's member table"},
	"ori/{device_id}/lan/disclosure/response/{request_id}": {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "not in the gateway's member table"},
	"ori/{device_id}/lan/proposal":                         {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "not in the gateway's member table"},
	"ori/{device_id}/lan/reply":                            {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "not in the gateway's member table"},
}

// lanMemberCasePins are the payload-members cases the gateway does not yet
// satisfy, keyed by case name.
var lanMemberCasePins = map[string]lanPin{
	"envelope_variant_07":         {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "accept, want case_variant_member"},
	"envelope_variant_08":         {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "accept, want case_variant_member"},
	"envelope_variant_09":         {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "accept, want case_variant_member"},
	"envelope_variant_10":         {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "accept, want case_variant_member"},
	"lan_envelope_only_variant_0": {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "accept, want case_variant_member"},
	"lan_envelope_only_variant_1": {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "accept, want case_variant_member"},
	"lan_envelope_only_variant_2": {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "accept, want case_variant_member"},
	"lan_envelope_only_variant_3": {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "accept, want case_variant_member"},
}

// lanSizeCasePins are the payload-size cases the gateway does not yet
// satisfy, keyed by case name.
var lanSizeCasePins = map[string]lanPin{
	"lan_proposal_at_maximum":                        {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_one_over":                          {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_four_times":                        {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_invalid_utf8_one_over":             {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_configured_half_at":                {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_configured_half_over":              {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_configured_above_table":            {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_proposal_configured_below_floor":            {Family: "ori/{device_id}/lan/proposal", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_reply_at_maximum":                           {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_one_over":                             {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_four_times":                           {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_invalid_utf8_one_over":                {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_configured_half_at":                   {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_configured_half_over":                 {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_configured_above_table":               {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_reply_configured_below_floor":               {Family: "ori/{device_id}/lan/reply", Requirement: "missing_requirement:lan-reply-relay", Issue: "https://github.com/ori-platform/ori-gateway/issues/119", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_at_maximum":              {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_one_over":                {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_four_times":              {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_invalid_utf8_one_over":   {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_configured_half_at":      {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_configured_half_over":    {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_configured_above_table":  {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_request_configured_below_floor":  {Family: "ori/{device_id}/lan/disclosure/request", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "topic is not in the gateway's table"},
	"lan_disclosure_response_at_maximum":             {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_one_over":               {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_four_times":             {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_invalid_utf8_one_over":  {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_configured_half_at":     {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_configured_half_over":   {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_configured_above_table": {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
	"lan_disclosure_response_configured_below_floor": {Family: "ori/{device_id}/lan/disclosure/response/{request_id}", Requirement: "missing_requirement:lan-disclosure", Issue: "https://github.com/ori-platform/ori-gateway/issues/117", Observed: "direction runtime_to_gateway disagrees with the families the gateway receives"},
}

// pinReporter is the part of testing.T a pin check reports through.
type pinReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

// checkLANPin reports observed, the deviation of the case or family name ("" when
// it conforms), against pins, and returns whether the caller must stop
// checking it.
func checkLANPin(t pinReporter, pins map[string]lanPin, name, observed string) bool {
	t.Helper()
	pin, pinned := pins[name]
	switch {
	case !pinned && observed == "":
		return false
	case !pinned:
		t.Errorf("%s: %s", name, observed)
	case observed == "":
		t.Errorf("%s now conforms; remove its pin (%s, %s)", name, pin.Requirement, pin.Issue)
	case observed != pin.Observed:
		t.Errorf("%s: pinned for %q (%s) but deviated with %q", name, pin.Observed, pin.Requirement, observed)
	default:
		t.Logf("expected failure, %s (%s): %s: %s", pin.Requirement, pin.Issue, name, observed)
	}
	return true
}

// checkLANPinsExist requires every pin to name something in the corpus, of
// the family it was pinned for, and to carry a requirement, its issue and its
// deviation. families maps each corpus name to its family.
func checkLANPinsExist(t *testing.T, pins map[string]lanPin, families map[string]string) {
	t.Helper()
	for name, pin := range pins {
		if family, ok := families[name]; !ok {
			t.Errorf("pin %q names nothing in the corpus", name)
		} else if family != pin.Family {
			t.Errorf("pin %q is for family %s; the corpus case is on %s", name, pin.Family, family)
		}
		if !lanPinRequirement.MatchString(pin.Requirement) || !lanPinIssue.MatchString(pin.Issue) || pin.Observed == "" {
			t.Errorf("pin %q needs a missing_requirement:<id>, its tracking issue's full URL and the observed deviation", name)
		}
	}
}

// pinRecorder records what a pin check reports.
type pinRecorder struct{ errors int }

func (r *pinRecorder) Helper()               {}
func (r *pinRecorder) Errorf(string, ...any) { r.errors++ }
func (r *pinRecorder) Logf(string, ...any)   {}

// TestALANPinFailsUnlessItsDeviationIsExact drives checkLANPin through every
// outcome: a conforming unpinned case passes and is checked further, a
// pinned case passes only with its exact deviation, and an unpinned
// deviation, a changed deviation and a pinned case that now conforms each
// fail.
func TestALANPinFailsUnlessItsDeviationIsExact(t *testing.T) {
	pins := map[string]lanPin{"pinned": {Requirement: "missing_requirement:lan-reply-relay", Observed: "deviates"}}
	for _, tc := range []struct {
		name, observed string
		stop           bool
		errors         int
	}{
		{"unpinned", "", false, 0},
		{"unpinned", "deviates", true, 1},
		{"pinned", "deviates", true, 0},
		{"pinned", "deviates otherwise", true, 1},
		{"pinned", "", true, 1},
	} {
		r := &pinRecorder{}
		if stop := checkLANPin(r, pins, tc.name, tc.observed); stop != tc.stop || r.errors != tc.errors {
			t.Errorf("%s observed %q: stop=%v errors=%d, want stop=%v errors=%d", tc.name, tc.observed, stop, r.errors, tc.stop, tc.errors)
		}
	}
}
