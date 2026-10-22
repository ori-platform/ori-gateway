// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

// vectorPin records a contract vector this gateway does not yet satisfy: the
// missing requirement, the issue that tracks it, and the exact deviation the
// harness observes. A pin is satisfied only by that deviation; any other
// failure, and a case that now conforms, fails the test, so a pin can neither
// hide an unrelated fault nor outlive the behaviour it stands for.
type vectorPin struct {
	// Requirement is a stable identifier, missing_requirement:<id>.
	Requirement string
	// Issue is the tracking issue's URL.
	Issue string
	// Observed is the deviation the harness reports, verbatim.
	Observed string
}

var pinRequirement = regexp.MustCompile(`^missing_requirement:[a-z0-9]+(-[a-z0-9]+)*$`)

// pinIssue is the one form a pin's tracking issue takes.
var pinIssue = regexp.MustCompile(`^https://github\.com/ori-platform/ori-gateway/issues/[1-9][0-9]*$`)

// checkPin settles a vector case once its behaviour has been observed:
// observed is "" when the case conforms, and otherwise the deviation.
func checkPin(t *testing.T, pins map[string]vectorPin, name, observed string) {
	t.Helper()
	pin, pinned := pins[name]
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

// checkPinsExist refuses a pin naming a case the corpus does not hold, and a
// pin without a well-formed requirement or tracking issue.
func checkPinsExist(t *testing.T, pins map[string]vectorPin, names []string) {
	t.Helper()
	have := make(map[string]bool, len(names))
	for _, name := range names {
		have[name] = true
	}
	for name, pin := range pins {
		if !have[name] {
			t.Errorf("pin %q names no case in the corpus", name)
		}
		if !pinRequirement.MatchString(pin.Requirement) || !pinIssue.MatchString(pin.Issue) || pin.Observed == "" {
			t.Errorf("pin %q needs a missing_requirement:<id>, its tracking issue's full URL and the observed deviation", name)
		}
	}
}

// waitUntil reports whether cond held within the harness's wait, without
// failing: a behaviour that never arrives is a deviation to report, not a
// harness fault. Its bound is waitFor's.
func waitUntil(cond func() bool) bool {
	return waitUntilWithin(eventBound, cond)
}

// pinnedAbsenceBound is how long a hold or an archive is waited for in the
// step a pin expects to deviate. Those pins observe the expectation absent, so
// the wait ends by timing out and need not last eventBound; every other wait
// in that step keeps eventBound. A pin that now conforms is still seen on any
// machine that reaches the hold or archive within the bound.
const pinnedAbsenceBound = 2 * time.Second

// pinnedStep is the step number a pinned sequence's observed deviation names,
// or -1 for a sequence that is not pinned.
func pinnedStep(pins map[string]vectorPin, name string) int {
	pin, ok := pins[name]
	if !ok {
		return -1
	}
	var step int
	if _, err := fmt.Sscanf(pin.Observed, "step %d ", &step); err != nil {
		return -1
	}
	return step
}

func waitUntilWithin(bound time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(bound)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}
