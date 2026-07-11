// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

// NodeEvidence is the registry's view of one node's evidence-chain signal,
// enriched with the gateway-side integrity observations that a single
// heartbeat cannot carry.
//
// The chain head is an opaque hash, so the gateway cannot order two heads.
// What it can detect, by remembering recent heads per device, is the two
// shapes a local tamper attempt takes:
//
//   - truncation: a device that previously reported a non-empty head starts
//     reporting an empty chain — the chain database was deleted or reset;
//   - regression: a device reports a head it had already moved past — the
//     chain was rolled back to an earlier state.
//
// Both flags are sticky for the lifetime of the registry entry: an operator
// must investigate; a subsequent normal heartbeat does not clear suspicion.
type NodeEvidence struct {
	ChainHeadHash       string `json:"chain_head_hash"`
	AttestationGapCount int    `json:"attestation_gap_count"`
	Available           bool   `json:"available"`
	// ActionEventType is the emission vocabulary the device reported for
	// new Tier C/D attestations ("" while signing is unavailable).
	// Informational: it never affects degradation.
	ActionEventType     string `json:"action_event_type"`
	TruncationSuspected bool   `json:"truncation_suspected"`
	HeadRegressed       bool   `json:"head_regressed"`
}

// evidenceHeadHistorySize bounds the per-device ring of recently seen chain
// heads used for regression detection. At one head change per Tier C/D
// action, 16 entries cover far more history than a plausible rollback window.
const evidenceHeadHistorySize = 16

// evidenceTrack is the registry's per-device evidence memory. It outlives
// individual heartbeats and is dropped only when the node is evicted.
type evidenceTrack struct {
	recentHeads         []string // newest last; bounded ring
	truncationSuspected bool
	headRegressed       bool
}

func (t *evidenceTrack) currentHead() string {
	if len(t.recentHeads) == 0 {
		return ""
	}
	return t.recentHeads[len(t.recentHeads)-1]
}

func (t *evidenceTrack) sawHeadBeforeCurrent(head string) bool {
	if len(t.recentHeads) < 2 {
		return false
	}
	for _, h := range t.recentHeads[:len(t.recentHeads)-1] {
		if h == head {
			return true
		}
	}
	return false
}

func (t *evidenceTrack) recordHead(head string) {
	t.recentHeads = append(t.recentHeads, head)
	if len(t.recentHeads) > evidenceHeadHistorySize {
		t.recentHeads = t.recentHeads[len(t.recentHeads)-evidenceHeadHistorySize:]
	}
}

// observeMissing handles a heartbeat that omitted the evidence block from a
// device with evidence history. A device that has reported evidence must keep
// reporting it — treating omission as "no evidence surface" would let an
// evidence-enabled runtime silently downgrade (misconfiguration or tamper).
// The last known head is preserved and Available is forced false, which the
// health projection treats as degraded until evidence reporting resumes.
func (t *evidenceTrack) observeMissing() NodeEvidence {
	return NodeEvidence{
		ChainHeadHash:       t.currentHead(),
		Available:           false,
		TruncationSuspected: t.truncationSuspected,
		HeadRegressed:       t.headRegressed,
	}
}

// observe folds one heartbeat's evidence signal into the track and returns
// the enriched NodeEvidence for the registry entry.
func (t *evidenceTrack) observe(reported NodeEvidence) NodeEvidence {
	head := reported.ChainHeadHash
	switch {
	case head == "":
		// An empty head after a non-empty one means the chain database
		// reset underneath the runtime. An empty head on a brand-new
		// device (no history) is normal.
		if t.currentHead() != "" {
			t.truncationSuspected = true
		}
	case head != t.currentHead():
		if t.sawHeadBeforeCurrent(head) {
			// The chain moved back to a head it had already advanced
			// past: a rollback, not an append.
			t.headRegressed = true
		}
		t.recordHead(head)
	}

	reported.TruncationSuspected = t.truncationSuspected
	reported.HeadRegressed = t.headRegressed
	return reported
}
