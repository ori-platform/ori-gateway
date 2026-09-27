// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
	"testing"
)

// routingProjectionPins are the routing-projection cases this gateway does not
// yet satisfy.
var routingProjectionPins = map[string]vectorPin{
	"a anchor_registration without anchor_epoch_id": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"a checkpoint whose anchor_epoch_id is uppercase hex": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"a checkpoint whose anchor_epoch_id member differs only in case": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"a checkpoint without anchor_epoch_id": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"a delivery_envelope without anchor_epoch_id": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"a v1 evidence disposition": {
		Requirement: "missing_requirement:disposition-carriage",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "artifact type evidence_disposition is not carried",
	},
	"an envelope naming device_id twice": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"an envelope whose anchor_epoch_id is not a digest": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"an envelope whose device_id member differs only in case": {
		Requirement: "missing_requirement:routing-anchor-epoch",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/104",
		Observed:    "routable true, corpus says false",
	},
	"an evidence disposition naming device_id twice": {
		Requirement: "missing_requirement:disposition-carriage",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "artifact type evidence_disposition is not carried",
	},
	"an evidence disposition without device_id": {
		Requirement: "missing_requirement:disposition-carriage",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/105",
		Observed:    "artifact type evidence_disposition is not carried",
	},
}

// TestRoutingProjectionVectors holds both hops to the evidence-exchange/v2
// stable routing projection: routability is decided by the routing fields
// alone, never by a declared version or by fields a later version adds.
func TestRoutingProjectionVectors(t *testing.T) {
	raw, err := specvectors.Read("evidence-exchange/vectors/routing-projection-v2.json")
	if err != nil {
		t.Fatalf("read routing-projection corpus: %v", err)
	}
	var doc struct {
		Contract string `json:"contract"`
		Note     string `json:"note"`
		Cases    []struct {
			Name         string          `json:"name"`
			ArtifactType string          `json:"artifact_type"`
			Artifact     json.RawMessage `json:"artifact"`
			// ArtifactJSON is the raw text a courier decodes, given where a
			// member is named twice and an object cannot carry the case.
			ArtifactJSON *string `json:"artifact_json"`
			Routable     bool    `json:"routable"`
			Missing      string  `json:"missing"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("the corpus has no cases")
	}
	names := make([]string, 0, len(doc.Cases))
	for _, tc := range doc.Cases {
		names = append(names, tc.Name)
	}
	checkPinsExist(t, routingProjectionPins, names)
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.ArtifactJSON != nil {
				if tc.Artifact != nil {
					t.Fatalf("a case carries artifact or artifact_json, never both")
				}
				tc.Artifact = json.RawMessage(*tc.ArtifactJSON)
			}
			var err error
			switch tc.ArtifactType {
			case string(ArtifactDeliveryEnvelope), string(ArtifactCheckpoint), string(ArtifactAnchorRegistration):
				err = validateArtifactRoutingFields(ArtifactType(tc.ArtifactType), tc.Artifact)
			case string(AuthorityDeliveryReceipt), string(AuthorityEpochConfirmation):
				_, err = authorityRoutingProjection(AuthorityArtifactType(tc.ArtifactType), tc.Artifact)
			default:
				checkPin(t, routingProjectionPins, tc.Name, fmt.Sprintf("artifact type %s is not carried", tc.ArtifactType))
				return
			}
			observed := ""
			if routable := err == nil; routable != tc.Routable {
				observed = fmt.Sprintf("routable %v, corpus says %v", routable, tc.Routable)
			}
			checkPin(t, routingProjectionPins, tc.Name, observed)
		})
	}
}
