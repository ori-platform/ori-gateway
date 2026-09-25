// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// routingProjectionVectorPath is the draft corpus in a sibling ori-specs
// checkout, read in place until the contract merges and it is vendored.
var routingProjectionVectorPath = filepath.Join("..", "..", "..", "ori-specs", "evidence-exchange", "vectors", "routing-projection.json")

// TestRoutingProjectionVectors holds both hops to the evidence-exchange/v1
// stable routing projection: routability is decided by the routing fields
// alone, never by a declared version or by fields a later version adds.
func TestRoutingProjectionVectors(t *testing.T) {
	raw, err := os.ReadFile(routingProjectionVectorPath)
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
			Routable     bool            `json:"routable"`
			Missing      string          `json:"missing"`
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
	for _, tc := range doc.Cases {
		var err error
		switch tc.ArtifactType {
		case string(ArtifactDeliveryEnvelope), string(ArtifactCheckpoint), string(ArtifactAnchorRegistration):
			err = validateArtifactRoutingFields(ArtifactType(tc.ArtifactType), tc.Artifact)
		case string(AuthorityDeliveryReceipt), string(AuthorityEpochConfirmation):
			_, err = authorityRoutingProjection(AuthorityArtifactType(tc.ArtifactType), tc.Artifact)
		default:
			t.Fatalf("%s: artifact type %q is not carried on the evidence hop", tc.Name, tc.ArtifactType)
		}
		if routable := err == nil; routable != tc.Routable {
			t.Errorf("%s: routable = %v (%v), corpus says %v (missing %q)", tc.Name, routable, err, tc.Routable, tc.Missing)
		}
	}
}
