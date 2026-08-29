// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"encoding/json"
	"fmt"
)

// DurableAuthoritySink stages exact authority artifacts in a queue separate
// from outbound evidence. Store returning nil is the durability boundary the
// delivery worker requires before retiring its outbound entry.
type DurableAuthoritySink struct {
	queue *DurableQueue
}

func OpenDurableAuthoritySink(opts QueueOptions) (*DurableAuthoritySink, error) {
	queue, err := openDurableQueue(opts)
	if err != nil {
		return nil, err
	}
	for _, id := range queue.order {
		if record := queue.entries[id]; record.Type != artifactDeliveryReceipt && record.Type != artifactEpochConfirmation && record.Type != artifactCustodyAcknowledgement {
			return nil, fmt.Errorf("evidence: authority return queue contains an outbound artifact")
		}
	}
	return &DurableAuthoritySink{queue: queue}, nil
}

func (s *DurableAuthoritySink) Store(_ context.Context, artifact AuthorityArtifact) error {
	if s == nil || s.queue == nil {
		return fmt.Errorf("evidence: authority return sink is not configured")
	}
	var kind ArtifactType
	switch artifact.Type {
	case AuthorityDeliveryReceipt:
		kind = artifactDeliveryReceipt
	case AuthorityEpochConfirmation:
		kind = artifactEpochConfirmation
	case InboundCustodyAcknowledgement:
		kind = artifactCustodyAcknowledgement
	default:
		return fmt.Errorf("evidence: unsupported authority artifact type %q", artifact.Type)
	}
	var routing struct {
		V        int    `json:"v"`
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(artifact.Payload, &routing); err != nil || routing.V != 1 || routing.DeviceID == "" || routing.DeviceID != artifact.DeviceID {
		return fmt.Errorf("evidence: authority artifact routing mismatch")
	}
	_, err := s.queue.enqueue(kind, artifact.Payload)
	return err
}

func (s *DurableAuthoritySink) Len() int {
	if s == nil || s.queue == nil {
		return 0
	}
	return s.queue.Len()
}
