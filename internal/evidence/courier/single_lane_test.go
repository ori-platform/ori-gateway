// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import "context"

// deliverHead offers the head of the lane holding the whole queue's oldest
// artifact. It is a convenience for a queue with one lane.
func (w *DeliveryWorker) deliverHead(ctx context.Context) (bool, error) {
	head, ok := w.queue.Peek()
	if !ok {
		return false, nil
	}
	return w.lane(artifactLane(head)).deliverHead(ctx)
}

// recordFailure records err against the lane holding the oldest artifact. It
// is a convenience for a queue with one lane.
func (w *DeliveryWorker) recordFailure(err error) {
	head, ok := w.queue.Peek()
	if !ok {
		return
	}
	w.lane(artifactLane(head)).recordFailure(err)
}

func artifactLane(queued QueuedArtifact) LaneKey {
	return LaneKey{Device: recordDevice(queued.Payload), Lane: LaneOf(queued.Type)}
}
