// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import "errors"

var (
	ErrNotConnected       = errors.New("broker: not connected")
	ErrAlreadyConnected   = errors.New("broker: already connected")
	ErrSubscriptionExists = errors.New("broker: subscription already exists with a different handler")
	ErrInvalidQoS         = errors.New("broker: qos must be 0, 1, or 2")
	ErrEmptyTopic         = errors.New("broker: topic must not be empty")
	ErrEmptyClientID      = errors.New("broker: client id must not be empty")
	ErrEmptyBrokerURL     = errors.New("broker: broker url must not be empty")
)
