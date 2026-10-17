// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"math/bits"
	"strings"
)

// Topic families of gateway-api/v2 Payload Size: a row of its table, named by
// its topic with the {device_id} and {request_id} segments left as written.
const (
	FamilyGatewayHealth       = "ori/gateway/health"
	FamilyRuntimeHeartbeat    = "ori/{device_id}/runtime/heartbeat"
	FamilyReasoningRequest    = "ori/{device_id}/reasoning/request"
	FamilyReasoningResponse   = "ori/{device_id}/reasoning/response"
	FamilyEnrichmentRequest   = "ori/{device_id}/tier_c/enrichment/request"
	FamilyEnrichmentResponse  = "ori/{device_id}/tier_c/enrichment/response"
	FamilyExportRequest       = "ori/{device_id}/export/request"
	FamilyExportResponse      = "ori/{device_id}/export/response/{request_id}"
	FamilyEvidenceOutbound    = "ori/{device_id}/evidence/outbound"
	FamilyEvidenceOutboundAck = "ori/{device_id}/evidence/outbound/ack"
	FamilyEvidenceInbound     = "ori/{device_id}/evidence/inbound"
	FamilyEvidenceInboundAck  = "ori/{device_id}/evidence/inbound/ack"
)

// PayloadMaxima is the Payload Size table: each family's maximum payload
// length in bytes, as delivered. The gateway offers no setting for a maximum,
// so nothing raises or lowers one.
var PayloadMaxima = map[string]int{
	FamilyGatewayHealth:       16384,
	FamilyRuntimeHeartbeat:    65536,
	FamilyReasoningRequest:    262144,
	FamilyReasoningResponse:   262144,
	FamilyEnrichmentRequest:   262144,
	FamilyEnrichmentResponse:  262144,
	FamilyExportRequest:       16384,
	FamilyExportResponse:      4194304,
	FamilyEvidenceOutbound:    1572864,
	FamilyEvidenceOutboundAck: 16384,
	FamilyEvidenceInbound:     1114112,
	FamilyEvidenceInboundAck:  16384,
}

// ReceivedFamilies are the families the gateway receives, in table order: the
// members of the site health mqtt_ingress posture.
var ReceivedFamilies = []string{
	FamilyRuntimeHeartbeat,
	FamilyReasoningRequest,
	FamilyEnrichmentRequest,
	FamilyExportResponse,
	FamilyEvidenceOutbound,
	FamilyEvidenceInboundAck,
}

const (
	// PayloadFloor is the lowest maximum a deployment may configure; every
	// required reply fits it.
	PayloadFloor = 4096
	// MaxAuthorityArtifactBytes bounds an inbound authority artifact's
	// canonical bytes, as evidence-transport/v2 bounds an outbound artifact.
	MaxAuthorityArtifactBytes = 1048576
	// MaxErrorScalars bounds the Unicode scalar values of an error response's
	// error member.
	MaxErrorScalars = 128
	// ErrorResponseTooLarge replaces a reasoning or enrichment response over
	// its topic's maximum.
	ErrorResponseTooLarge = "response_too_large"
	// maxRoutingID bounds a device_id or request_id on this hop.
	maxRoutingID = 128
)

// PayloadFamily returns the family of a concrete topic or a subscription
// filter, matching a {device_id} or {request_id} segment against any one
// segment, a "+" wildcard included. A topic outside the table has none.
func PayloadFamily(topic string) (string, bool) {
	segments := strings.Split(topic, "/")
	for family := range PayloadMaxima {
		pattern := strings.Split(family, "/")
		if len(pattern) != len(segments) {
			continue
		}
		matched := true
		for i, want := range pattern {
			if strings.HasPrefix(want, "{") {
				if segments[i] == "" || segments[i] == "#" {
					matched = false
					break
				}
				continue
			}
			if segments[i] != want {
				matched = false
				break
			}
		}
		if matched {
			return family, true
		}
	}
	return "", false
}

// PayloadMaximum returns the maximum payload length for a topic or filter.
func PayloadMaximum(topic string) (int, bool) {
	family, ok := PayloadFamily(topic)
	if !ok {
		return 0, false
	}
	return PayloadMaxima[family], true
}

// SizeClass is the least power of two not below length: all a size refusal
// records of a payload.
func SizeClass(length int) int {
	if length <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(length-1))
}

// ValidRoutingID reports whether value is a request_id this hop admits, 1 to
// 128 characters from [A-Za-z0-9._-]; runtime-config/v2 holds a runtime's
// device.id to the same form.
func ValidRoutingID(value string) bool {
	if value == "" || len(value) > maxRoutingID {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

// BoundedError cuts message to the scalars an error response's error member
// may carry.
func BoundedError(message string) string {
	count := 0
	for i := range message {
		if count == MaxErrorScalars {
			return message[:i]
		}
		count++
	}
	return message
}
