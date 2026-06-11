// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"fmt"
	"strings"
)

const (
	GatewayHealthTopic                 = "ori/gateway/health"
	GatewayReasoningRequestTopicFilter = "ori/+/reasoning/request"
	HeartbeatMessageType               = "gateway.heartbeat"
	HeartbeatAuthScheme                = "hmac-sha256"

	ActionTierA = "A"
	ActionTierB = "B"
	ActionTierC = "C"
	ActionTierD = "D"
)

type HistoryPoint struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

type ReasoningContext struct {
	Value     float64        `json:"value"`
	Unit      string         `json:"unit"`
	Timestamp int64          `json:"timestamp"`
	History   []HistoryPoint `json:"history"`
}

type ReasoningRequest struct {
	RequestID      string           `json:"request_id"`
	DeviceID       string           `json:"device_id"`
	SensorType     string           `json:"sensor_type"`
	TriggerName    string           `json:"trigger_name"`
	Prompt         string           `json:"prompt"`
	Context        ReasoningContext `json:"context"`
	ActionTierHint string           `json:"action_tier_hint"`
	TimeoutMS      int              `json:"timeout_ms"`
}

type ReasoningResponse struct {
	RequestID      string  `json:"request_id"`
	Text           string  `json:"text"`
	Model          string  `json:"model"`
	TokensUsed     int     `json:"tokens_used"`
	LatencyMS      int64   `json:"latency_ms"`
	Confidence     float64 `json:"confidence"`
	ActionTier     string  `json:"action_tier"`
	ProposedAction *string `json:"proposed_action"`
	Error          *string `json:"error,omitempty"`
}

type HeartbeatAuth struct {
	Scheme     string `json:"scheme"`
	SignedAtMS int64  `json:"signed_at_ms"`
	Signature  string `json:"signature"`
}

type Heartbeat struct {
	Status       string         `json:"status"`
	UptimeS      float64        `json:"uptime_s"`
	Provider     string         `json:"provider"`
	SIMAvailable bool           `json:"sim_available"`
	TimestampMS  int64          `json:"timestamp_ms"`
	Auth         *HeartbeatAuth `json:"auth,omitempty"`
}

func RequestTopic(deviceID string) (string, error) {
	if err := validateMQTTDeviceID(deviceID); err != nil {
		return "", err
	}
	return fmt.Sprintf("ori/%s/reasoning/request", deviceID), nil
}

func ResponseTopic(deviceID string) (string, error) {
	if err := validateMQTTDeviceID(deviceID); err != nil {
		return "", err
	}
	return fmt.Sprintf("ori/%s/reasoning/response", deviceID), nil
}

func ExportRequestTopic(deviceID string) (string, error) {
	if err := validateMQTTDeviceID(deviceID); err != nil {
		return "", err
	}
	return fmt.Sprintf("ori/%s/export/request", deviceID), nil
}

func ExportResponseTopic(deviceID string, requestID string) (string, error) {
	if err := validateMQTTDeviceID(deviceID); err != nil {
		return "", err
	}
	if err := validateMQTTRequestID(requestID); err != nil {
		return "", err
	}
	return fmt.Sprintf("ori/%s/export/response/%s", deviceID, requestID), nil
}

func ExportResponseTopicFilter(deviceID string) (string, error) {
	if err := validateMQTTDeviceID(deviceID); err != nil {
		return "", err
	}
	return fmt.Sprintf("ori/%s/export/response/+", deviceID), nil
}

func validateMQTTDeviceID(deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device_id must not be empty")
	}
	if strings.TrimSpace(deviceID) != deviceID {
		return fmt.Errorf("device_id must not contain leading or trailing whitespace")
	}
	if strings.ContainsAny(deviceID, "/+#") {
		return fmt.Errorf("device_id must not contain MQTT topic separators or wildcards")
	}
	return nil
}

func validateMQTTRequestID(requestID string) error {
	if requestID == "" {
		return fmt.Errorf("request_id must not be empty")
	}
	if strings.TrimSpace(requestID) != requestID {
		return fmt.Errorf("request_id must not contain leading or trailing whitespace")
	}
	if strings.ContainsAny(requestID, "/+#") {
		return fmt.Errorf("request_id must not contain MQTT topic separators or wildcards")
	}
	for _, ch := range requestID {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			continue
		}
		return fmt.Errorf("request_id must contain only ASCII letters, digits, hyphen, or underscore")
	}
	return nil
}

func NewErrorResponse(requestID string, actionTier string, message string) ReasoningResponse {
	return ReasoningResponse{
		RequestID:      requestID,
		Text:           "",
		Model:          "gateway",
		TokensUsed:     0,
		LatencyMS:      0,
		Confidence:     0,
		ActionTier:     actionTier,
		ProposedAction: nil,
		Error:          &message,
	}
}

func IsValidActionTier(tier string) bool {
	switch tier {
	case ActionTierA, ActionTierB, ActionTierC, ActionTierD:
		return true
	default:
		return false
	}
}

func ValidateRequest(req ReasoningRequest) error {
	if err := validateMQTTRequestID(req.RequestID); err != nil {
		return fmt.Errorf("request_id: %w", err)
	}
	if err := validateMQTTDeviceID(req.DeviceID); err != nil {
		return err
	}
	if req.Prompt == "" {
		return fmt.Errorf("prompt must not be empty")
	}
	if !IsValidActionTier(req.ActionTierHint) {
		return fmt.Errorf("action_tier_hint must be A, B, C, or D")
	}
	return nil
}

func ValidateResponseForRequest(req ReasoningRequest, resp ReasoningResponse) error {
	if req.RequestID != resp.RequestID {
		return fmt.Errorf("response request_id %q does not match request_id %q", resp.RequestID, req.RequestID)
	}
	if !IsValidActionTier(resp.ActionTier) {
		return fmt.Errorf("action_tier must be A, B, C, or D")
	}
	return nil
}
