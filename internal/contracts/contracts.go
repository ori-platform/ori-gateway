// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import "fmt"

const (
	GatewayHealthTopic = "ori/gateway/health"

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

type Heartbeat struct {
	Status       string  `json:"status"`
	UptimeS      float64 `json:"uptime_s"`
	Provider     string  `json:"provider"`
	SIMAvailable bool    `json:"sim_available"`
	TimestampMS  int64   `json:"timestamp_ms"`
}

func RequestTopic(deviceID string) (string, error) {
	if deviceID == "" {
		return "", fmt.Errorf("device_id must not be empty")
	}
	return fmt.Sprintf("ori/%s/reasoning/request", deviceID), nil
}

func ResponseTopic(deviceID string) (string, error) {
	if deviceID == "" {
		return "", fmt.Errorf("device_id must not be empty")
	}
	return fmt.Sprintf("ori/%s/reasoning/response", deviceID), nil
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
	if req.RequestID == "" {
		return fmt.Errorf("request_id must not be empty")
	}
	if req.DeviceID == "" {
		return fmt.Errorf("device_id must not be empty")
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
