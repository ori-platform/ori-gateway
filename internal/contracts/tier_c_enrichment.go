// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import "fmt"

// TierCEnrichmentRequest asks the gateway to improve operator-facing context
// for an already-created Tier C proposal. It is advisory only: runtime remains
// the authority for tier, action, safe default, approval, and execution.
type TierCEnrichmentRequest struct {
	RequestID         string                         `json:"request_id"`
	ProposalID        string                         `json:"proposal_id"`
	DeviceID          string                         `json:"device_id"`
	SkillName         string                         `json:"skill_name"`
	TriggerName       string                         `json:"trigger_name"`
	SensorID          string                         `json:"sensor_id"`
	SensorType        string                         `json:"sensor_type"`
	ReadingValue      float64                        `json:"reading_value"`
	Unit              string                         `json:"unit"`
	HistoryWindow     []TierCEnrichmentHistorySample `json:"history_window"`
	ProposedAction    string                         `json:"proposed_action"`
	SafeDefaultAction string                         `json:"safe_default_action"`
	OperatorMessage   string                         `json:"operator_message"`
	TimeoutMS         int                            `json:"timeout_ms"`
	Auth              *HeartbeatAuth                 `json:"auth,omitempty"`
}

// TierCEnrichmentHistorySample is the recent context used to explain a Tier C proposal.
type TierCEnrichmentHistorySample struct {
	SensorID    string  `json:"sensor_id"`
	SensorType  string  `json:"sensor_type"`
	Unit        string  `json:"unit"`
	TimestampMS int64   `json:"timestamp_ms"`
	Value       float64 `json:"value"`
	Quality     float64 `json:"quality"`
}

// TierCEnrichmentResponse contains only advisory text. It intentionally cannot
// change action tier, action name, safe default, approval requirement, or actuator settings.
type TierCEnrichmentResponse struct {
	RequestID                  string         `json:"request_id"`
	ProposalID                 string         `json:"proposal_id"`
	DeviceID                   string         `json:"device_id"`
	Explanation                string         `json:"explanation,omitempty"`
	EstimatedImpact            string         `json:"estimated_impact,omitempty"`
	RecommendedOperatorContext string         `json:"recommended_operator_context,omitempty"`
	Provider                   string         `json:"provider,omitempty"`
	Model                      string         `json:"model,omitempty"`
	TokensUsed                 int            `json:"tokens_used,omitempty"`
	LatencyMS                  int64          `json:"latency_ms,omitempty"`
	Error                      *string        `json:"error,omitempty"`
	Auth                       *HeartbeatAuth `json:"auth,omitempty"`
}

// NewTierCEnrichmentErrorResponse preserves correlation when enrichment fails.
func NewTierCEnrichmentErrorResponse(requestID string, proposalID string, deviceID string, message string) TierCEnrichmentResponse {
	if message == "" {
		message = "enrichment unavailable"
	}
	return TierCEnrichmentResponse{
		RequestID:  requestID,
		ProposalID: proposalID,
		DeviceID:   deviceID,
		Error:      &message,
	}
}

// ValidateTierCEnrichmentRequest checks the required fields and topic-safe ids.
func ValidateTierCEnrichmentRequest(req TierCEnrichmentRequest) error {
	if err := validateMQTTRequestID(req.RequestID); err != nil {
		return fmt.Errorf("request_id: %w", err)
	}
	if err := validateMQTTRequestID(req.ProposalID); err != nil {
		return fmt.Errorf("proposal_id: %w", err)
	}
	if err := validateMQTTDeviceID(req.DeviceID); err != nil {
		return err
	}
	if req.SkillName == "" {
		return fmt.Errorf("skill_name must not be empty")
	}
	if req.TriggerName == "" {
		return fmt.Errorf("trigger_name must not be empty")
	}
	if req.SensorID == "" {
		return fmt.Errorf("sensor_id must not be empty")
	}
	if req.SensorType == "" {
		return fmt.Errorf("sensor_type must not be empty")
	}
	if req.ProposedAction == "" {
		return fmt.Errorf("proposed_action must not be empty")
	}
	if req.SafeDefaultAction == "" {
		return fmt.Errorf("safe_default_action must not be empty")
	}
	if req.OperatorMessage == "" {
		return fmt.Errorf("operator_message must not be empty")
	}
	if req.TimeoutMS <= 0 {
		return fmt.Errorf("timeout_ms must be positive")
	}
	return nil
}

// ValidateTierCEnrichmentResponseForRequest checks response correlation and shape.
func ValidateTierCEnrichmentResponseForRequest(req TierCEnrichmentRequest, resp TierCEnrichmentResponse) error {
	if req.RequestID != resp.RequestID {
		return fmt.Errorf("response request_id %q does not match request_id %q", resp.RequestID, req.RequestID)
	}
	if req.ProposalID != resp.ProposalID {
		return fmt.Errorf("response proposal_id %q does not match proposal_id %q", resp.ProposalID, req.ProposalID)
	}
	if req.DeviceID != resp.DeviceID {
		return fmt.Errorf("response device_id %q does not match device_id %q", resp.DeviceID, req.DeviceID)
	}
	if resp.Error != nil {
		if *resp.Error == "" {
			return fmt.Errorf("error must not be empty when present")
		}
		return nil
	}
	if resp.Explanation == "" {
		return fmt.Errorf("explanation must not be empty")
	}
	return nil
}
