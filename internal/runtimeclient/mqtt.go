// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const (
	exportTypeHealth           = "health"
	exportTypeSensorHistory    = "sensor_history"
	exportTypeActionLog        = "action_log"
	exportTypeTierCDecisionLog = "tier_c_decision_log"
	exportTypeReasoningLog     = "reasoning_log"
	maxExportPages             = 100
)

type mqttBroker interface {
	Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

type requestIDFunc func() (string, error)

// MQTTClient implements Client over runtime MQTT export request/response topics.
type MQTTClient struct {
	broker    mqttBroker
	qos       byte
	requestID requestIDFunc

	subscribeMu sync.Mutex
	mu          sync.Mutex
	pending     map[string]chan exportResponse
	subscribed  map[string]bool
}

// MQTTClientOption customises MQTTClient construction.
type MQTTClientOption func(*MQTTClient)

// WithRequestIDFunc overrides request id generation. It is intended for tests.
func WithRequestIDFunc(fn requestIDFunc) MQTTClientOption {
	return func(c *MQTTClient) {
		if fn != nil {
			c.requestID = fn
		}
	}
}

// NewMQTTClient constructs a runtime export client backed by the gateway broker.
func NewMQTTClient(b mqttBroker, opts ...MQTTClientOption) (*MQTTClient, error) {
	if b == nil {
		return nil, fmt.Errorf("runtimeclient: broker must not be nil")
	}
	client := &MQTTClient{
		broker:     b,
		qos:        broker.QoSReasoning,
		requestID:  randomRequestID,
		pending:    make(map[string]chan exportResponse),
		subscribed: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

func (c *MQTTClient) Health(ctx context.Context, req HealthRequest) (HealthSnapshot, error) {
	normalized, err := NormalizeHealthRequest(req)
	if err != nil {
		return HealthSnapshot{}, err
	}
	items, err := c.exportAll(ctx, exportRequest{DeviceID: normalized.DeviceID, ExportType: exportTypeHealth})
	if err != nil {
		return HealthSnapshot{}, err
	}
	if len(items) == 0 {
		return HealthSnapshot{}, fmt.Errorf("runtimeclient: health export returned no items")
	}
	return mapHealthSnapshot(items[0], normalized.DeviceID)
}

func (c *MQTTClient) SensorHistory(ctx context.Context, req SensorHistoryRequest) ([]SensorAggregate, error) {
	normalized, err := NormalizeSensorHistoryRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeSensorHistory,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
		Params: map[string]any{
			"sensor_id": normalized.SensorID,
			"bucket_ms": normalized.BucketMS,
		},
	})
	if err != nil {
		return nil, err
	}
	out := make([]SensorAggregate, 0, len(items))
	for _, item := range items {
		row, err := mapSensorAggregate(item, normalized.DeviceID, normalized.SensorID)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) ActionLog(ctx context.Context, req ActionLogRequest) ([]ActionLogEntry, error) {
	normalized, err := NormalizeActionLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeActionLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ActionLogEntry, 0, len(items))
	for _, item := range items {
		row := mapActionLogEntry(item, normalized.DeviceID)
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) TierCDecisionLog(ctx context.Context, req TierCDecisionLogRequest) ([]TierCDecisionEntry, error) {
	normalized, err := NormalizeTierCDecisionLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeTierCDecisionLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]TierCDecisionEntry, 0, len(items))
	for _, item := range items {
		row, err := mapTierCDecisionEntry(item, normalized.DeviceID)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) ReasoningLog(ctx context.Context, req ReasoningLogRequest) ([]ReasoningLogEntry, error) {
	normalized, err := NormalizeReasoningLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeReasoningLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
		Params:     reasoningLogParams(normalized),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ReasoningLogEntry, 0, len(items))
	for _, item := range items {
		out = append(out, mapReasoningLogEntry(item, normalized.DeviceID))
	}
	return out, nil
}

func reasoningLogParams(req ReasoningLogRequest) map[string]any {
	params := map[string]any{}
	if req.TierUsed != "" {
		params["tier_used"] = req.TierUsed
	}
	if req.ActionTier != "" {
		params["action_tier"] = req.ActionTier
	}
	if req.ReasoningStatus != "" {
		params["reasoning_status"] = req.ReasoningStatus
	}
	if req.CorrelationID != "" {
		params["correlation_id"] = req.CorrelationID
	}
	if len(params) == 0 {
		return nil
	}
	return params
}

func (c *MQTTClient) exportAll(ctx context.Context, base exportRequest) ([]map[string]any, error) {
	if err := c.ensureResponseSubscription(ctx, base.DeviceID); err != nil {
		return nil, err
	}

	var all []map[string]any
	pageToken := ""
	seenPageTokens := make(map[string]bool)
	for page := 0; page < maxExportPages; page++ {
		base.PageToken = pageToken
		resp, err := c.requestPage(ctx, base)
		if err != nil {
			return nil, err
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("runtimeclient: runtime export %s failed: %s", base.ExportType, resp.Error)
		}
		if len(all)+len(resp.Items) > MaxLimit {
			return nil, fmt.Errorf("runtimeclient: runtime export %s exceeded max item limit %d", base.ExportType, MaxLimit)
		}
		all = append(all, resp.Items...)
		if resp.Complete {
			return all, nil
		}
		if resp.NextPageToken == "" {
			return nil, fmt.Errorf("runtimeclient: runtime export %s incomplete without next_page_token", base.ExportType)
		}
		if seenPageTokens[resp.NextPageToken] {
			return nil, fmt.Errorf("runtimeclient: runtime export %s repeated next_page_token %q", base.ExportType, resp.NextPageToken)
		}
		seenPageTokens[resp.NextPageToken] = true
		pageToken = resp.NextPageToken
	}
	return nil, fmt.Errorf("runtimeclient: runtime export %s exceeded max page limit %d", base.ExportType, maxExportPages)
}

func (c *MQTTClient) ensureResponseSubscription(ctx context.Context, deviceID string) error {
	topic, err := contracts.ExportResponseTopicFilter(deviceID)
	if err != nil {
		return err
	}

	c.subscribeMu.Lock()
	defer c.subscribeMu.Unlock()

	c.mu.Lock()
	if c.subscribed[topic] {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.broker.Subscribe(ctx, topic, c.qos, c.handleResponseMessage); err != nil {
		return fmt.Errorf("runtimeclient: subscribe export response: %w", err)
	}

	c.mu.Lock()
	c.subscribed[topic] = true
	c.mu.Unlock()
	return nil
}

func (c *MQTTClient) requestPage(ctx context.Context, req exportRequest) (exportResponse, error) {
	requestID, err := c.requestID()
	if err != nil {
		return exportResponse{}, err
	}
	if requestID == "" {
		return exportResponse{}, fmt.Errorf("runtimeclient: request_id generator returned empty id")
	}
	if _, err := contracts.ExportResponseTopic(req.DeviceID, requestID); err != nil {
		return exportResponse{}, err
	}
	req.RequestID = requestID

	responseCh := make(chan exportResponse, 1)
	c.mu.Lock()
	if _, exists := c.pending[requestID]; exists {
		c.mu.Unlock()
		return exportResponse{}, fmt.Errorf("runtimeclient: duplicate request_id %q", requestID)
	}
	c.pending[requestID] = responseCh
	c.mu.Unlock()
	defer c.removePending(requestID)

	payload, err := json.Marshal(req)
	if err != nil {
		return exportResponse{}, err
	}
	topic, err := contracts.ExportRequestTopic(req.DeviceID)
	if err != nil {
		return exportResponse{}, err
	}
	if err := c.broker.Publish(ctx, topic, c.qos, false, payload); err != nil {
		return exportResponse{}, fmt.Errorf("runtimeclient: publish export request: %w", err)
	}

	select {
	case <-ctx.Done():
		return exportResponse{}, ctx.Err()
	case resp := <-responseCh:
		if resp.RequestID != requestID {
			return exportResponse{}, fmt.Errorf("runtimeclient: response request_id %q does not match %q", resp.RequestID, requestID)
		}
		if resp.DeviceID != "" && resp.DeviceID != req.DeviceID {
			return exportResponse{}, fmt.Errorf("runtimeclient: response device_id %q does not match %q", resp.DeviceID, req.DeviceID)
		}
		if resp.ExportType != "" && resp.ExportType != req.ExportType {
			return exportResponse{}, fmt.Errorf("runtimeclient: response export_type %q does not match %q", resp.ExportType, req.ExportType)
		}
		return resp, nil
	}
}

func (c *MQTTClient) handleResponseMessage(_ string, payload []byte) {
	var resp exportResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return
	}
	if resp.RequestID == "" {
		return
	}

	c.mu.Lock()
	ch := c.pending[resp.RequestID]
	c.mu.Unlock()
	if ch == nil {
		return
	}

	select {
	case ch <- resp:
	default:
	}
}

func (c *MQTTClient) removePending(requestID string) {
	c.mu.Lock()
	delete(c.pending, requestID)
	c.mu.Unlock()
}

type exportRequest struct {
	RequestID  string         `json:"request_id"`
	ExportType string         `json:"export_type"`
	DeviceID   string         `json:"device_id"`
	SinceMS    int64          `json:"since_ms,omitempty"`
	UntilMS    int64          `json:"until_ms,omitempty"`
	Limit      int            `json:"limit,omitempty"`
	PageToken  string         `json:"page_token,omitempty"`
	Params     map[string]any `json:"params,omitempty"`
}

type exportResponse struct {
	RequestID     string           `json:"request_id"`
	ExportType    string           `json:"export_type"`
	DeviceID      string           `json:"device_id"`
	Items         []map[string]any `json:"items"`
	NextPageToken string           `json:"next_page_token"`
	Complete      bool             `json:"complete"`
	Error         string           `json:"error"`
}

func randomRequestID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("runtimeclient: generate request_id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func mapHealthSnapshot(item map[string]any, fallbackDeviceID string) (HealthSnapshot, error) {
	status := stringValue(item, "status", "")
	if status == "" {
		status = "ok"
	}
	lastReadingMS := int64Value(item, "last_reading_ms")
	if lastReadingMS == 0 {
		lastReadingMS = latestSensorSeenMS(item["sensors"])
	}
	gatewaySeen := boolValue(item, "gateway_seen")
	if posture, ok := item["capability_posture"].(map[string]any); ok {
		gatewaySeen = gatewaySeen || boolValue(posture, "gateway_reachable")
	}
	policyStatus := stringValue(item, "policy_status", "")
	if policy, ok := item["device_policy"].(map[string]any); ok {
		policyStatus = stringValue(policy, "tier", policyStatus)
		if policyStatus == "" && boolValue(policy, "enabled") {
			policyStatus = "enabled"
		}
	}
	return HealthSnapshot{
		DeviceID:             stringValue(item, "device_id", fallbackDeviceID),
		Status:               status,
		UptimeS:              floatValue(item, "uptime_s"),
		LastReadingMS:        lastReadingMS,
		GatewaySeen:          gatewaySeen,
		PolicyStatus:         policyStatus,
		GatewayBrokerPosture: mapGatewayBrokerPosture(item),
		StateStoreEncryption: mapStateStoreEncryptionPosture(item),
		AlertOutbox:          mapAlertOutboxPosture(item),
		LockoutRiskLevels:    mapLockoutRiskLevels(item),
	}, nil
}

func mapGatewayBrokerPosture(item map[string]any) *GatewayBrokerPosture {
	posture, ok := subMap(item, "gateway_broker_posture")
	if !ok {
		return nil
	}
	return &GatewayBrokerPosture{
		Available:             boolValue(posture, "available"),
		GatewayEnabled:        boolValue(posture, "gateway_enabled"),
		DeploymentCheck:       stringValue(posture, "deployment_check", ""),
		AnonymousAccess:       stringValue(posture, "anonymous_access", ""),
		ACLPolicy:             stringValue(posture, "acl_policy", ""),
		RequireCredentials:    boolValue(posture, "require_credentials"),
		CredentialsConfigured: boolValue(posture, "credentials_configured"),
		RequiresACLHardening:  boolValue(posture, "requires_acl_hardening"),
	}
}

func mapStateStoreEncryptionPosture(item map[string]any) *StateStoreEncryptionPosture {
	posture, ok := subMap(item, "state_store_encryption")
	if !ok {
		return nil
	}
	return &StateStoreEncryptionPosture{
		Available:            boolValue(posture, "available"),
		Mode:                 stringValue(posture, "mode", ""),
		Satisfied:            boolValue(posture, "satisfied"),
		MarkerConfigured:     boolValue(posture, "marker_configured"),
		PathPrefixConfigured: boolValue(posture, "path_prefix_configured"),
	}
}

func mapAlertOutboxPosture(item map[string]any) *AlertOutboxPosture {
	posture, ok := subMap(item, "alert_outbox")
	if !ok {
		return nil
	}
	return &AlertOutboxPosture{
		Available:                     true,
		BacklogCount:                  intValue(posture, "backlog_count"),
		OldestQueuedOriginalMS:        int64Value(posture, "oldest_queued_original_ts"),
		OldestQueuedAgeMS:             int64Value(posture, "oldest_queued_age_ms"),
		RetryIntervalMinutes:          floatValue(posture, "retry_interval_minutes"),
		MaxNonTierDAttempts:           intValue(posture, "max_non_tier_d_attempts"),
		TierDCriticalWarningThreshold: intValue(posture, "tier_d_critical_warning_threshold"),
		BatchSize:                     intValue(posture, "batch_size"),
	}
}

func subMap(item map[string]any, key string) (map[string]any, bool) {
	value, ok := item[key]
	if !ok || value == nil {
		return nil, false
	}
	out, ok := value.(map[string]any)
	return out, ok
}

func mapSensorAggregate(item map[string]any, fallbackDeviceID string, fallbackSensorID string) (SensorAggregate, error) {
	timestamp := int64Value(item, "timestamp")
	startMS := int64Value(item, "start_ms")
	if startMS == 0 {
		startMS = timestamp
	}
	endMS := int64Value(item, "end_ms")
	if endMS == 0 {
		endMS = timestamp
	}
	avgValue := floatValue(item, "avg_value")
	if v, ok := item["avg_value"]; !ok || v == nil {
		avgValue = floatValue(item, "value")
	}
	return SensorAggregate{
		DeviceID: stringValue(item, "device_id", fallbackDeviceID),
		SensorID: stringValue(item, "sensor_id", fallbackSensorID),
		Type:     stringValue(item, "sensor_type", ""),
		Unit:     stringValue(item, "unit", ""),
		StartMS:  startMS,
		EndMS:    endMS,
		AvgValue: avgValue,
		MinValue: floatValue(item, "min_value"),
		MaxValue: floatValue(item, "max_value"),
		Samples:  intValue(item, "sample_count"),
		Quality:  floatValue(item, "quality"),
		Metadata: mapValue(item, "metadata"),
	}, nil
}

func mapActionLogEntry(item map[string]any, fallbackDeviceID string) ActionLogEntry {
	return ActionLogEntry{
		DeviceID:        stringValue(item, "device_id", fallbackDeviceID),
		CreatedAtMS:     int64Value(item, "timestamp"),
		ActionName:      stringValue(item, "action_name", ""),
		Tier:            stringValue(item, "tier", ""),
		SkillName:       stringValue(item, "skill_name", ""),
		TriggerName:     stringValue(item, "trigger_name", ""),
		SensorType:      stringValue(item, "sensor_type", ""),
		SafeDefaultUsed: boolValue(item, "safe_default_used"),
		Success:         boolValue(item, "executed"),
		Result:          mapValue(item, "result"),
	}
}

func mapTierCDecisionEntry(item map[string]any, fallbackDeviceID string) (TierCDecisionEntry, error) {
	history, err := mapHistoryWindow(item["history_window"])
	if err != nil {
		return TierCDecisionEntry{}, err
	}
	return TierCDecisionEntry{
		DeviceID:          stringValue(item, "device_id", fallbackDeviceID),
		ProposalID:        stringValue(item, "proposal_id", ""),
		CreatedAtMS:       int64Value(item, "created_at"),
		SkillName:         stringValue(item, "skill_name", ""),
		TriggerName:       stringValue(item, "trigger_name", ""),
		SensorID:          stringValue(item, "sensor_id", ""),
		SensorType:        stringValue(item, "sensor_type", ""),
		ReadingValue:      floatValue(item, "reading_value"),
		HistoryWindow:     history,
		ProposedAction:    stringValue(item, "proposed_action", ""),
		Confidence:        floatValue(item, "confidence"),
		OperatorDecision:  stringValue(item, "operator_decision", ""),
		DecisionLatencyS:  floatValue(item, "decision_latency_s"),
		SafeDefaultUsed:   boolValue(item, "safe_default_used"),
		FinalActionResult: mapValue(item, "final_action_result"),
		SiteType:          stringValue(item, "site_type", ""),
		Location:          stringValue(item, "location", ""),
		Timezone:          stringValue(item, "timezone", ""),
		LaterOutcome:      mapValue(item, "later_outcome"),
	}, nil
}

func mapReasoningLogEntry(item map[string]any, fallbackDeviceID string) ReasoningLogEntry {
	createdAtMS := int64Value(item, "created_at_ms")
	if createdAtMS == 0 {
		createdAtMS = int64Value(item, "created_at", int64Value(item, "timestamp"))
	}
	return ReasoningLogEntry{
		DeviceID:        stringValue(item, "device_id", fallbackDeviceID),
		SkillName:       stringValue(item, "skill_name", ""),
		TriggerName:     stringValue(item, "trigger_name", ""),
		SensorID:        stringValue(item, "sensor_id", ""),
		SensorType:      stringValue(item, "sensor_type", ""),
		TierUsed:        stringValue(item, "tier_used", ""),
		Model:           stringValue(item, "model", ""),
		PromptText:      stringValue(item, "prompt_text", ""),
		ReasoningText:   stringValue(item, "reasoning_text", ""),
		Confidence:      floatValue(item, "confidence"),
		ProposedAction:  stringValue(item, "proposed_action", ""),
		ActionTier:      stringValue(item, "action_tier", ""),
		TokenCount:      intValue(item, "token_count"),
		LatencyMS:       int64Value(item, "latency_ms"),
		ReasoningStatus: stringValue(item, "reasoning_status", ""),
		CorrelationID:   stringValue(item, "correlation_id", ""),
		CreatedAtMS:     createdAtMS,
	}
}

func mapLockoutRiskLevels(item map[string]any) map[string]LockoutRiskState {
	if raw, ok := item["lockout_risk_levels"].(map[string]any); ok && len(raw) > 0 {
		out := make(map[string]LockoutRiskState, len(raw))
		for key, value := range raw {
			m, ok := value.(map[string]any)
			if !ok {
				continue
			}
			out[key] = lockoutRiskStateFromMap(m)
		}
		return out
	}

	lockout, ok := item["remote_command_lockout"].(map[string]any)
	if !ok {
		return nil
	}
	senders, ok := lockout["senders"].([]any)
	if !ok || len(senders) == 0 {
		return nil
	}
	out := make(map[string]LockoutRiskState, len(senders))
	for _, value := range senders {
		m, ok := value.(map[string]any)
		if !ok {
			continue
		}
		key := stringValue(m, "channel", "") + ":" + stringValue(m, "from_number", "")
		out[key] = lockoutRiskStateFromMap(m)
	}
	return out
}

func lockoutRiskStateFromMap(m map[string]any) LockoutRiskState {
	return LockoutRiskState{
		RiskLevel:   stringValue(m, "risk_level", ""),
		LockedOut:   boolValue(m, "locked_out"),
		Stale:       boolValue(m, "stale"),
		CheckedAtMS: int64Value(m, "checked_at_ms"),
	}
}

func latestSensorSeenMS(value any) int64 {
	items, ok := value.([]any)
	if !ok {
		return 0
	}
	var latest int64
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		seen := int64Value(m, "last_seen_ms")
		if seen > latest {
			latest = seen
		}
	}
	return latest
}

func mapHistoryWindow(value any) ([]HistorySample, error) {
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("runtimeclient: history_window must be an array")
	}
	out := make([]HistorySample, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("runtimeclient: history_window item must be an object")
		}
		out = append(out, HistorySample{
			SensorID:    stringValue(m, "sensor_id", ""),
			SensorType:  stringValue(m, "sensor_type", ""),
			Unit:        stringValue(m, "unit", ""),
			TimestampMS: int64Value(m, "timestamp", int64Value(m, "timestamp_ms")),
			Value:       floatValue(m, "value"),
			Quality:     floatValue(m, "quality"),
		})
	}
	return out, nil
}

func stringValue(m map[string]any, key string, fallback string) string {
	value, ok := m[key]
	if !ok || value == nil {
		return fallback
	}
	switch v := value.(type) {
	case string:
		if v == "" {
			return fallback
		}
		return v
	default:
		return fmt.Sprint(v)
	}
}

func intValue(m map[string]any, key string) int {
	return int(int64Value(m, key))
}

func int64Value(m map[string]any, key string, fallback ...int64) int64 {
	var def int64
	if len(fallback) > 0 {
		def = fallback[0]
	}
	value, ok := m[key]
	if !ok || value == nil {
		return def
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		parsed, err := v.Int64()
		if err == nil {
			return parsed
		}
		f, err := v.Float64()
		if err == nil {
			return int64(f)
		}
	}
	return def
}

func floatValue(m map[string]any, key string) float64 {
	value, ok := m[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case json.Number:
		parsed, err := v.Float64()
		if err == nil {
			return parsed
		}
	}
	return 0
}

func boolValue(m map[string]any, key string) bool {
	value, ok := m[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int:
		return v != 0
	}
	return false
}

func mapValue(m map[string]any, key string) map[string]any {
	value, ok := m[key]
	if !ok || value == nil {
		return nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = v
	}
	return out
}
