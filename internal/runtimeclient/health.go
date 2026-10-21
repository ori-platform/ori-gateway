// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"encoding/json"
	"sync"
)

// The severities a health export reads as (gateway-api/v2, Reading `status`
// while runtimes adopt runtime-health/v4). HealthSnapshot.Status is always
// one of them.
const (
	HealthStatusOK       = "ok"
	HealthStatusDegraded = "degraded"
	HealthStatusCritical = "critical"
	HealthStatusUnknown  = "unknown"
)

// healthProjectionMarker marks an item as runtime-health/v4's remote health
// projection. It is compared whole, never by prefix or after trimming.
const healthProjectionMarker = "runtime-health/v4/remote"

// projectionOnlyMembers are members no runtime release has sent outside the
// projection: an unmarked item carrying one is neither shape.
var projectionOnlyMembers = []string{"projection", "unknown_critical_blocks", "action_log_records", "safety_registry", "skill_hooks"}

// jsonKind is a JSON type as the member table states it.
type jsonKind int

const (
	kindString jsonKind = iota
	kindNumber
	kindBoolean
	kindArray
	kindObject
	kindObjectOrNull
)

// snapshotMember is one row of the pre-v4 snapshot member table.
type snapshotMember struct {
	name string
	kind jsonKind
}

// snapshotRequired are the members both pre-v4 encodings require.
var snapshotRequired = []snapshotMember{
	{"device_id", kindString},
	{"uptime_s", kindNumber},
	{"health_socket_path", kindString},
	{"sensors", kindArray},
	{"degradation_reasons", kindArray},
	{"safety", kindObjectOrNull},
	{"alert_outbox", kindObject},
	{"capability_posture", kindObject},
	{"commissioning", kindObject},
	{"community_skills", kindObject},
	{"config_authority", kindObject},
	{"device_policy", kindObject},
	{"evidence", kindObject},
	{"firmware_liveness", kindObject},
	{"firmware_mqtt_operator", kindObject},
	{"gateway_broker_posture", kindObject},
	{"last_alert_timestamps", kindObject},
	{"remote_command_lockout", kindObject},
	{"state_store_encryption", kindObject},
	{"telemetry_export", kindObject},
}

// snapshotRequiredAfterV250 are the members only a release after v2.5.0
// requires, and v2.5.0 never sends.
var snapshotRequiredAfterV250 = []snapshotMember{
	{"critical", kindBoolean},
	{"firmware_reading_age", kindObject},
	{"firmware_controller_profiles", kindObject},
}

// snapshotOptional are listed members neither encoding requires: each is
// present only while its condition holds, and is part of v2.5.0's closed set.
var snapshotOptional = []snapshotMember{
	{"status", kindString},
	{"safety_zones", kindArray},
}

// healthReading is the severity of one health export item, and, when it reads
// unknown, the fault: a fixed name, never content from the item.
type healthReading struct {
	severity string
	fault    string
	marked   bool
}

// healthReader reads every health export item a client receives, and holds
// each device's latch: once a device has served the projection, an unmarked
// item from it is refused. It is held in memory only, for the client's life.
type healthReader struct {
	mu      sync.Mutex
	latched map[string]bool
	// logged is the fault last logged for each device, so a fault is logged
	// once when it begins, and again only after a reading without it.
	logged map[string]string
}

// read reads item, from a verified export requested from deviceID, and
// updates deviceID's latch. It reports the fault to log, empty when the
// reading has none or it was already logged for the device.
func (r *healthReader) read(item map[string]any, deviceID string) (healthReading, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latched == nil {
		r.latched = make(map[string]bool)
		r.logged = make(map[string]string)
	}
	reading := readHealthItem(item, deviceID, r.latched[deviceID])
	if reading.marked {
		r.latched[deviceID] = true
	}
	toLog := ""
	if reading.fault != r.logged[deviceID] {
		toLog = reading.fault
	}
	if reading.fault == "" {
		delete(r.logged, deviceID)
	} else {
		r.logged[deviceID] = reading.fault
	}
	return reading, toLog
}

// readHealthItem reads the severity of items[0] of a verified health export
// requested from deviceID, under gateway-api/v2's reading of `status` while
// runtimes adopt runtime-health/v4. latched is whether a marked item has been
// read from deviceID before. Nothing healthy is ever inferred from absence:
// ok comes only from an explicit ok in a marked item, or from an unmarked
// item well formed for its encoding that states no condition holds.
func readHealthItem(item map[string]any, deviceID string, latched bool) healthReading {
	projection, hasProjection := item["projection"]
	marked := hasProjection && projection == healthProjectionMarker
	status, hasStatus := item["status"]

	// Critical is never read as less: not for a malformed item, a mistyped
	// member, any projection, or the latch.
	if item["critical"] == true || status == HealthStatusCritical {
		return healthReading{severity: HealthStatusCritical, marked: marked}
	}
	if marked {
		switch status {
		case HealthStatusOK, HealthStatusDegraded:
			return healthReading{severity: status.(string), marked: true}
		}
		return unknownReading("marked_status_unrecognised", true)
	}
	if latched {
		return unknownReading("unmarked_after_projection", false)
	}
	for _, name := range projectionOnlyMembers {
		if _, ok := item[name]; ok {
			return unknownReading("projection_member_unmarked", false)
		}
	}

	// A pre-v4 snapshot: a release after v2.5.0 always writes critical, and
	// v2.5.0 writes it only when it is true.
	_, hasCritical := item["critical"]
	if fault := snapshotFault(item, deviceID, hasCritical); fault != "" {
		return unknownReading(fault, false)
	}
	reasons, _ := item["degradation_reasons"].([]any)
	switch {
	case hasStatus && status == HealthStatusDegraded:
		return healthReading{severity: HealthStatusDegraded}
	case len(reasons) != 0 && (!hasStatus || status == HealthStatusOK):
		// A runtime that names a reason also degrades status.
		return unknownReading("inconsistent_degradation_reasons", false)
	case !hasStatus:
		return healthReading{severity: HealthStatusOK}
	case status == HealthStatusOK && hasCritical:
		return healthReading{severity: HealthStatusOK}
	}
	// v2.5.0 never writes ok, and no encoding lists any other status.
	return unknownReading("status_unrecognised", false)
}

func unknownReading(fault string, marked bool) healthReading {
	return healthReading{severity: HealthStatusUnknown, fault: fault, marked: marked}
}

// snapshotFault names why item is not well formed for its pre-v4 encoding,
// after v2.5.0 when afterV250 and v2.5.0 otherwise, or is empty when it is.
// Members are checked at the top level only, and a fault names only a member
// the table lists, never a value or a member the item alone names.
func snapshotFault(item map[string]any, deviceID string, afterV250 bool) string {
	required := snapshotRequired
	if afterV250 {
		required = append(append([]snapshotMember(nil), snapshotRequired...), snapshotRequiredAfterV250...)
	}
	for _, member := range required {
		value, ok := item[member.name]
		if !ok {
			return "member_absent:" + member.name
		}
		if !hasKind(value, member.kind) {
			return "member_mistyped:" + member.name
		}
	}
	for _, member := range snapshotOptional {
		if value, ok := item[member.name]; ok && !hasKind(value, member.kind) {
			return "member_mistyped:" + member.name
		}
	}
	if item["device_id"] != deviceID {
		return "device_mismatch"
	}
	if !afterV250 {
		// v2.5.0's member set is closed: that is its fingerprint. Every
		// required member is present, so any further member is one the set
		// does not list.
		present := len(snapshotRequired)
		for _, member := range snapshotOptional {
			if _, ok := item[member.name]; ok {
				present++
			}
		}
		if len(item) != present {
			return "member_outside_v2_5_0_set"
		}
	}
	return ""
}

// hasKind reports whether a decoded JSON value has the table's JSON type. A
// present null is of the type null, never absent.
func hasKind(value any, kind jsonKind) bool {
	switch kind {
	case kindString:
		_, ok := value.(string)
		return ok
	case kindNumber:
		switch value.(type) {
		case json.Number, float64:
			return true
		}
		return false
	case kindBoolean:
		_, ok := value.(bool)
		return ok
	case kindArray:
		_, ok := value.([]any)
		return ok
	case kindObject:
		_, ok := value.(map[string]any)
		return ok
	case kindObjectOrNull:
		if value == nil {
			return true
		}
		_, ok := value.(map[string]any)
		return ok
	}
	return false
}
