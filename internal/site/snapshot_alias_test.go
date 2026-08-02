// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"fmt"
	"reflect"
	"testing"
)

// Registry.Snapshot must return values a caller cannot mutate registry state
// through. A NodeHeartbeat value copy duplicates pointers, slice headers and
// map headers, so every reference-bearing field needs an explicit copy.
//
// The audit below is recursive and kind-driven rather than a list of field
// names. A guard that names the fields it knows about protects exactly the
// fields someone already thought about — which is not the failure mode. The
// original defect was that a value copy *looks* like a copy, so the fields
// that need protecting are the ones nobody examined.

// mutableTarget is one leaf reachable from a snapshot through indirection.
// Mutating it must not be observable in the registry.
type mutableTarget struct {
	path  string
	value reflect.Value // settable, or a map handled via setMapLeaf
	mapOf *mapLeaf
}

type mapLeaf struct {
	m   reflect.Value
	key reflect.Value
}

// collectMutableTargets walks v and returns every leaf that a caller could
// write through. `viaIndirection` tracks whether the path has passed a
// pointer, slice or map: leaves reached without indirection live in the
// caller's own copy and are harmless to mutate.
func collectMutableTargets(v reflect.Value, path string, viaIndirection bool) []mutableTarget {
	var out []mutableTarget

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return collectMutableTargets(v.Elem(), path+"->", true)

	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return collectMutableTargets(v.Elem(), path, viaIndirection)

	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil
		}
		for i := 0; i < v.Len(); i++ {
			out = append(out, collectMutableTargets(
				v.Index(i), fmt.Sprintf("%s[%d]", path, i), true)...)
		}
		return out

	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		iter := v.MapRange()
		for iter.Next() {
			key := fmt.Sprintf("%s[%v]", path, iter.Key().Interface())
			// Map values are not addressable, so entry replacement goes
			// through SetMapIndex. This target proves the map CONTAINERS are
			// independent.
			out = append(out, mutableTarget{
				path:  key + " (entry)",
				mapOf: &mapLeaf{m: v, key: iter.Key()},
			})
			// Container independence is not value independence. A shallow
			// clone — maps.Clone on a map of pointers, say — produces
			// distinct containers whose entries still point at shared state,
			// and the entry-replacement target above would pass. Recurse
			// through the value: a pointer's Elem and a slice's Index are
			// addressable however the pointer or header was obtained.
			out = append(out, collectMutableTargets(iter.Value(), key, true)...)
		}
		return out

	case reflect.Struct:
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if typ.Field(i).PkgPath != "" {
				continue // unexported
			}
			out = append(out, collectMutableTargets(
				v.Field(i), path+"."+typ.Field(i).Name, viaIndirection)...)
		}
		return out

	default:
		if viaIndirection && v.CanSet() {
			return []mutableTarget{{path: path, value: v}}
		}
		return nil
	}
}

// mutate writes a distinguishable value into the target.
func (t mutableTarget) mutate() bool {
	if t.mapOf != nil {
		elem := t.mapOf.m.Type().Elem()
		t.mapOf.m.SetMapIndex(t.mapOf.key, reflect.Zero(elem))
		return true
	}
	switch t.value.Kind() {
	case reflect.String:
		t.value.SetString("mutated_by_caller")
	case reflect.Bool:
		t.value.SetBool(!t.value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		t.value.SetInt(t.value.Int() + 9973)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		t.value.SetUint(t.value.Uint() + 9973)
	case reflect.Float32, reflect.Float64:
		t.value.SetFloat(t.value.Float() + 9973)
	default:
		return false
	}
	return true
}

func aliasProbeHeartbeat() NodeHeartbeat {
	return NodeHeartbeat{
		DeviceID:           "dev-01",
		Status:             NodeStatusDegraded,
		LastSeenMS:         1,
		GatewaySeen:        2,
		ActiveTriggers:     []string{"grid_sag", "battery_cycle_stress"},
		DegradationReasons: []string{"firmware_liveness_degraded"},
		Posture: &SiteNodePosture{
			BrokerHardening: &SiteNodeBrokerPosture{ACLPolicy: "per_device_required"},
			Encryption:      &SiteNodeEncryptionPosture{Mode: "at_rest"},
			AlertOutbox:     &SiteNodeAlertOutboxPosture{BacklogCount: 3},
		},
		Evidence: &NodeEvidence{ChainHeadHash: "abc", AttestationGapCount: 1},
	}
}

// Every reference-bearing field must be populated, or the mutation audit
// silently skips it and proves nothing about it.
//
// The check descends the same way the walker does. A top-level-only version
// would see a populated Posture pointer and pass while a nested slice inside
// SiteNodeBrokerPosture sat nil and unaudited — coverage measured at the wrong
// depth is how a guard reports protection it is not providing.
func assertProbeComplete(t *testing.T, v reflect.Value, path string) {
	t.Helper()

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			t.Fatalf(
				"aliasProbeHeartbeat leaves %s nil, so the alias audit never "+
					"reaches it.\nPopulate it — a reference the probe does not "+
					"reach is a reference the guard does not protect.",
				path,
			)
		}
		assertProbeComplete(t, v.Elem(), path+"->")

	case reflect.Slice, reflect.Map:
		if v.IsNil() {
			t.Fatalf(
				"aliasProbeHeartbeat leaves %s nil, so the alias audit never "+
					"reaches it.\nPopulate it with at least one element.",
				path,
			)
		}
		if v.Len() == 0 {
			t.Fatalf(
				"aliasProbeHeartbeat leaves %s empty, so the alias audit "+
					"produces no target for it.\nGive it at least one element.",
				path,
			)
		}
		if v.Kind() == reflect.Slice {
			for i := 0; i < v.Len(); i++ {
				assertProbeComplete(t, v.Index(i), fmt.Sprintf("%s[%d]", path, i))
			}
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			assertProbeComplete(t, iter.Value(),
				fmt.Sprintf("%s[%v]", path, iter.Key().Interface()))
		}

	case reflect.Struct:
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if typ.Field(i).PkgPath != "" {
				continue // unexported
			}
			assertProbeComplete(t, v.Field(i), path+"."+typ.Field(i).Name)
		}
	}
}

func TestAliasProbeCoversEveryReferenceField(t *testing.T) {
	assertProbeComplete(t, reflect.ValueOf(aliasProbeHeartbeat()), "NodeHeartbeat")
}

// The audit itself: mutate every leaf reachable through indirection, one at a
// time, and require the registry to be unmoved.
func TestSnapshotSharesNoMutableReference(t *testing.T) {
	probe := aliasProbeHeartbeat()

	reg := NewRegistry()
	reg.Upsert(aliasProbeHeartbeat())
	baseline := reg.Snapshot()[0]

	// Count the paths once so the walker cannot silently degrade to nothing.
	held := baseline
	paths := collectMutableTargets(reflect.ValueOf(&held).Elem(), "NodeHeartbeat", false)
	if len(paths) == 0 {
		t.Fatal("the reflection walk found no mutable references; the audit is broken")
	}
	t.Logf("auditing %d reachable mutable references", len(paths))

	for idx, describe := range paths {
		t.Run(describe.path, func(t *testing.T) {
			fresh := NewRegistry()
			fresh.Upsert(aliasProbeHeartbeat())

			taken := fresh.Snapshot()[0]
			targets := collectMutableTargets(reflect.ValueOf(&taken).Elem(), "NodeHeartbeat", false)
			if idx >= len(targets) {
				t.Fatalf("target %d disappeared between walks", idx)
			}
			if !targets[idx].mutate() {
				// Skipping here would let a future field type quietly
				// disable its own guard.
				t.Fatalf(
					"no mutation defined for %s (kind %s); extend mutate() so "+
						"this leaf is actually exercised",
					targets[idx].path, targets[idx].value.Kind(),
				)
			}

			after := fresh.Snapshot()[0]
			if !reflect.DeepEqual(after, baselineOf(probe, fresh)) {
				t.Fatalf(
					"%s is aliased: mutating a snapshot changed registry state.\n"+
						"Add a copy for it in Registry.Snapshot.",
					targets[idx].path,
				)
			}
		})
	}
}

// baselineOf returns what a snapshot should still look like: the registry's
// own view, taken from a registry no caller has touched.
func baselineOf(probe NodeHeartbeat, _ *Registry) NodeHeartbeat {
	clean := NewRegistry()
	clean.Upsert(probe)
	return clean.Snapshot()[0]
}

// Two snapshots must not share backing arrays with each other either.
func TestSnapshotsAreIndependentOfEachOther(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(aliasProbeHeartbeat())

	first := reg.Snapshot()[0]
	second := reg.Snapshot()[0]

	first.ActiveTriggers[0] = "mutated_by_caller"
	if second.ActiveTriggers[0] == "mutated_by_caller" {
		t.Fatal("two snapshots share an ActiveTriggers backing array")
	}
	first.DegradationReasons[0] = "mutated_by_caller"
	if second.DegradationReasons[0] == "mutated_by_caller" {
		t.Fatal("two snapshots share a DegradationReasons backing array")
	}
	first.Posture.BrokerHardening.ACLPolicy = "mutated_by_caller"
	if second.Posture.BrokerHardening.ACLPolicy == "mutated_by_caller" {
		t.Fatal("two snapshots share a Posture pointer")
	}
}

// cloneStrings preserves nil, which DegradationReasons depends on: absent and
// present-empty are different states on the wire, and collapsing them would
// make the gateway emit a malformed field.
func TestCloneStringsPreservesNil(t *testing.T) {
	if got := cloneStrings(nil); got != nil {
		t.Fatalf("nil became %#v", got)
	}
	if got := cloneStrings([]string{}); got == nil || len(got) != 0 {
		t.Fatalf("empty became %#v", got)
	}
}
