// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// decodeRouting decodes an artifact's routing projection into dst, a pointer
// to a struct whose json tags name the routing members. A routing member must
// appear at most once and spelled exactly (evidence-exchange/v2): encoding/json
// matches member names without regard to case and keeps the last duplicate,
// which would let the gateway route by a member another reader of the same
// bytes does not see. No other member is read.
func decodeRouting(payload []byte, dst any) error {
	members := routingMembers(dst)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if open, err := decoder.Token(); err != nil || open != json.Delim('{') {
		return fmt.Errorf("routing projection is not a JSON object")
	}
	seen := make(map[string]bool, len(members))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("routing projection is unparseable")
		}
		key, _ := token.(string)
		for _, member := range members {
			switch {
			case key == member && seen[member]:
				return fmt.Errorf("routing member %s appears more than once", member)
			case key == member:
				seen[member] = true
			case strings.EqualFold(key, member):
				return fmt.Errorf("routing member %s is misspelled by case", member)
			}
		}
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			return fmt.Errorf("routing projection is unparseable")
		}
	}
	return json.Unmarshal(payload, dst)
}

// routingMembers lists the json member names of the struct dst points to.
func routingMembers(dst any) []string {
	typ := reflect.TypeOf(dst).Elem()
	members := make([]string, 0, typ.NumField())
	for field := range typ.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name != "" && name != "-" {
			members = append(members, name)
		}
	}
	return members
}
