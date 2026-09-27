// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// decodeRouting decodes an artifact's routing projection into dst, a pointer
// to a struct whose json tags name the routing members, after
// refuseAmbiguousMembers. No other member is read.
func decodeRouting(payload []byte, dst any) error {
	if err := refuseAmbiguousMembers(payload, dst); err != nil {
		return err
	}
	return json.Unmarshal(payload, dst)
}

// refuseAmbiguousMembers requires each top-level member that dst's json tags
// name to appear at most once, spelled exactly, and, as a string, to carry
// only Unicode scalar values (evidence-exchange/v2). encoding/json matches
// member names without regard to case, keeps the last duplicate and repairs
// invalid strings into U+FFFD, each of which would let the gateway act on a
// member another reader of the same bytes does not see.
func refuseAmbiguousMembers(payload []byte, dst any) error {
	members := routingMembers(dst)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if open, err := decoder.Token(); err != nil || open != json.Delim('{') {
		return fmt.Errorf("not a JSON object")
	}
	seen := make(map[string]bool, len(members))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("unparseable")
		}
		key, _ := token.(string)
		routing := false
		for _, member := range members {
			switch {
			case key == member && seen[member]:
				return fmt.Errorf("member %s appears more than once", member)
			case key == member:
				seen[member], routing = true, true
			case strings.EqualFold(key, member):
				return fmt.Errorf("member %s is misspelled by case", member)
			}
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("unparseable")
		}
		// encoding/json replaces invalid UTF-8 and a lone surrogate escape
		// with U+FFFD, a string no reader of the exact bytes sees.
		if routing && len(value) > 0 && value[0] == '"' && !scalarValueString(value) {
			return fmt.Errorf("member %s is not a string of Unicode scalar values", key)
		}
	}
	return nil
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

// scalarValueString reports whether a raw JSON string is valid UTF-8 whose
// escapes encode only Unicode scalar values: a surrogate escape must be a high
// half followed at once by a low half.
func scalarValueString(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) || raw[i] != 'u' {
			continue
		}
		unit, ok := hexUnit(raw, i+1)
		if !ok {
			return false
		}
		i += 4
		switch {
		case unit >= 0xDC00 && unit <= 0xDFFF:
			return false
		case unit >= 0xD800 && unit <= 0xDBFF:
			if i+2 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, ok := hexUnit(raw, i+3)
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}

// hexUnit reads the four hex digits of a \u escape starting at raw[at].
func hexUnit(raw []byte, at int) (uint16, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	unit, err := strconv.ParseUint(string(raw[at:at+4]), 16, 16)
	return uint16(unit), err == nil
}
