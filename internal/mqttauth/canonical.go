// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Ori canonical JSON is the shared signing preimage format for runtime-gateway
// envelopes. The runtime produces it with Python's
// json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False), so this
// package must reproduce those bytes exactly or HMACs will not agree.
//
// encoding/json cannot be configured to do so. json.Marshal escapes '<', '>' and
// '&', and both Marshal and Encoder escape U+2028 and U+2029 unconditionally --
// SetEscapeHTML(false) suppresses only the first three. Any message carrying one
// of those characters in a string or a key would be rejected as unauthenticated,
// including LLM prose in reasoning_log exports. Hence a hand-written writer.
//
// Escaping rule: emit only '"', '\\' and C0 controls as escapes, with the short
// forms Python uses for \b \f \n \r \t and lowercase \u00xx for the rest. Every
// other code point is written literally, including '<', '>', '&', U+2028, U+2029,
// DEL and all non-ASCII.
const hexDigits = "0123456789abcdef"

// writeCanonicalString appends s to b as a canonical JSON string, quotes included.
func writeCanonicalString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexDigits[(r>>4)&0xF])
				b.WriteByte(hexDigits[r&0xF])
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

// writeCanonicalValue appends v to b. v must come from a json.Decoder configured
// with UseNumber, which yields only these six types; anything else is a
// programming error and fails closed rather than serialising to something the
// runtime would not reproduce.
func writeCanonicalValue(b *strings.Builder, v any) error {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(t))
	case string:
		writeCanonicalString(b, t)
	case json.Number:
		// Preserve the spelling received on the wire. The producer already emitted
		// its own canonical form, so reproducing that text reproduces the preimage
		// exactly. Re-formatting here would be wrong in both directions: it would
		// diverge from the producer, and it would impose evidence/v2's D-011 zone
		// on a transport that legitimately carries small sensor readings such as
		// 5e-05. D-011 governs evidence artifacts, not gateway MQTT.
		b.WriteString(t.String())
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonicalString(b, k)
			b.WriteByte(':')
			if err := writeCanonicalValue(b, t[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeCanonicalValue(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	default:
		return fmt.Errorf("mqtt auth: cannot canonicalise value of type %T", v)
	}
	return nil
}

// CanonicalJSON returns the canonical JSON encoding of a decoded payload.
func CanonicalJSON(payload map[string]any) ([]byte, error) {
	var b strings.Builder
	if err := writeCanonicalValue(&b, payload); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

// ValidateWireUnicode refuses payload bytes whose Unicode the two runtimes would
// not agree on. Go's JSON decoder silently substitutes U+FFFD for a lone
// surrogate escape or malformed UTF-8, while Python surfaces the lone surrogate
// and then fails to encode it; accepting such a payload would mean the two sides
// canonicalise different text. Only refusal keeps them equivalent.
func ValidateWireUnicode(raw []byte) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("mqtt auth: payload is not valid UTF-8")
	}
	for i := 0; i+1 < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		// A backslash run of even length is escaped backslashes, not an escape.
		backslashes := 0
		for j := i; j >= 0 && raw[j] == '\\'; j-- {
			backslashes++
		}
		if backslashes%2 == 0 || raw[i+1] != 'u' {
			continue
		}
		high, ok := parseHex4(raw, i+2)
		if !ok {
			return fmt.Errorf("mqtt auth: malformed \\u escape")
		}
		if high < 0xD800 || high > 0xDFFF {
			continue
		}
		if high > 0xDBFF {
			return fmt.Errorf("mqtt auth: unpaired low surrogate U+%04X", high)
		}
		if i+12 > len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
			return fmt.Errorf("mqtt auth: unpaired high surrogate U+%04X", high)
		}
		low, ok := parseHex4(raw, i+8)
		if !ok || low < 0xDC00 || low > 0xDFFF {
			return fmt.Errorf("mqtt auth: high surrogate U+%04X is not followed by a low surrogate", high)
		}
		i += 11
	}
	return nil
}

func parseHex4(raw []byte, at int) (rune, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	var v rune
	for _, c := range raw[at : at+4] {
		switch {
		case c >= '0' && c <= '9':
			v = v<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			v = v<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			v = v<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return v, true
}
