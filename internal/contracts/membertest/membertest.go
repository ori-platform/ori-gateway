// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package membertest checks that a struct a runtime-facing payload is decoded
// into, or encoded from, names only its family's defined members. On a payload
// the Payload Member Rule admits, encoding/json's case-insensitive field match
// then reads exactly the members spelled as the struct names them: any other
// name equal to one under the fold is a case variant of a defined member, and
// the rule has refused it.
package membertest

import (
	"reflect"
	"slices"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// UndefinedNames returns every json member name the type of v names, at any
// depth, that family does not define.
func UndefinedNames(family string, v any) []string {
	defined := contracts.DefinedMembers(family)
	var undefined []string
	seen := map[reflect.Type]bool{}
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct || seen[t] {
			return
		}
		seen[t] = true
		for field := range t.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			// encoding/json decodes the exported fields of an embedded
			// struct, exported or not, as the outer struct's own.
			if name == "" && field.Anonymous {
				inner := field.Type
				if inner.Kind() == reflect.Pointer {
					inner = inner.Elem()
				}
				if inner.Kind() == reflect.Struct {
					walk(field.Type)
					continue
				}
				// An embedded non-struct is a member named by its type.
				if !field.IsExported() {
					continue
				}
				name = inner.Name()
			}
			if !field.IsExported() {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if !slices.Contains(defined, name) && !slices.Contains(undefined, name) {
				undefined = append(undefined, name)
			}
			walk(field.Type)
		}
	}
	walk(reflect.TypeOf(v))
	return undefined
}
