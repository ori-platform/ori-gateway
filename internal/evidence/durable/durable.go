// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package durable holds the filesystem durability primitives the evidence
// packages share.
package durable

import "os"

// SyncDirectory flushes a directory's entries, so a file created, renamed or
// removed in it survives a crash.
func SyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
