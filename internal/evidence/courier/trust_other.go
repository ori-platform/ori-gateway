// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package courier

import "os"

// fileOwner reports no owner, so a bundle is refused where ownership cannot be
// read.
func fileOwner(os.FileInfo) (int, bool) {
	return 0, false
}
