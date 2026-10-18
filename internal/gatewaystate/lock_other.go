// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package gatewaystate

import (
	"fmt"
	"os"
)

// lockDirectory refuses: a gateway that signs runs on a unix host, where the
// state directory can be held exclusively.
func lockDirectory(string) (*os.File, error) {
	return nil, fmt.Errorf("gateway state directory locking is supported on unix hosts only")
}
