// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package gatewaystate

import "os"

func fileOwner(os.FileInfo) (int, bool) { return 0, false }
