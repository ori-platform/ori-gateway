// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func main() {
	fmt.Printf("ori-gateway bootstrap: heartbeat topic %s\n", contracts.GatewayHealthTopic)
}
