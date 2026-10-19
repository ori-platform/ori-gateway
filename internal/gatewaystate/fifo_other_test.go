// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package gatewaystate

import "errors"

func makeFIFO(string) error { return errors.New("no FIFOs on this host") }
