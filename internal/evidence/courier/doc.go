// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package courier implements the gateway's role in evidence-exchange/v1.
//
// The gateway is a blind courier. It signs nothing on the evidence path and
// holds no evidence-authority key: outbound artifacts arrive already signed by
// the runtime's device key and leave unchanged, and inbound authority artifacts
// are handed back unmodified. The one artifact it authenticates, the custody
// acknowledgement, is package custody's.
package courier
