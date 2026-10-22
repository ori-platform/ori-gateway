// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"
	"time"
)

// flushLogs waits until every record the client and its ingress counts have
// queued is written, so a test reads the whole log, absences included.
func (c *Client) flushLogs(t *testing.T) {
	t.Helper()
	if !c.logs.Flush(10*time.Second) || !c.ingress.logs.Flush(10*time.Second) {
		t.Fatal("the client's log records were not written")
	}
}
