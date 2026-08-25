// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestAuthoritySinkIsDurableAndIdempotentAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "authority-return")
	opts := QueueOptions{
		Directory: dir,
		MaxItems:  10,
		MaxBytes:  1 << 20,
		Now:       func() time.Time { return time.UnixMilli(1787000002000) },
	}
	sink, err := OpenDurableAuthoritySink(opts)
	if err != nil {
		t.Fatal(err)
	}
	artifact := AuthorityArtifact{
		Type:     AuthorityEpochConfirmation,
		DeviceID: "site-a-edge-01",
		Payload:  validEpochConfirmationBytes("site-a-edge-01"),
	}
	if err := sink.Store(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	if err := sink.Store(context.Background(), artifact); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDurableAuthoritySink(opts)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 1 {
		t.Fatalf("reopened authority queue length = %d, want 1", reopened.Len())
	}
}

func TestAuthoritySinkRejectsRoutingMismatch(t *testing.T) {
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "authority-return"),
		MaxItems:  10,
		MaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Store(context.Background(), AuthorityArtifact{
		Type: AuthorityDeliveryReceipt, DeviceID: "other", Payload: validReceiptBytes("site-a-edge-01", 1, 1),
	}); err == nil {
		t.Fatal("authority sink accepted a mismatched routing identity")
	}
}
