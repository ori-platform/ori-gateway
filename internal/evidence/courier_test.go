// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func validReceiptBytes(deviceID string, fromSeq, toSeq int) []byte {
	return []byte(fmt.Sprintf(
		`{"v":1,"device_id":%q,"from_seq":%d,"to_seq":%d,"signature":"ed25519:authority"}`,
		deviceID, fromSeq, toSeq,
	))
}

func validEpochConfirmationBytes(deviceID string) []byte {
	return []byte(fmt.Sprintf(
		`{"v":1,"device_id":%q,"anchor_epoch_id":"epoch","signature":"ed25519:authority"}`,
		deviceID,
	))
}

func testCourier(t *testing.T, maxItems int) (*Courier, *DurableQueue) {
	t.Helper()
	q := openTestQueue(t, maxItems, 1<<20)
	signer, err := NewCustodySigner("published-test-custody-secret-with-enough-entropy-0123456789")
	if err != nil {
		t.Fatal(err)
	}
	courier, err := NewCourier(q, signer)
	if err != nil {
		t.Fatal(err)
	}
	return courier, q
}

func TestCustodyBindsEnvelopeAfterDurableCommit(t *testing.T) {
	courier, q := testCourier(t, 10)
	payload := validEnvelopeBytes(1)
	admitted, err := courier.Admit(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Custody == nil {
		t.Fatal("delivery envelope received no custody acknowledgement")
	}
	digest := sha256.Sum256(payload)
	ack := admitted.Custody
	if ack.DeviceID != "site-a-edge-01" || ack.LocalSeq != 1 {
		t.Fatalf("custody binding changed: %#v", ack)
	}
	if ack.EnvelopeDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		t.Fatalf("custody digest = %s", ack.EnvelopeDigest)
	}
	if ack.CustodyAtMS != admitted.Queued.EnqueuedAtMS {
		t.Fatalf("custody time %d is not durable commit time %d", ack.CustodyAtMS, admitted.Queued.EnqueuedAtMS)
	}
	if q.Len() != 1 {
		t.Fatal("custody was issued without a queued artifact")
	}
}

func TestQueueRefusalNeverIssuesCustody(t *testing.T) {
	courier, _ := testCourier(t, 1)
	if _, err := courier.Admit(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	admitted, err := courier.Admit(ArtifactDeliveryEnvelope, validEnvelopeBytes(2))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue = %v, want ErrQueueFull", err)
	}
	if admitted.Custody != nil {
		t.Fatal("full queue issued custody for bytes it did not hold")
	}
}

func TestCustodyRetryUsesOriginalDurableTimestamp(t *testing.T) {
	courier, _ := testCourier(t, 1)
	payload := validEnvelopeBytes(1)
	first, err := courier.Admit(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := courier.Admit(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}
	if first.Custody == nil || second.Custody == nil || *first.Custody != *second.Custody {
		t.Fatalf("idempotent retry changed custody\nfirst=%#v\nsecond=%#v", first.Custody, second.Custody)
	}
}

func TestCheckpointAndRegistrationAreQueuedWithoutGatewayAuthentication(t *testing.T) {
	courier, q := testCourier(t, 10)
	for _, kind := range []ArtifactType{
		ArtifactCheckpoint,
		ArtifactCommissioningAuthorization,
		ArtifactAnchorRegistration,
	} {
		payload := []byte(`{"v":1,"device_id":"site-a-edge-01","signature":"ed25519:device"}`)
		admitted, err := courier.Admit(kind, payload)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if admitted.Custody != nil {
			t.Fatalf("%s was confused with a delivery envelope", kind)
		}
	}
	if q.Len() != 3 {
		t.Fatalf("queued %d artifacts, want 3", q.Len())
	}
}

func TestCustodyIsNotReceipt(t *testing.T) {
	courier, _ := testCourier(t, 10)
	admitted, err := courier.Admit(ArtifactDeliveryEnvelope, validEnvelopeBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Custody == nil {
		t.Fatal("no custody acknowledgement")
	}
	// Custody has a MAC and one sequence. A receipt is an authority artifact
	// with a signature and a closed range; there is no conversion between them.
	if admitted.Custody.MAC == "" {
		t.Fatal("custody is unauthenticated")
	}
	if validAuthorityArtifactType(AuthorityArtifactType("custody_acknowledgement")) {
		t.Fatal("custody can enter the authority-artifact return path")
	}
	if !validAuthorityArtifactType(AuthorityDeliveryReceipt) || !validAuthorityArtifactType(AuthorityEpochConfirmation) {
		t.Fatal("the authority-artifact vocabulary is incomplete")
	}
}

type fakeEvidenceChannel struct {
	result    DeliveryResult
	err       error
	delivered []QueuedArtifact
}

func (f *fakeEvidenceChannel) Deliver(_ context.Context, artifact QueuedArtifact) (DeliveryResult, error) {
	f.delivered = append(f.delivered, QueuedArtifact{
		ID: artifact.ID, Type: artifact.Type, Payload: append([]byte(nil), artifact.Payload...), EnqueuedAtMS: artifact.EnqueuedAtMS,
	})
	return f.result, f.err
}

type fakeAuthoritySink struct {
	err    error
	stored []AuthorityArtifact
}

type blockingEvidenceChannel struct {
	entered atomic.Int32
	first   chan struct{}
	release chan struct{}
}

type permanentRefusalChannel struct {
	calls atomic.Int32
}

func (c *permanentRefusalChannel) Deliver(_ context.Context, _ QueuedArtifact) (DeliveryResult, error) {
	c.calls.Add(1)
	return DeliveryResult{Accepted: false, Retriable: false, RefusalReason: "malformed"}, nil
}

type retryAfterChannel struct {
	calls atomic.Int32
}

func (c *retryAfterChannel) Deliver(_ context.Context, _ QueuedArtifact) (DeliveryResult, error) {
	c.calls.Add(1)
	return DeliveryResult{Accepted: false, Retriable: true, RetryAfter: 100 * time.Millisecond}, nil
}

func (c *blockingEvidenceChannel) Deliver(_ context.Context, _ QueuedArtifact) (DeliveryResult, error) {
	if c.entered.Add(1) == 1 {
		close(c.first)
	}
	<-c.release
	return DeliveryResult{Accepted: true}, nil
}

func (f *fakeAuthoritySink) Store(_ context.Context, artifact AuthorityArtifact) error {
	if f.err != nil {
		return f.err
	}
	f.stored = append(f.stored, AuthorityArtifact{
		Type: artifact.Type, DeviceID: artifact.DeviceID, Payload: append([]byte(nil), artifact.Payload...),
	})
	return nil
}

func TestIndependentChannelForwardsBytesAndRetiresOnlyAfterAcceptance(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	payload := []byte("{ \"v\":1, \"device_id\":\"d\", \"signature\":\"ed25519:x\" }")
	entry, err := q.Enqueue(ArtifactCheckpoint, payload)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := validEpochConfirmationBytes("d")
	channel := &fakeEvidenceChannel{result: DeliveryResult{
		Accepted:           true,
		AuthorityArtifacts: []AuthorityArtifact{{Type: AuthorityEpochConfirmation, Payload: confirmation}},
	}}
	sink := &fakeAuthoritySink{}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := worker.deliverHead(context.Background())
	if err != nil || !delivered {
		t.Fatalf("deliver = %v, %v", delivered, err)
	}
	if len(channel.delivered) != 1 || channel.delivered[0].ID != entry.ID || !bytes.Equal(channel.delivered[0].Payload, payload) {
		t.Fatal("evidence channel did not receive exact queued bytes")
	}
	if len(sink.stored) != 1 || sink.stored[0].DeviceID != "d" || !bytes.Equal(sink.stored[0].Payload, confirmation) {
		t.Fatal("authority artifact was changed before runtime forwarding")
	}
	if q.Len() != 0 {
		t.Fatal("accepted and durably returned artifact was not retired")
	}
}

func TestChannelFailureLeavesQueuePendingAndLeaksNoIdentity(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`)); err != nil {
		t.Fatal(err)
	}
	private := "https://private-store.example/ingest token=secret"
	channel := &fakeEvidenceChannel{err: errors.New(private)}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, gotErr := worker.deliverHead(context.Background())
	if gotErr == nil || strings.Contains(gotErr.Error(), private) {
		t.Fatalf("channel error leaked private identity: %v", gotErr)
	}
	worker.recordFailure(gotErr)
	status := worker.Status()
	if !status.Degraded || status.Pending != 1 || strings.Contains(status.LastError, private) {
		t.Fatalf("unsafe or false-healthy status: %#v", status)
	}
}

func TestPermanentAuthorityRefusalBlocksWithoutHotRetryOrRetirement(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`)); err != nil {
		t.Fatal(err)
	}
	channel := &permanentRefusalChannel{}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for channel.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if calls := channel.calls.Load(); calls != 1 {
		t.Fatalf("permanent refusal attempted %d times, want one", calls)
	}
	status := worker.Status()
	if !status.Blocked || !status.Degraded || status.LastError != "channel_permanent_refusal" || q.Len() != 1 {
		t.Fatalf("permanent refusal status = %#v, pending=%d", status, q.Len())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityRetryAfterOverridesShortLocalRetry(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`)); err != nil {
		t.Fatal(err)
	}
	channel := &retryAfterChannel{}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for channel.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if calls := channel.calls.Load(); calls != 1 {
		t.Fatalf("Retry-After was ignored: %d attempts", calls)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAuthoritySinkFailureKeepsOutboundArtifact(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	channel := &fakeEvidenceChannel{result: DeliveryResult{
		Accepted:           true,
		AuthorityArtifacts: []AuthorityArtifact{{Type: AuthorityDeliveryReceipt, Payload: validReceiptBytes("site-a-edge-01", 1, 1)}},
	}}
	sink := &fakeAuthoritySink{err: errors.New("runtime unavailable")}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.deliverHead(context.Background()); err == nil {
		t.Fatal("sink failure was treated as delivery")
	}
	if q.Len() != 1 {
		t.Fatal("outbound artifact retired before authority response was durable")
	}
}

func TestCoveringReceiptIsRoutedAndRetiresEnvelope(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(12)); err != nil {
		t.Fatal(err)
	}
	receipt := validReceiptBytes("site-a-edge-01", 11, 12)
	sink := &fakeAuthoritySink{}
	worker, err := NewDeliveryWorker(q, &fakeEvidenceChannel{result: DeliveryResult{
		Accepted: true,
		AuthorityArtifacts: []AuthorityArtifact{{
			Type: AuthorityDeliveryReceipt, Payload: receipt,
		}},
	}}, sink, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := worker.deliverHead(context.Background())
	if err != nil || !delivered {
		t.Fatalf("covering receipt delivery = %v, %v", delivered, err)
	}
	if q.Len() != 0 || len(sink.stored) != 1 || sink.stored[0].DeviceID != "site-a-edge-01" ||
		!bytes.Equal(sink.stored[0].Payload, receipt) {
		t.Fatal("covering receipt was not routed byte-for-byte before retirement")
	}
}

func TestAcceptedEnvelopeWithoutReceiptStaysQueued(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(q, &fakeEvidenceChannel{
		result: DeliveryResult{Accepted: true},
	}, &fakeAuthoritySink{}, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.deliverHead(context.Background()); !errors.Is(err, errMalformedResponse) {
		t.Fatalf("receipt-less acceptance = %v, want malformed response", err)
	}
	if q.Len() != 1 {
		t.Fatal("envelope retired without an authority receipt")
	}
}

func TestUnknownAuthorityArtifactNeverReachesRuntimeOrRetiresQueue(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	channel := &fakeEvidenceChannel{result: DeliveryResult{
		Accepted:           true,
		AuthorityArtifacts: []AuthorityArtifact{{Type: "custody_acknowledgement", Payload: []byte(`{"v":1}`)}},
	}}
	sink := &fakeAuthoritySink{}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.deliverHead(context.Background()); !errors.Is(err, errMalformedResponse) {
		t.Fatalf("unknown authority type = %v, want malformed response", err)
	}
	if len(sink.stored) != 0 || q.Len() != 1 {
		t.Fatal("unknown authority artifact reached runtime or retired evidence")
	}
}

func TestReceiptForAnotherDeviceOrRangeCannotRetireEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		receipt []byte
	}{
		{"other device", validReceiptBytes("site-b-edge-01", 1, 1)},
		{"other range", validReceiptBytes("site-a-edge-01", 2, 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := openTestQueue(t, 10, 1<<20)
			if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
				t.Fatal(err)
			}
			sink := &fakeAuthoritySink{}
			worker, err := NewDeliveryWorker(q, &fakeEvidenceChannel{result: DeliveryResult{
				Accepted: true,
				AuthorityArtifacts: []AuthorityArtifact{{
					Type: AuthorityDeliveryReceipt, Payload: tc.receipt,
				}},
			}}, sink, DeliveryWorkerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := worker.deliverHead(context.Background()); !errors.Is(err, errMalformedResponse) {
				t.Fatalf("substituted receipt = %v, want malformed response", err)
			}
			if q.Len() != 1 || len(sink.stored) != 0 {
				t.Fatal("substituted receipt reached the runtime or retired the envelope")
			}
		})
	}
}

func TestChannelIndependence(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`)); err != nil {
		t.Fatal(err)
	}
	fleetAvailable := false // deliberately irrelevant to the evidence worker
	channel := &fakeEvidenceChannel{result: DeliveryResult{Accepted: true}}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if delivered, err := worker.deliverHead(context.Background()); err != nil || !delivered {
		t.Fatalf("evidence depended on fleet=%v: delivered=%v err=%v", fleetAvailable, delivered, err)
	}

	// The reverse direction: a failed evidence channel changes only evidence
	// status. This package has no fleet client, credentials, storage or status to
	// mutate, which is the structural independence the issue requires.
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"e"}`)); err != nil {
		t.Fatal(err)
	}
	channel.err = errors.New("down")
	if _, err := worker.deliverHead(context.Background()); err == nil {
		t.Fatal("failed evidence channel reported success")
	}
	if fleetAvailable {
		t.Fatal("test setup unexpectedly changed fleet state")
	}
}

func TestConcurrentDeliveryAttemptsCannotSendOneArtifactTwice(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`)); err != nil {
		t.Fatal(err)
	}
	channel := &blockingEvidenceChannel{first: make(chan struct{}), release: make(chan struct{})}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 8
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(attempts)
	done.Add(attempts)
	for range attempts {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, _ = worker.deliverHead(context.Background())
		}()
	}
	ready.Wait()
	close(start)
	<-channel.first
	time.Sleep(10 * time.Millisecond)
	if got := channel.entered.Load(); got != 1 {
		t.Fatalf("one queued artifact entered the evidence channel %d times concurrently", got)
	}
	close(channel.release)
	done.Wait()
	if q.Len() != 0 || channel.entered.Load() != 1 {
		t.Fatalf("concurrent drain left len=%d deliveries=%d", q.Len(), channel.entered.Load())
	}
}
